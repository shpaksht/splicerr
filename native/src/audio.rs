use crate::catalog::{Catalog, Result, Sample};
use rodio::{Decoder, OutputStream, OutputStreamBuilder, Sink, Source};
use std::{
    io::Cursor,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

const MAX_AUDIO_BYTES: usize = 24 * 1024 * 1024;
const MAX_SAMPLES: usize = 30_000_000;

pub struct Clip {
    samples: Box<[f32]>,
    channels: u16,
    rate: u32,
}

impl Clip {
    pub async fn fetch(catalog: &Catalog, sample: &Sample, trim_delay: bool) -> Result<Self> {
        let url = sample
            .preview()
            .ok_or("This sample has no audio preview.")?;
        let mut bytes = catalog.media(url, MAX_AUDIO_BYTES).await?;
        let duration = sample_duration(sample.duration);
        tokio::task::spawn_blocking(move || {
            let offset = unpack_preview(&mut bytes)?;
            Self::decode(bytes, offset, duration, trim_delay && offset != 0)
        })
        .await
        .map_err(|_| "Audio decoding was interrupted.".to_string())?
    }

    fn decode(
        bytes: Vec<u8>,
        offset: usize,
        duration: Option<Duration>,
        trim_delay: bool,
    ) -> Result<Self> {
        let mut stream = Cursor::new(bytes);
        // The decoder expects the start of the media to be byte zero.
        let bytes = if offset == 0 {
            stream.into_inner()
        } else {
            stream.get_mut().split_off(offset)
        };
        let decoder = Decoder::try_from(Cursor::new(bytes))
            .map_err(|_| "Could not decode this audio preview.".to_string())?;
        let channels = decoder.channels();
        let rate = decoder.sample_rate();
        let trim = if trim_delay {
            (rate as usize * 12 / 1000) * channels as usize
        } else {
            0
        };
        let limit = duration
            .map(|d| (d.as_secs_f64() * rate as f64) as usize * channels as usize)
            .unwrap_or(MAX_SAMPLES)
            .min(MAX_SAMPLES);
        let mut samples: Vec<f32> = decoder.skip(trim).take(limit).collect();
        samples.truncate(samples.len() / channels as usize * channels as usize);
        if samples.is_empty() {
            return Err("This preview has no playable audio.".into());
        }
        Ok(Self {
            samples: samples.into_boxed_slice(),
            channels,
            rate,
        })
    }

    pub fn demo() -> Self {
        let rate = 22_050;
        let samples = (0..rate * 2)
            .map(|i| {
                let t = i as f32 / rate as f32;
                let beat = t % 0.5;
                (beat * -22.0).exp() * (t * 75.0 * std::f32::consts::TAU).sin() * 0.4
            })
            .collect();
        Self {
            samples,
            channels: 1,
            rate,
        }
    }

    pub fn duration(&self) -> Duration {
        Duration::from_secs_f64(
            self.samples.len() as f64 / f64::from(self.rate) / f64::from(self.channels),
        )
    }

    pub fn export(&self, directory: &Path, sample: &Sample, trim_delay: bool) -> Result<PathBuf> {
        let path = preview_path(directory, sample, trim_delay);
        let folder = path.parent().ok_or("Invalid WAV destination.")?;
        std::fs::create_dir_all(folder).map_err(file_error)?;
        let folder = folder.canonicalize().map_err(file_error)?;
        let path = folder.join(path.file_name().ok_or("Invalid WAV filename.")?);
        let mut temporary = tempfile::NamedTempFile::new_in(&folder).map_err(file_error)?;
        let spec = hound::WavSpec {
            channels: self.channels,
            sample_rate: self.rate,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        };
        let mut writer =
            hound::WavWriter::new(temporary.as_file_mut(), spec).map_err(file_error)?;
        for sample in &self.samples {
            writer
                .write_sample((sample.clamp(-1.0, 1.0) * f32::from(i16::MAX)).round() as i16)
                .map_err(file_error)?;
        }
        writer.finalize().map_err(file_error)?;
        match temporary.persist_noclobber(&path) {
            Ok(_) => Ok(path),
            Err(error)
                if error.error.kind() == std::io::ErrorKind::AlreadyExists && path.is_file() =>
            {
                Ok(path)
            }
            Err(error) => Err(file_error(error)),
        }
    }
}

pub fn preview_path(directory: &Path, sample: &Sample, trim_delay: bool) -> PathBuf {
    let pack = sample
        .pack()
        .map(|p| p.name.as_str())
        .unwrap_or("Splice previews");
    let name = Path::new(sample.filename())
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("preview");
    let suffix: String = sample.uuid.chars().take(10).collect();
    let variant = if trim_delay { "" } else { " - untrimmed" };
    directory.join(safe_name(pack)).join(format!(
        "{} - {}{variant}.wav",
        safe_name(name),
        safe_name(&suffix)
    ))
}

fn sample_duration(duration: Option<u32>) -> Option<Duration> {
    duration
        .filter(|&ms| ms != 0)
        .map(|ms| Duration::from_millis(u64::from(ms)))
}

fn file_error(error: impl std::fmt::Display) -> String {
    format!("Could not save the WAV preview. {error}")
}

fn safe_name(value: &str) -> String {
    let mut sanitized = String::new();
    for c in value.chars() {
        let c = if c.is_control() || "/\\:<>|\"?*".contains(c) {
            '_'
        } else {
            c
        };
        if sanitized.len() + c.len_utf8() > 180 {
            break;
        }
        sanitized.push(c);
    }
    let trimmed = sanitized.trim_matches(|c: char| c == '.' || c.is_whitespace());
    if trimmed.is_empty() {
        "preview".into()
    } else {
        trimmed.into()
    }
}

// A catalog preview may be ordinary MP3 or the legacy "sp" preview envelope.
fn unpack_preview(bytes: &mut [u8]) -> Result<usize> {
    if !bytes.starts_with(b"sp") {
        return Ok(0);
    }
    let (header, payload) = bytes
        .split_at_mut_checked(28)
        .ok_or("Truncated Splice preview.")?;
    let length = usize::try_from(u64::from_le_bytes(header[2..10].try_into().unwrap()))
        .map_err(|_| "Invalid Splice preview length.")?;
    let key = &header[10..28];
    // The format restarts the key after one untouched block of the same size.
    for start in [0, length.saturating_mul(2)] {
        let available = payload.len().saturating_sub(start).min(length);
        if available > 0 {
            for (byte, mask) in payload[start..start + available]
                .iter_mut()
                .zip(key.iter().cycle())
            {
                *byte ^= mask;
            }
        }
    }
    Ok(28)
}

pub struct Player {
    stream: Option<OutputStream>,
    sink: Option<Sink>,
    start: Duration,
}

impl Player {
    pub fn new() -> Self {
        Self {
            stream: None,
            sink: None,
            start: Duration::ZERO,
        }
    }

    pub fn stop(&mut self) {
        self.sink = None;
        self.start = Duration::ZERO;
    }

    pub fn start(
        &mut self,
        clip: Arc<Clip>,
        fraction: f32,
        volume: f32,
        repeat: bool,
        paused: bool,
    ) -> Result<()> {
        self.stop();
        if self.stream.is_none() {
            self.stream = Some(
                OutputStreamBuilder::open_default_stream()
                    .map_err(|_| "Could not open the default audio output.".to_string())?,
            );
        }
        let sink = Sink::connect_new(self.stream.as_ref().unwrap().mixer());
        let frames = clip.samples.len() / clip.channels as usize;
        let frame = ((frames as f32 * fraction.clamp(0.0, 1.0)) as usize).min(frames - 1);
        self.start = Duration::from_secs_f64(frame as f64 / f64::from(clip.rate));
        if paused {
            sink.pause();
        }
        sink.set_volume(volume);
        sink.append(ClipSource {
            index: frame * clip.channels as usize,
            clip,
            repeat,
        });
        self.sink = Some(sink);
        Ok(())
    }

    pub fn playing(&self) -> bool {
        self.sink
            .as_ref()
            .is_some_and(|sink| !sink.is_paused() && !sink.empty())
    }

    pub fn toggle(&self) -> bool {
        let Some(sink) = &self.sink else { return false };
        if sink.empty() {
            return false;
        }
        if sink.is_paused() {
            sink.play();
        } else {
            sink.pause();
        }
        true
    }

    pub fn volume(&self, volume: f32) {
        if let Some(sink) = &self.sink {
            sink.set_volume(volume);
        }
    }

    pub fn position(&self, clip: &Clip, repeat: bool) -> f32 {
        let Some(sink) = &self.sink else { return 0.0 };
        if sink.empty() {
            return 1.0;
        }
        let ratio = (self.start + sink.get_pos()).as_secs_f32() / clip.duration().as_secs_f32();
        if repeat {
            ratio.fract()
        } else {
            ratio.clamp(0.0, 1.0)
        }
    }
}

struct ClipSource {
    index: usize,
    clip: Arc<Clip>,
    repeat: bool,
}

impl Iterator for ClipSource {
    type Item = f32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.repeat && self.index == self.clip.samples.len() {
            self.index = 0;
        }
        let sample = *self.clip.samples.get(self.index)?;
        self.index += 1;
        Some(sample)
    }
}

impl Source for ClipSource {
    fn current_span_len(&self) -> Option<usize> {
        None
    }
    fn channels(&self) -> u16 {
        self.clip.channels
    }
    fn sample_rate(&self) -> u32 {
        self.clip.rate
    }
    fn total_duration(&self) -> Option<Duration> {
        (!self.repeat).then(|| self.clip.duration())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_preview_mask_resets_after_the_gap_and_clamps_the_last_block() {
        let mut bytes = b"sp".to_vec();
        bytes.extend(3_u64.to_le_bytes());
        bytes.extend(1..=18);
        bytes.extend([10; 8]);
        assert_eq!(unpack_preview(&mut bytes), Ok(28));
        assert_eq!(&bytes[28..], &[11, 8, 9, 10, 10, 10, 11, 8]);
        assert!(unpack_preview(&mut b"sp".to_vec()).is_err());
        assert_eq!(unpack_preview(&mut b"ID3".to_vec()), Ok(0));
    }
}
