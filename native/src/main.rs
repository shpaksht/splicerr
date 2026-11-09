mod app;
mod audio;
mod catalog;
mod media;
mod sample_drag;

use eframe::egui::{ViewportBuilder, vec2};
use std::time::Instant;

fn main() -> eframe::Result {
    let mode = std::env::args().nth(1);
    if mode.as_deref() == Some("--check-catalog") {
        if let Err(error) = check_catalog() {
            eprintln!("{error}");
            std::process::exit(1);
        }
        return Ok(());
    }
    let demo = mode.as_deref() == Some("--demo");
    eframe::run_native(
        "Splicerr Native",
        eframe::NativeOptions {
            viewport: ViewportBuilder::default()
                .with_title("Splicerr Native")
                .with_inner_size(vec2(1220.0, 790.0))
                .with_min_inner_size(vec2(720.0, 510.0)),
            ..Default::default()
        },
        Box::new(move |cc| {
            Ok(Box::new(
                app::App::new(cc, demo).map_err(std::io::Error::other)?,
            ))
        }),
    )
}

fn check_catalog() -> catalog::Result<()> {
    let runtime = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
    runtime.block_on(async {
        let filters = catalog::Filters {
            term: "drum and bass drums".into(),
            category: "loop",
            ..Default::default()
        };
        let catalog = catalog::Catalog::new()?;
        let start = Instant::now();
        let first = catalog.search(&filters, 1).await?;
        println!(
            "Page 1: {} rows, {} matches, {} tags, {} ms",
            first.items.len(),
            first.total().unwrap_or_default(),
            first.tags().len(),
            start.elapsed().as_millis()
        );
        let start = Instant::now();
        let second = catalog.search(&filters, 2).await?;
        println!(
            "Page 2: {} rows in {} ms",
            second.items.len(),
            start.elapsed().as_millis()
        );
        if first.items.is_empty() || second.items.is_empty() {
            return Err("Expected matches on both catalog pages.".into());
        }
        if first
            .items
            .iter()
            .all(|row| second.items.iter().any(|other| other.uuid == row.uuid))
        {
            return Err("Catalog returned identical pages.".into());
        }
        let suggestions = catalog.suggestions("drum and ba").await?;
        println!("Autocomplete: {} suggestions", suggestions.len());
        let sample = first
            .items
            .iter()
            .find(|row| {
                row.preview().is_some()
                    && row.waveform().is_some()
                    && row.pack().and_then(|pack| pack.cover()).is_some()
            })
            .ok_or("No sample with a preview, waveform and cover on the first page.")?;
        let (wave, cover, clip) = tokio::join!(
            media::waveform(&catalog, sample.waveform().unwrap()),
            media::cover(&catalog, sample.pack().unwrap().cover().unwrap()),
            audio::Clip::fetch(&catalog, sample, true),
        );
        let media::ImageData::Wave(peaks) = wave? else {
            unreachable!()
        };
        let media::ImageData::Cover(cover) = cover? else {
            unreachable!()
        };
        let clip = clip?;
        let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
        let write_started = Instant::now();
        let exported = clip.export(temp.path(), sample, true)?;
        let write_ms = write_started.elapsed().as_millis();
        let wav = hound::WavReader::open(exported).map_err(|e| e.to_string())?;
        if wav.duration() == 0 {
            return Err("Exported WAV is empty.".into());
        }
        println!(
            "Preview: {} peaks, {}×{} cover, {:.2} s audio, {} Hz WAV written in {write_ms} ms",
            peaks.len(),
            cover.size[0],
            cover.size[1],
            clip.duration().as_secs_f32(),
            wav.spec().sample_rate
        );
        Ok(())
    })
}
