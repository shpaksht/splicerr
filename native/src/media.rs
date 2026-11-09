use crate::catalog::{Catalog, Result};
use eframe::egui::ColorImage;
use std::io::Cursor;

pub const PEAKS: usize = 144;
const MAX_WAVE_BYTES: usize = 128 * 1024;
const MAX_COVER_BYTES: usize = 4 * 1024 * 1024;

pub enum ImageData {
    Wave(Vec<f32>),
    Cover(ColorImage),
}

pub async fn waveform(catalog: &Catalog, url: &str) -> Result<ImageData> {
    let bytes = catalog.media(url, MAX_WAVE_BYTES).await?;
    let samples: Vec<f32> =
        serde_json::from_slice(&bytes).map_err(|_| "Invalid waveform response.".to_string())?;
    if samples.is_empty() {
        return Err("Waveform is empty.".into());
    }
    let count = samples.len().min(PEAKS);
    let peaks = (0..count)
        .map(|i| {
            let from = i * samples.len() / count;
            let to = (i + 1) * samples.len() / count;
            samples[from..to]
                .iter()
                .fold(0.0_f32, |peak, value| peak.max(value.abs()))
                .min(1.0)
        })
        .collect();
    Ok(ImageData::Wave(peaks))
}

pub async fn cover(catalog: &Catalog, url: &str) -> Result<ImageData> {
    let bytes = catalog.media(url, MAX_COVER_BYTES).await?;
    tokio::task::spawn_blocking(move || {
        let mut reader = image::ImageReader::new(Cursor::new(bytes))
            .with_guessed_format()
            .map_err(|_| "Invalid cover image.".to_string())?;
        let mut limits = image::Limits::default();
        limits.max_alloc = Some(32 * 1024 * 1024);
        limits.max_image_width = Some(2400);
        limits.max_image_height = Some(2400);
        reader.limits(limits);
        let image = reader
            .decode()
            .map_err(|_| "Invalid cover image.".to_string())?
            .thumbnail(96, 96)
            .into_rgba8();
        let size = [image.width() as usize, image.height() as usize];
        Ok(ImageData::Cover(ColorImage::from_rgba_unmultiplied(
            size,
            image.as_raw(),
        )))
    })
    .await
    .map_err(|_| "Cover loading was interrupted.".to_string())?
}
