use crate::catalog::Result;
use std::path::PathBuf;

#[cfg(any(target_os = "macos", target_os = "windows"))]
pub fn start(frame: &eframe::Frame, path: PathBuf) -> Result<()> {
    drag::start_drag(
        frame,
        drag::DragItem::Files(vec![path]),
        drag::Image::Raw(include_bytes!("drag-preview.png").to_vec()),
        |_, _| {},
        Default::default(),
    )
    .map_err(|error| format!("Could not start dragging. {error}"))
}

#[cfg(not(any(target_os = "macos", target_os = "windows")))]
pub fn start(_: &eframe::Frame, _: PathBuf) -> Result<()> {
    Err("Use the exported WAV file from your file manager.".into())
}
