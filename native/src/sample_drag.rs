use crate::catalog::Result;
use std::{
    path::PathBuf,
    sync::{Arc, Condvar, Mutex},
};

#[derive(Default)]
pub struct PreparedWav {
    result: Mutex<Option<Result<PathBuf>>>,
    changed: Condvar,
}

impl PreparedWav {
    pub fn ready(path: PathBuf) -> Self {
        Self {
            result: Mutex::new(Some(Ok(path))),
            ..Self::default()
        }
    }

    pub fn finish(&self, result: Result<PathBuf>) {
        *self.result.lock().unwrap() = Some(result);
        self.changed.notify_all();
    }

    #[cfg(any(target_os = "macos", target_os = "windows"))]
    fn path(&self) -> Option<PathBuf> {
        self.result.lock().unwrap().as_ref()?.as_ref().ok().cloned()
    }

    #[cfg(target_os = "macos")]
    fn wait_for_path(&self) -> Option<PathBuf> {
        // AppKit can request the URL while a new WAV is being prepared. A failed
        // or slow download must not stall the system drag session indefinitely.
        let result = self
            .changed
            .wait_timeout_while(
                self.result.lock().unwrap(),
                std::time::Duration::from_secs(3),
                |result| result.is_none(),
            )
            .ok()?
            .0;
        result.as_ref()?.as_ref().ok().cloned()
    }
}

#[cfg(any(target_os = "macos", target_os = "windows"))]
pub fn start(frame: &eframe::Frame, file: Arc<PreparedWav>) -> Result<()> {
    let item = if let Some(path) = file.path() {
        drag::DragItem::Files(vec![path])
    } else {
        #[cfg(target_os = "macos")]
        {
            drag::DragItem::Data {
                types: vec!["public.file-url".into()],
                provider: Box::new(move |_| {
                    let url = reqwest::Url::from_file_path(file.wait_for_path()?).ok()?;
                    Some(url.as_str().as_bytes().to_vec())
                }),
            }
        }
        #[cfg(target_os = "windows")]
        return Err("WAV is being prepared. Drag it again when ready.".into());
    };
    drag::start_drag(
        frame,
        item,
        drag::Image::Raw(include_bytes!("drag-preview.png").to_vec()),
        |_, _| {},
        Default::default(),
    )
    .map_err(|error| format!("Could not start dragging. {error}"))
}

#[cfg(not(any(target_os = "macos", target_os = "windows")))]
pub fn start(_: &eframe::Frame, _: Arc<PreparedWav>) -> Result<()> {
    Err("Use the exported WAV file from your file manager.".into())
}
