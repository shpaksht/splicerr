use crate::{
    audio::{Clip, Player},
    catalog::{Catalog, Filters, PAGE_SIZE, Result, Sample, Tag},
    media::{self, ImageData},
    sample_drag,
};
use eframe::egui::*;
use lru::LruCache;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    num::NonZeroUsize,
    path::PathBuf,
    sync::{Arc, mpsc},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{runtime::Runtime, sync::watch, task::JoinHandle};

const DEBOUNCE: Duration = Duration::from_millis(240);
const MEDIA_CONCURRENCY: usize = 6;
const PREVIEW_HOVER_DELAY: Duration = Duration::from_millis(120);
const MAX_PREFETCH_DURATION_MS: u32 = 20_000;
const ROW_HEIGHT: f32 = 58.0;
const LIKES: &str = "Likes";
const SORTS: &[(&str, &str)] = &[
    ("relevance", "Most relevant"),
    ("popularity", "Popular"),
    ("recency", "Newest"),
    ("name", "Filename"),
    ("duration", "Time"),
    ("key", "Key"),
    ("bpm", "BPM"),
    ("random", "Shuffle"),
];

type Row = Arc<Sample>;
type Job = JoinHandle<()>;
type PreviewRequest = watch::Receiver<Option<Result<Arc<Clip>>>>;
type ExportKey = (u64, String);

enum Event {
    Search(u64, u32, Result<Page>),
    Suggest(u64, Vec<String>),
    Media(u64, MediaKey, Result<ImageData>),
    Audio(u64, Result<Arc<Clip>>),
    Export(ExportKey, Result<PathBuf>),
}

struct Page {
    rows: Vec<Sample>,
    tags: Vec<Tag>,
    total: Option<u64>,
    more: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum MediaKey {
    Wave(String),
    Cover(String),
}

enum CachedMedia {
    Wave(Vec<f32>),
    Cover(TextureHandle),
    Unavailable,
}

struct Worker {
    runtime: Runtime,
    catalog: Catalog,
    tx: mpsc::Sender<Event>,
    ctx: Context,
}

impl Worker {
    fn spawn<F>(&self, future: F) -> Job
    where
        F: Future<Output = Event> + Send + 'static,
    {
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.runtime.spawn(async move {
            if tx.send(future.await).is_ok() {
                ctx.request_repaint();
            }
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
struct Preferences {
    directory: String,
    theme: String,
    volume: f32,
    repeat: bool,
    trim_delay: bool,
    collections: Vec<Collection>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct Collection {
    name: String,
    #[serde(default)]
    samples: Vec<Row>,
}

impl Default for Preferences {
    fn default() -> Self {
        let directory = dirs::download_dir()
            .or_else(dirs::home_dir)
            .map(|path| {
                path.join("Splicerr Previews")
                    .to_string_lossy()
                    .into_owned()
            })
            .unwrap_or_default();
        Self {
            directory,
            theme: "System".into(),
            volume: 0.8,
            repeat: true,
            trim_delay: true,
            collections: vec![Collection {
                name: LIKES.into(),
                samples: vec![],
            }],
        }
    }
}

impl Preferences {
    fn load(storage: Option<&dyn eframe::Storage>) -> Self {
        if let Some(saved) = storage.and_then(|storage| eframe::get_value(storage, "preferences")) {
            return saved;
        }
        let mut prefs = Self::default();
        let Some(folder) = dirs::data_dir().map(|dir| dir.join("de.kosro.splicerr")) else {
            return prefs;
        };

        #[derive(Deserialize)]
        struct LegacyConfig {
            samples_dir: Option<String>,
            ui_theme: Option<String>,
            repeat_audio: Option<bool>,
            cut_mp3_delay: Option<bool>,
        }
        if let Some(old) = read_json::<LegacyConfig>(folder.join("config.json")) {
            if let Some(directory) = old.samples_dir.filter(|path| !path.is_empty()) {
                prefs.directory = directory;
            }
            if let Some(theme) = old.ui_theme {
                prefs.theme = match theme.as_str() {
                    "dark" => "Dark",
                    "light" => "Light",
                    _ => "System",
                }
                .into();
            }
            prefs.repeat = old.repeat_audio.unwrap_or(prefs.repeat);
            prefs.trim_delay = old.cut_mp3_delay.unwrap_or(prefs.trim_delay);
        }
        if let Some(old) = read_json::<serde_json::Value>(folder.join("collections.json"))
            && let Some(collections) = old.get("collections").and_then(|items| items.as_array())
        {
            for item in collections {
                let Some(name) = item.get("name").and_then(|n| n.as_str()) else {
                    continue;
                };
                let samples = item
                    .get("samples")
                    .and_then(|value| {
                        if let Some(list) = value.as_array() {
                            Some(list.iter().collect::<Vec<_>>())
                        } else {
                            value.as_object().map(|items| items.values().collect())
                        }
                    })
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|value| serde_json::from_value(value.clone()).ok())
                    .collect::<Vec<Row>>();
                if name.eq_ignore_ascii_case(LIKES) {
                    prefs.collections[0].samples = samples;
                } else {
                    prefs.collections.push(Collection {
                        name: name.into(),
                        samples,
                    });
                }
            }
        }
        prefs
    }
}

fn read_json<T: serde::de::DeserializeOwned>(path: PathBuf) -> Option<T> {
    serde_json::from_slice(&std::fs::read(path).ok()?).ok()
}

#[derive(Default)]
struct SearchState {
    ticket: u64,
    pending: Option<Instant>,
    search: Option<Job>,
    suggest: Option<Job>,
    suggestions: Vec<String>,
    rows: Vec<Row>,
    seen: HashSet<String>,
    tags: Vec<Tag>,
    total: Option<u64>,
    next_page: u32,
    error: Option<String>,
}

impl SearchState {
    fn reset(&mut self, due: Instant) {
        self.ticket += 1;
        cancel(&mut self.search);
        cancel(&mut self.suggest);
        self.pending = Some(due);
        self.suggestions.clear();
        self.error = None;
        self.next_page = 1;
    }

    fn apply(&mut self, ticket: u64, page: u32, reply: Result<Page>) {
        if ticket != self.ticket {
            return;
        }
        self.search = None;
        match reply {
            Ok(reply) => {
                if page == 1 {
                    self.rows.clear();
                    self.seen.clear();
                    self.tags = reply.tags;
                    self.total = reply.total;
                }
                self.rows.extend(
                    reply
                        .rows
                        .into_iter()
                        .filter(|row| self.seen.insert(row.uuid.clone()))
                        .map(Arc::new),
                );
                self.next_page = if reply.more { page + 1 } else { 0 };
                self.error = None;
            }
            Err(error) => self.error = Some(error),
        }
    }
}

fn cancel(job: &mut Option<Job>) {
    if let Some(job) = job.take() {
        job.abort();
    }
}

enum WavExport {
    Ready(PathBuf),
    Preparing(ExportKey),
}

async fn await_preview(mut request: PreviewRequest) -> Result<Arc<Clip>> {
    request
        .wait_for(Option::is_some)
        .await
        .map_err(|_| "Preview loading was interrupted.".to_string())?
        .clone()
        .expect("wait_for returned a completed preview")
}

struct Selection {
    row: Row,
    clip: Option<Arc<Clip>>,
    fraction: f32,
}

pub struct App {
    worker: Worker,
    rx: mpsc::Receiver<Event>,
    filters: Filters,
    search: SearchState,
    prefs: Preferences,
    selected_collection: Option<usize>,
    collection_name: Option<String>,
    settings_open: bool,
    tags_expanded: bool,
    search_focus: bool,
    suppress_suggestions: bool,
    reset_scroll: bool,
    reveal_row: Option<usize>,
    row_offset: f32,
    row_view_height: f32,
    advance_after_page: bool,
    media_cache: LruCache<MediaKey, CachedMedia>,
    media_jobs: HashMap<MediaKey, (u64, Job)>,
    media_ticket: u64,
    selection: Option<Selection>,
    player: Player,
    audio_job: Option<Job>,
    audio_ticket: u64,
    drag_started: bool,
    pending_drag: Option<ExportKey>,
    hovered_sample: Option<(String, Instant)>,
    cached_preview: Option<(String, PreviewRequest)>,
    export_revision: u64,
    exports: HashMap<ExportKey, bool>,
    status: String,
    demo: bool,
}

impl App {
    pub fn new(cc: &eframe::CreationContext<'_>, demo: bool) -> Result<Self> {
        let (tx, rx) = mpsc::channel();
        let prefs = if demo {
            Preferences::default()
        } else {
            Preferences::load(cc.storage)
        };
        cc.egui_ctx.set_theme(theme(&prefs.theme));
        cc.egui_ctx.style_mut(|style| {
            style.spacing.item_spacing = vec2(7.0, 7.0);
            style.spacing.button_padding = vec2(10.0, 6.0);
            style
                .text_styles
                .insert(TextStyle::Body, FontId::proportional(14.0));
            style
                .text_styles
                .insert(TextStyle::Button, FontId::proportional(14.0));
        });
        let mut app = Self {
            worker: Worker {
                runtime: Runtime::new().map_err(|e| e.to_string())?,
                catalog: Catalog::new()?,
                tx,
                ctx: cc.egui_ctx.clone(),
            },
            rx,
            filters: Filters::default(),
            search: SearchState::default(),
            prefs,
            selected_collection: None,
            collection_name: None,
            settings_open: false,
            tags_expanded: false,
            search_focus: true,
            suppress_suggestions: true,
            reset_scroll: true,
            reveal_row: None,
            row_offset: 0.0,
            row_view_height: ROW_HEIGHT,
            advance_after_page: false,
            media_cache: LruCache::new(NonZeroUsize::new(192).unwrap()),
            media_jobs: HashMap::new(),
            media_ticket: 0,
            selection: None,
            player: Player::new(),
            audio_job: None,
            audio_ticket: 0,
            drag_started: false,
            pending_drag: None,
            hovered_sample: None,
            cached_preview: None,
            export_revision: 0,
            exports: HashMap::new(),
            status: String::new(),
            demo,
        };
        app.queue_search(false, false);
        Ok(app)
    }

    fn queue_search(&mut self, debounced: bool, suggest: bool) {
        self.selected_collection = None;
        self.suppress_suggestions = !suggest;
        self.hovered_sample = None;
        self.search
            .reset(Instant::now() + if debounced { DEBOUNCE } else { Duration::ZERO });
        self.worker.ctx.request_repaint();
    }

    fn start_page(&mut self, page: u32) {
        self.search.error = None;
        let filters = self.filters.clone();
        let ticket = self.search.ticket;
        let catalog = self.worker.catalog.clone();
        let demo = self.demo;
        self.search.search = Some(self.worker.spawn(async move {
            let reply = if demo {
                Ok(demo_page(&filters, page))
            } else {
                catalog.search(&filters, page).await.map(|result| Page {
                    tags: if page == 1 { result.tags() } else { vec![] },
                    total: result.total(),
                    more: result.has_more(page),
                    rows: result.items,
                })
            };
            Event::Search(ticket, page, reply)
        }));
    }

    fn start_due(&mut self, ctx: &Context) {
        let Some(due) = self.search.pending else {
            return;
        };
        if let Some(wait) = due.checked_duration_since(Instant::now()) {
            ctx.request_repaint_after(wait);
            return;
        }
        self.search.pending = None;
        self.start_page(1);
        if !self.demo && !self.suppress_suggestions && self.filters.term.trim().chars().count() >= 2
        {
            let term = self.filters.term.trim().to_owned();
            let ticket = self.search.ticket;
            let catalog = self.worker.catalog.clone();
            self.search.suggest = Some(self.worker.spawn(async move {
                Event::Suggest(ticket, catalog.suggestions(&term).await.unwrap_or_default())
            }));
        }
    }

    fn events(&mut self, ctx: &Context, frame: &eframe::Frame) {
        for _ in 0..32 {
            let Ok(event) = self.rx.try_recv() else {
                return;
            };
            match event {
                Event::Search(ticket, page, reply) => {
                    if ticket == self.search.ticket && page == 1 && reply.is_ok() {
                        self.reset_scroll = true;
                    }
                    let advance = ticket == self.search.ticket
                        && page > 1
                        && reply.is_ok()
                        && self.selected_collection.is_none()
                        && self.advance_after_page;
                    self.search.apply(ticket, page, reply);
                    if advance {
                        self.next(1);
                    }
                }
                Event::Suggest(ticket, values) if ticket == self.search.ticket => {
                    self.search.suggest = None;
                    self.search.suggestions = values;
                }
                Event::Media(ticket, key, reply) => {
                    if self
                        .media_jobs
                        .get(&key)
                        .is_none_or(|(current, _)| *current != ticket)
                    {
                        continue;
                    }
                    self.media_jobs.remove(&key);
                    let item = match reply {
                        Ok(ImageData::Wave(peaks)) => CachedMedia::Wave(peaks),
                        Ok(ImageData::Cover(image)) => CachedMedia::Cover(ctx.load_texture(
                            "pack cover",
                            image,
                            TextureOptions::LINEAR,
                        )),
                        Err(_) => CachedMedia::Unavailable,
                    };
                    self.media_cache.put(key, item);
                }
                Event::Audio(ticket, result) if ticket == self.audio_ticket => {
                    self.audio_job = None;
                    match result {
                        Ok(clip) => {
                            if let Some(selected) = &mut self.selection {
                                selected.clip = Some(clip);
                                let fraction = selected.fraction;
                                self.play_at(fraction, false);
                            }
                        }
                        Err(error) => self.status = error,
                    }
                }
                Event::Export(key, result) => {
                    let reveal = self.exports.remove(&key).unwrap_or(false);
                    let continue_drag = self.pending_drag.as_ref() == Some(&key);
                    if continue_drag {
                        self.pending_drag = None;
                    }
                    if key.0 != self.export_revision {
                        continue;
                    }
                    let result = result.and_then(|path| {
                        if continue_drag {
                            sample_drag::start(frame, path)
                        } else if reveal {
                            reveal_file(&path)
                        } else {
                            Ok(())
                        }
                    });
                    if let Err(error) = result {
                        self.status = error;
                    }
                }
                _ => {}
            }
        }
        ctx.request_repaint();
    }

    fn request_visible_media(&mut self, visible: &[Row]) {
        if self.demo {
            return;
        }
        let mut seen = HashSet::new();
        let keys = visible
            .iter()
            .flat_map(|row| {
                [
                    row.waveform().map(|url| MediaKey::Wave(url.into())),
                    row.pack()
                        .and_then(|pack| pack.cover())
                        .map(|url| MediaKey::Cover(url.into())),
                ]
                .into_iter()
                .flatten()
            })
            .filter(|key| seen.insert(key.clone()))
            .collect::<Vec<_>>();

        self.media_jobs.retain(|key, (_, job)| {
            let still_visible = seen.contains(key);
            if !still_visible {
                job.abort();
            }
            still_visible
        });
        for key in keys {
            if self.media_jobs.len() >= MEDIA_CONCURRENCY {
                break;
            }
            if self.media_cache.contains(&key) || self.media_jobs.contains_key(&key) {
                continue;
            }
            self.media_ticket += 1;
            let ticket = self.media_ticket;
            let catalog = self.worker.catalog.clone();
            let request = key.clone();
            let job = self.worker.spawn(async move {
                let result = match &request {
                    MediaKey::Wave(url) => media::waveform(&catalog, url).await,
                    MediaKey::Cover(url) => media::cover(&catalog, url).await,
                };
                Event::Media(ticket, request, result)
            });
            self.media_jobs.insert(key, (ticket, job));
        }
    }

    fn play_row(&mut self, row: Row, fraction: f32) {
        self.status.clear();
        if let Some(selected) = &mut self.selection
            && selected.row.uuid == row.uuid
        {
            selected.fraction = fraction;
            if selected.clip.is_some() {
                self.play_at(fraction, false);
                return;
            }
            if self.audio_job.is_some() {
                return;
            }
        }

        cancel(&mut self.audio_job);
        self.player.stop();
        self.selection = Some(Selection {
            row: row.clone(),
            clip: None,
            fraction,
        });
        self.audio_ticket += 1;
        let ticket = self.audio_ticket;
        let request = self.request_preview(&row);
        self.audio_job = Some(
            self.worker
                .spawn(async move { Event::Audio(ticket, await_preview(request).await) }),
        );
    }

    fn request_preview(&mut self, row: &Row) -> PreviewRequest {
        if let Some((id, request)) = &self.cached_preview
            && id == &row.uuid
            && !request.borrow().as_ref().is_some_and(Result::is_err)
        {
            return request.clone();
        }
        let (sender, request) = watch::channel(None);
        let catalog = self.worker.catalog.clone();
        let row = row.clone();
        let trim = self.prefs.trim_delay;
        let demo = self.demo;
        self.cached_preview = Some((row.uuid.clone(), request.clone()));
        self.worker.runtime.spawn(async move {
            let fetch = async {
                if demo {
                    Ok(Clip::demo())
                } else {
                    Clip::fetch(&catalog, &row, trim).await
                }
            };
            tokio::select! {
                _ = sender.closed() => {},
                result = fetch => {
                    let _ = sender.send(Some(result.map(Arc::new)));
                }
            }
        });
        request
    }

    fn prefetch_hovered_preview(&mut self, hovered: Option<&Row>, ctx: &Context) {
        let Some(row) = hovered.filter(|row| {
            !self.drag_started
                && row
                    .duration
                    .is_some_and(|ms| ms <= MAX_PREFETCH_DURATION_MS)
                && !self
                    .selection
                    .as_ref()
                    .is_some_and(|selected| selected.row.uuid == row.uuid)
        }) else {
            self.hovered_sample = None;
            return;
        };
        if self
            .cached_preview
            .as_ref()
            .is_some_and(|(id, _)| id == &row.uuid)
        {
            return;
        }
        let now = Instant::now();
        let deadline = match self.hovered_sample.as_ref() {
            Some((id, deadline)) if id == &row.uuid => *deadline,
            _ => {
                let deadline = now + PREVIEW_HOVER_DELAY;
                self.hovered_sample = Some((row.uuid.clone(), deadline));
                deadline
            }
        };
        if deadline > now {
            ctx.request_repaint_after(deadline - now);
        } else {
            self.request_preview(row);
        }
    }

    fn play_at(&mut self, fraction: f32, paused: bool) {
        let Some(clip) = self.selection.as_ref().and_then(|s| s.clip.clone()) else {
            return;
        };
        if let Err(error) =
            self.player
                .start(clip, fraction, self.prefs.volume, self.prefs.repeat, paused)
        {
            self.status = error;
        }
    }

    fn toggle_play(&mut self) {
        if self.player.toggle() {
            return;
        }
        if let Some(row) = self
            .selection
            .as_ref()
            .map(|s| s.row.clone())
            .or_else(|| self.rows().first().cloned())
        {
            self.play_row(row, 0.0);
        }
    }

    fn next(&mut self, direction: isize) {
        self.advance_after_page = false;
        let rows = self.rows();
        let index = self
            .selection
            .as_ref()
            .and_then(|selected| rows.iter().position(|row| row.uuid == selected.row.uuid));
        let target = index.map_or(Some(0), |index| index.checked_add_signed(direction));
        if let Some((index, row)) = target.and_then(|i| rows.get(i).cloned().map(|row| (i, row))) {
            self.reveal_row = Some(index);
            self.play_row(row, 0.0);
        } else if direction == 1
            && index.is_some()
            && self.selected_collection.is_none()
            && self.search.pending.is_none()
            && self.search.next_page > 1
            && self.search.error.is_none()
        {
            self.advance_after_page = true;
            if self.search.search.is_none() {
                self.start_page(self.search.next_page);
            }
        }
    }

    fn audition_row(&mut self, row: &Row, fraction: Option<f32>) {
        self.advance_after_page = false;
        if let Some(fraction) = fraction {
            self.play_row(row.clone(), fraction);
        } else if self
            .selection
            .as_ref()
            .is_some_and(|selection| selection.row.uuid == row.uuid)
        {
            self.toggle_play();
        } else {
            self.play_row(row.clone(), 0.0);
        }
    }

    fn rows(&self) -> &[Row] {
        if let Some(index) = self.selected_collection {
            &self.prefs.collections[index].samples
        } else {
            &self.search.rows
        }
    }

    fn toggle_like(&mut self, row: &Row) {
        let likes = &mut self.prefs.collections[0].samples;
        if let Some(index) = likes.iter().position(|other| other.uuid == row.uuid) {
            likes.remove(index);
        } else {
            likes.push(row.clone());
        }
    }

    fn prepare_wav(&mut self, row: &Row, reveal: bool) -> Result<WavExport> {
        self.status.clear();
        let directory = PathBuf::from(self.prefs.directory.trim());
        if !directory.is_absolute() {
            return Err("Set an absolute export folder in Settings.".into());
        }
        let trim = self.prefs.trim_delay;
        let path = crate::audio::preview_path(&directory, row, trim);
        if path.is_file() {
            let path = path.canonicalize().map_err(|error| error.to_string())?;
            if reveal {
                reveal_file(&path)?;
            }
            return Ok(WavExport::Ready(path));
        }

        let key = (self.export_revision, row.uuid.clone());
        if let Some(reveal_when_ready) = self.exports.get_mut(&key) {
            *reveal_when_ready |= reveal;
            return Ok(WavExport::Preparing(key));
        }
        if self.exports.len() >= 2 {
            return Err("Two WAV previews are being prepared. Try again when one finishes.".into());
        }
        let clip = self
            .selection
            .as_ref()
            .filter(|selected| selected.row.uuid == row.uuid)
            .and_then(|selected| selected.clip.clone());
        let request = clip.is_none().then(|| self.request_preview(row));
        let task_key = key.clone();
        let row = row.clone();
        self.worker.spawn(async move {
            let result = async {
                let clip = match request {
                    Some(request) => await_preview(request).await?,
                    None => clip.expect("the selected clip was already decoded"),
                };
                tokio::task::spawn_blocking(move || clip.export(&directory, &row, trim))
                    .await
                    .map_err(|_| "WAV export was interrupted.".to_string())?
            }
            .await;
            Event::Export(task_key, result)
        });
        self.exports.insert(key.clone(), reveal);
        Ok(WavExport::Preparing(key))
    }

    fn start_sample_drag(&mut self, frame: &eframe::Frame, row: &Row) {
        self.advance_after_page = false;
        let result = match self.prepare_wav(row, false) {
            Ok(WavExport::Ready(path)) => sample_drag::start(frame, path),
            Ok(WavExport::Preparing(key)) => {
                self.pending_drag = Some(key);
                Ok(())
            }
            Err(error) => Err(error),
        };
        if let Err(error) = result {
            self.status = error;
        }
    }

    fn sidebar(&mut self, ctx: &Context) {
        SidePanel::left("library")
            .exact_width(173.0)
            .resizable(false)
            .show(ctx, |ui| {
                ui.add_space(12.0);
                ui.label(RichText::new("LIBRARY").small().weak().strong());
                ui.add_space(3.0);
                if ui
                    .selectable_label(self.selected_collection.is_none(), "  Browse")
                    .clicked()
                {
                    self.selected_collection = None;
                    self.reveal_row = None;
                    self.reset_scroll = true;
                }
                ui.add_space(18.0);
                ui.horizontal(|ui| {
                    ui.label(RichText::new("COLLECTIONS").small().weak().strong());
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        if ui
                            .small_button("+")
                            .on_hover_text("New collection")
                            .clicked()
                        {
                            self.collection_name = Some(String::new());
                        }
                    });
                });
                ScrollArea::vertical()
                    .id_salt("collections")
                    .max_height((ui.available_height() - 62.0).max(40.0))
                    .show(ui, |ui| {
                        for (index, collection) in self.prefs.collections.iter().enumerate() {
                            let label =
                                format!("  {}  ·  {}", collection.name, collection.samples.len());
                            if ui
                                .selectable_label(self.selected_collection == Some(index), label)
                                .clicked()
                            {
                                self.selected_collection = Some(index);
                                self.advance_after_page = false;
                                self.reveal_row = None;
                                self.reset_scroll = true;
                                self.suppress_suggestions = true;
                            }
                        }
                    });
                ui.with_layout(Layout::bottom_up(Align::LEFT), |ui| {
                    ui.add_space(9.0);
                    if ui.button("Settings").clicked() {
                        self.settings_open = true;
                    }
                    ui.separator();
                });
            });
    }

    fn filters_ui(&mut self, ui: &mut Ui) {
        let mut changed = false;
        let mut submitted = false;
        let mut input_changed = false;
        ui.horizontal(|ui| {
            ui.spacing_mut().interact_size.y = 34.0;
            let width = (ui.available_width() - 372.0).max(130.0);
            let search = ui.add_sized(
                [width, 34.0],
                TextEdit::singleline(&mut self.filters.term)
                    .id(search_input_id())
                    .hint_text("Search samples…")
                    .desired_width(width)
                    .char_limit(240),
            );
            if std::mem::take(&mut self.search_focus) {
                search.request_focus();
            }
            input_changed = search.changed();
            submitted = search.lost_focus() && ui.input(|i| i.key_pressed(Key::Enter));
            if ui.input(|i| i.key_pressed(Key::Escape)) {
                self.suppress_suggestions = true;
                ui.memory_mut(|memory| memory.surrender_focus(search_input_id()));
            }
            changed |= choices(
                ui,
                "key",
                &mut self.filters.key,
                &[
                    ("", "Any key"),
                    ("C", "C"),
                    ("C#", "C#"),
                    ("D", "D"),
                    ("D#", "D#"),
                    ("E", "E"),
                    ("F", "F"),
                    ("F#", "F#"),
                    ("G", "G"),
                    ("G#", "G#"),
                    ("A", "A"),
                    ("A#", "A#"),
                    ("B", "B"),
                ],
            );
            changed |= choices(
                ui,
                "mode",
                &mut self.filters.mode,
                &[("", "Any mode"), ("major", "Major"), ("minor", "Minor")],
            );

            let min = self.filters.min_bpm.trim();
            let max = self.filters.max_bpm.trim();
            let bpm_label = match (min, max) {
                ("", "") => "Any BPM".into(),
                (min, "") => format!("{min}+ BPM"),
                ("", max) => format!("≤{max} BPM"),
                (min, max) if min == max => format!("{min} BPM"),
                (min, max) => format!("{min}–{max} BPM"),
            };
            ui.menu_button(bpm_label, |ui| {
                ui.label("Tempo range");
                ui.horizontal(|ui| {
                    for (value, hint) in [
                        (&mut self.filters.min_bpm, "Min"),
                        (&mut self.filters.max_bpm, "Max"),
                    ] {
                        changed |= ui
                            .add(
                                TextEdit::singleline(value)
                                    .hint_text(hint)
                                    .desired_width(52.0)
                                    .char_limit(3),
                            )
                            .changed();
                    }
                });
                if ui.button("Clear BPM").clicked() {
                    self.filters.min_bpm.clear();
                    self.filters.max_bpm.clear();
                    changed = true;
                    ui.close();
                }
            });
            changed |= choices(
                ui,
                "kind",
                &mut self.filters.category,
                &[
                    ("", "All sounds"),
                    ("loop", "Loops"),
                    ("oneshot", "One-shots"),
                ],
            );
        });

        if !self.suppress_suggestions
            && !input_changed
            && !submitted
            && !self.search.suggestions.is_empty()
        {
            ui.horizontal_wrapped(|ui| {
                for term in &self.search.suggestions {
                    if ui.small_button(term).clicked() {
                        self.filters.term.clone_from(term);
                        submitted = true;
                    }
                }
            });
        }
        ui.horizontal_wrapped(|ui| {
            if let Some((_, name)) = &self.filters.pack
                && ui.small_button(format!("Pack: {name}  ×")).clicked()
            {
                self.filters.pack = None;
                changed = true;
            }
            self.filters.tags.retain(|tag| {
                let remove = ui.small_button(format!("{}  ×", tag.label)).clicked();
                changed |= remove;
                !remove
            });
            let shown = if self.tags_expanded { 40 } else { 8 };
            for tag in self.search.tags.iter().take(shown) {
                if !self
                    .filters
                    .tags
                    .iter()
                    .any(|active| active.uuid == tag.uuid)
                    && ui.small_button(&tag.label).clicked()
                {
                    self.filters.tags.push(tag.clone());
                    changed = true;
                }
            }
            if self.search.tags.len() > 8
                && ui
                    .small_button(if self.tags_expanded {
                        "Fewer tags"
                    } else {
                        "More tags"
                    })
                    .clicked()
            {
                self.tags_expanded = !self.tags_expanded;
            }
        });
        if input_changed || changed || submitted {
            self.queue_search(!submitted, input_changed && !submitted);
        }
    }

    fn sort_ui(&mut self, ui: &mut Ui) {
        let mut changed = choices(ui, "sort", &mut self.filters.sort, SORTS);
        if ui
            .small_button(if self.filters.ascending {
                "Asc"
            } else {
                "Desc"
            })
            .on_hover_text("Reverse sort order")
            .clicked()
        {
            self.filters.ascending = !self.filters.ascending;
            changed = true;
        }
        if ui.small_button("Shuffle").clicked() {
            self.filters.sort = "random";
            self.filters.seed = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros()
                .to_string();
            changed = true;
        }
        if changed {
            self.queue_search(false, false);
        }
    }

    fn body(&mut self, ctx: &Context, frame: &eframe::Frame) {
        CentralPanel::default().show(ctx, |ui| {
            if self.selected_collection.is_none() {
                self.filters_ui(ui);
            } else if let Some(index) = self.selected_collection {
                ui.heading(&self.prefs.collections[index].name);
            }
            let browse = self.selected_collection.is_none();
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                if browse && (self.search.pending.is_some() || self.search.search.is_some()) {
                    ui.spinner();
                    ui.label(RichText::new("Searching Splice…").weak());
                } else if let Some(total) = browse.then_some(self.search.total).flatten() {
                    ui.label(
                        RichText::new(format!(
                            "{} results  ·  {} loaded",
                            grouped(total),
                            self.rows().len()
                        ))
                        .weak(),
                    );
                } else {
                    ui.label(RichText::new(format!("{} samples", self.rows().len())).weak());
                }
                if self.demo {
                    ui.label(
                        RichText::new("LOCAL DEMO")
                            .small()
                            .color(ui.visuals().hyperlink_color),
                    );
                }
                if browse {
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| self.sort_ui(ui));
                }
            });
            if browse && let Some(error) = self.search.error.clone() {
                ui.horizontal_wrapped(|ui| {
                    ui.colored_label(ui.visuals().error_fg_color, error);
                    if ui.button("Retry").clicked() {
                        self.start_page(self.search.next_page);
                    }
                });
            }
            ui.separator();
            let columns = Columns::new(ui.available_width());
            let (rect, _) =
                ui.allocate_exact_size(vec2(ui.available_width(), 24.0), Sense::hover());
            for (text, x) in [
                ("PACK", 3.0),
                ("FILENAME", 86.0),
                ("WAVEFORM", columns.wave),
                ("TIME", columns.time),
                ("KEY", columns.key),
                ("BPM", columns.bpm),
            ] {
                if x == columns.wave && columns.wave_width < 45.0 {
                    continue;
                }
                ui.painter().text(
                    rect.left_top() + vec2(x, 8.0),
                    Align2::LEFT_TOP,
                    text,
                    FontId::proportional(10.0),
                    ui.visuals().weak_text_color(),
                );
            }
            ui.separator();

            let count = self.rows().len();
            if count == 0 {
                if !browse
                    || (self.search.pending.is_none()
                        && self.search.search.is_none()
                        && self.search.error.is_none())
                {
                    ui.centered_and_justified(|ui| {
                        ui.label(if browse {
                            "No samples match these filters."
                        } else {
                            "This collection is empty."
                        });
                    });
                }
                self.request_visible_media(&[]);
                self.prefetch_hovered_preview(None, ctx);
                self.row_offset = 0.0;
                return;
            }
            let mut scroll = row_scroll();
            if std::mem::take(&mut self.reset_scroll) {
                self.row_offset = 0.0;
                scroll = scroll.vertical_scroll_offset(0.0);
            }
            if let Some(index) = self.reveal_row.take() {
                let pitch = ROW_HEIGHT + ui.spacing().item_spacing.y;
                let offset = offset_to_reveal(self.row_offset, self.row_view_height, pitch, index);
                scroll = scroll.vertical_scroll_offset(offset);
            }
            let mut visible = Vec::new();
            let mut hovered = None;
            let mut last = 0;
            let view = scroll.show_rows(ui, ROW_HEIGHT, count, |ui, range| {
                last = range.end;
                visible = self.rows()[range.clone()].to_vec();
                for (i, row) in visible.iter().enumerate() {
                    if self.sample_row(ui, frame, row, range.start + i, &columns) {
                        hovered = Some(row.clone());
                    }
                }
            });
            self.row_offset = view.state.offset.y;
            self.row_view_height = view.inner_rect.height();
            self.request_visible_media(&visible);
            self.prefetch_hovered_preview(hovered.as_ref(), ctx);
            if browse
                && self.search.pending.is_none()
                && self.search.search.is_none()
                && self.search.error.is_none()
                && self.search.next_page > 1
                && last + 9 >= count
            {
                self.start_page(self.search.next_page);
            }
        });
    }

    fn sample_row(
        &mut self,
        ui: &mut Ui,
        frame: &eframe::Frame,
        row: &Row,
        index: usize,
        c: &Columns,
    ) -> bool {
        let (rect, _) =
            ui.allocate_exact_size(vec2(ui.available_width(), ROW_HEIGHT), Sense::hover());
        let mut row_ui = ui.new_child(UiBuilder::new().id_salt(&row.uuid).max_rect(rect));
        let ui = &mut row_ui;
        let hovered = ui.rect_contains_pointer(rect);
        let waiting = self
            .pending_drag
            .as_ref()
            .is_some_and(|(_, id)| id == &row.uuid);
        let response = ui.interact(rect, ui.id().with("preview"), Sense::click_and_drag());
        let active = self
            .selection
            .as_ref()
            .is_some_and(|s| s.row.uuid == row.uuid);
        if active || waiting {
            ui.painter().rect_filled(
                rect,
                4.0,
                ui.visuals().selection.bg_fill.gamma_multiply(0.62),
            );
        } else if hovered {
            ui.painter()
                .rect_filled(rect, 4.0, ui.visuals().widgets.hovered.weak_bg_fill);
        } else if index.is_multiple_of(2) {
            ui.painter()
                .rect_filled(rect, 3.0, ui.visuals().faint_bg_color);
        }
        let at = |x, y, w, h| Rect::from_min_size(rect.min + vec2(x, y), vec2(w, h));
        let cover_rect = at(4.0, 8.0, 40.0, 40.0);
        ui.painter()
            .rect_filled(cover_rect, 5.0, ui.visuals().widgets.inactive.bg_fill);
        let cover = row.pack().and_then(|pack| pack.cover());
        if let Some(url) = cover
            && let Some(CachedMedia::Cover(texture)) =
                self.media_cache.get(&MediaKey::Cover(url.into()))
        {
            ui.painter().image(
                texture.id(),
                cover_rect,
                Rect::from_min_max(pos2(0.0, 0.0), pos2(1.0, 1.0)),
                Color32::WHITE,
            );
        } else {
            ui.painter().text(
                cover_rect.center(),
                Align2::CENTER_CENTER,
                "♫",
                FontId::proportional(18.0),
                ui.visuals().weak_text_color(),
            );
        }

        let play_label = if active && self.audio_job.is_some() {
            "…"
        } else if active && self.player.playing() {
            "II"
        } else {
            "▶"
        };
        ui.painter().text(
            at(48.0, 12.0, 32.0, 32.0).center(),
            Align2::CENTER_CENTER,
            play_label,
            FontId::proportional(14.0),
            ui.visuals().text_color(),
        );
        let name_width = (c.wave - 95.0).max(75.0);
        place_left(
            ui,
            at(86.0, 6.0, name_width, 24.0),
            Label::new(RichText::new(row.filename()).strong())
                .truncate()
                .selectable(false),
        )
        .on_hover_text(row.filename());
        let mut x = 86.0;
        for tag in row.tags.iter().take(4) {
            let width = (tag.label.chars().count() as f32 * 6.8 + 12.0).min(120.0);
            if x + width > c.wave - 5.0 {
                break;
            }
            place_left(
                ui,
                at(x, 32.0, width, 19.0),
                Label::new(RichText::new(&tag.label).small().weak())
                    .truncate()
                    .selectable(false),
            );
            x += width + 4.0;
        }

        let wave_rect = (c.wave_width >= 45.0).then(|| at(c.wave, 9.0, c.wave_width - 10.0, 38.0));
        if let Some(wave_rect) = wave_rect {
            let progress = self
                .selection
                .as_ref()
                .filter(|_| active)
                .and_then(|s| s.clip.as_ref())
                .map_or(0.0, |clip| self.player.position(clip, self.prefs.repeat));
            let wave = row
                .waveform()
                .and_then(|url| self.media_cache.get(&MediaKey::Wave(url.into())));
            if let Some(CachedMedia::Wave(peaks)) = wave {
                paint_wave(ui, wave_rect, peaks, progress);
            } else if self.demo {
                paint_wave(ui, wave_rect, &demo_wave(index), progress);
            } else {
                ui.painter().hline(
                    wave_rect.x_range(),
                    wave_rect.center().y,
                    ui.visuals().widgets.noninteractive.bg_stroke,
                );
            }
        }
        let response = response.on_hover_cursor(if waiting {
            CursorIcon::Progress
        } else {
            CursorIcon::Grab
        });
        if response.clicked() {
            let seek = response
                .interact_pointer_pos()
                .zip(wave_rect)
                .filter(|(pos, wave)| wave.contains(*pos))
                .map(|(pos, wave)| ((pos.x - wave.left()) / wave.width()).clamp(0.0, 1.0));
            self.audition_row(row, seek);
        }

        for (value, x) in [
            (
                row.duration
                    .map(|ms| format_time(ms / 1000))
                    .unwrap_or_else(|| "—".into()),
                c.time,
            ),
            (
                row.key
                    .as_ref()
                    .map(|key| {
                        format!(
                            "{key}{}",
                            if row.chord_type.as_deref() == Some("minor") {
                                "m"
                            } else {
                                ""
                            }
                        )
                    })
                    .unwrap_or_else(|| "—".into()),
                c.key,
            ),
            (
                row.bpm
                    .map(|bpm| bpm.to_string())
                    .unwrap_or_else(|| "—".into()),
                c.bpm,
            ),
        ] {
            ui.painter().text(
                rect.left_top() + vec2(x, ROW_HEIGHT * 0.5 - 1.0),
                Align2::LEFT_CENTER,
                value,
                FontId::proportional(13.0),
                ui.visuals().weak_text_color(),
            );
        }

        let liked = self.prefs.collections[0]
            .samples
            .iter()
            .any(|sample| sample.uuid == row.uuid);
        if ui
            .put(
                at(c.actions, 13.0, 31.0, 29.0),
                Button::new(if liked { "♥" } else { "♡" }).frame(false),
            )
            .on_hover_text("Toggle like")
            .clicked()
        {
            self.toggle_like(row);
        }
        let busy = self
            .exports
            .contains_key(&(self.export_revision, row.uuid.clone()));
        let drag_handle = ui
            .put(
                at(c.actions + 32.0, 13.0, 50.0, 29.0),
                Button::new(if busy { "…" } else { "WAV" }).sense(Sense::drag()),
            )
            .on_hover_cursor(if waiting {
                CursorIcon::Progress
            } else {
                CursorIcon::Grab
            })
            .on_hover_text("Drag WAV to your DAW");
        let like_rect = at(c.actions, 13.0, 31.0, 29.0);
        let menu_rect = at(c.actions + 83.0, 13.0, 26.0, 29.0);
        let drag_from_row = ui.input(|input| {
            let pointer = &input.pointer;
            let Some(origin) = pointer.press_origin() else {
                return false;
            };
            pointer.primary_down()
                && rect.intersect(ui.clip_rect()).contains(origin)
                && !like_rect.contains(origin)
                && !menu_rect.contains(origin)
                && (drag_handle.rect.contains(origin)
                    || pointer
                        .latest_pos()
                        .is_some_and(|pos| pos.distance(origin) >= 6.0))
        });
        if drag_from_row
            && !self.drag_started
            && !self.settings_open
            && self.collection_name.is_none()
            && !Popup::is_any_open(ui.ctx())
        {
            self.drag_started = true;
            self.start_sample_drag(frame, row);
        }
        title_menu(ui, &row.uuid, menu_rect, |ui| {
            if ui.button("Save WAV and show in Finder").clicked() {
                if let Err(error) = self.prepare_wav(row, true) {
                    self.status = error;
                }
                ui.close();
            }
            if let Some(pack) = row.pack() {
                if ui.button("Browse this pack").clicked() {
                    self.filters = Filters {
                        pack: Some((pack.uuid.clone(), pack.name.clone())),
                        ..Filters::default()
                    };
                    self.queue_search(false, false);
                    ui.close();
                }
                if let Some(url) = pack_url(pack)
                    && ui.button("Open pack on Splice").clicked()
                {
                    ui.ctx().open_url(OpenUrl::new_tab(url));
                    ui.close();
                }
            }
            if !row.tags.is_empty() {
                ui.menu_button("Filter by tag", |ui| {
                    for tag in &row.tags {
                        if ui.button(&tag.label).clicked() {
                            if !self
                                .filters
                                .tags
                                .iter()
                                .any(|active| active.uuid == tag.uuid)
                            {
                                self.filters.tags.push(tag.clone());
                                self.queue_search(false, false);
                            }
                            ui.close();
                        }
                    }
                });
            }
            for collection in self.prefs.collections.iter_mut().skip(1) {
                let position = collection.samples.iter().position(|s| s.uuid == row.uuid);
                let label = if position.is_some() {
                    format!("Remove from {}", collection.name)
                } else {
                    format!("Add to {}", collection.name)
                };
                if ui.button(label).clicked() {
                    if let Some(pos) = position {
                        collection.samples.remove(pos);
                    } else {
                        collection.samples.push(row.clone());
                    }
                    ui.close();
                }
            }
            if self.prefs.collections.len() == 1 {
                ui.label(RichText::new("Create a collection using +").weak());
            }
        });
        hovered
            && !self.settings_open
            && self.collection_name.is_none()
            && !Popup::is_any_open(ui.ctx())
    }

    fn player_ui(&mut self, ctx: &Context) {
        TopBottomPanel::bottom("player").show(ctx, |ui| {
            ui.add_space(4.0);
            if let Some(clip) = self.selection.as_ref().and_then(|s| s.clip.as_ref()) {
                let progress = self.player.position(clip, self.prefs.repeat);
                let (bar, response) =
                    ui.allocate_exact_size(vec2(ui.available_width(), 5.0), Sense::click());
                ui.painter()
                    .rect_filled(bar, 2.0, ui.visuals().widgets.inactive.bg_fill);
                let played = Rect::from_min_max(
                    bar.min,
                    pos2(bar.left() + bar.width() * progress, bar.bottom()),
                );
                ui.painter()
                    .rect_filled(played, 2.0, ui.visuals().selection.stroke.color);
                if response.clicked()
                    && let Some(pointer) = response.interact_pointer_pos()
                {
                    self.play_at(
                        (pointer.x - bar.left()) / bar.width(),
                        !self.player.playing(),
                    );
                }
            }
            ui.horizontal(|ui| {
                if ui
                    .button("⏮")
                    .on_hover_text("Previous sample · Up")
                    .clicked()
                {
                    self.next(-1);
                }
                if ui
                    .button(if self.player.playing() { "II" } else { "▶" })
                    .on_hover_text("Play or pause · Space")
                    .clicked()
                {
                    self.toggle_play();
                }
                if ui.button("⏭").on_hover_text("Next sample · Down").clicked() {
                    self.next(1);
                }
                ui.separator();
                let width = (ui.available_width() - 240.0).max(100.0);
                let title = self
                    .selection
                    .as_ref()
                    .map(|s| s.row.filename())
                    .unwrap_or("Select a sample to preview");
                ui.add_sized(
                    [width, 28.0],
                    Label::new(RichText::new(title).strong()).truncate(),
                );
                if ui.toggle_value(&mut self.prefs.repeat, "Loop").changed()
                    && let Some(clip) = self.selection.as_ref().and_then(|s| s.clip.as_ref())
                {
                    let fraction = self.player.position(clip, !self.prefs.repeat);
                    self.play_at(fraction, !self.player.playing());
                }
                ui.spacing_mut().slider_width = 88.0;
                if ui
                    .add(
                        Slider::new(&mut self.prefs.volume, 0.0..=1.0)
                            .show_value(false)
                            .text("Volume"),
                    )
                    .changed()
                {
                    self.player.volume(self.prefs.volume);
                }
            });
            if !self.status.is_empty() {
                ui.add(Label::new(RichText::new(&self.status).small().weak()).truncate());
            }
            ui.add_space(4.0);
        });
    }

    fn dialogs(&mut self, ctx: &Context) {
        let mut open = self.settings_open;
        if open {
            Window::new("Settings")
                .open(&mut open)
                .collapsible(false)
                .resizable(false)
                .show(ctx, |ui| {
                    ui.label("WAV preview folder");
                    if ui
                        .add(TextEdit::singleline(&mut self.prefs.directory).desired_width(420.0))
                        .changed()
                    {
                        self.export_revision += 1;
                    }
                    ui.label(
                        RichText::new("Exports use the audio preview available from Splice.")
                            .weak(),
                    );
                    let before = self.prefs.theme.clone();
                    ComboBox::from_label("Appearance")
                        .selected_text(&self.prefs.theme)
                        .show_ui(ui, |ui| {
                            for theme in ["System", "Light", "Dark"] {
                                ui.selectable_value(&mut self.prefs.theme, theme.into(), theme);
                            }
                        });
                    if self.prefs.theme != before {
                        ctx.set_theme(theme(&self.prefs.theme));
                    }
                    if ui
                        .checkbox(&mut self.prefs.trim_delay, "Trim legacy MP3 preview delay")
                        .changed()
                    {
                        self.export_revision += 1;
                        self.cached_preview = None;
                        cancel(&mut self.audio_job);
                        self.audio_ticket += 1;
                        self.player.stop();
                        if let Some(selected) = &mut self.selection {
                            selected.clip = None;
                        }
                    }
                });
        }
        self.settings_open = open;

        let Some(name) = self.collection_name.as_mut() else {
            return;
        };
        let mut create = false;
        let mut open = true;
        Window::new("New collection")
            .open(&mut open)
            .collapsible(false)
            .resizable(false)
            .show(ctx, |ui| {
                let field = ui.add(TextEdit::singleline(name).hint_text("Name").char_limit(60));
                let valid = !name.trim().is_empty()
                    && !self
                        .prefs
                        .collections
                        .iter()
                        .any(|c| c.name.eq_ignore_ascii_case(name.trim()));
                create = ui.add_enabled(valid, Button::new("Create")).clicked()
                    || (valid && field.lost_focus() && ui.input(|i| i.key_pressed(Key::Enter)));
            });
        if create {
            self.prefs.collections.push(Collection {
                name: name.trim().to_owned(),
                samples: vec![],
            });
            self.selected_collection = Some(self.prefs.collections.len() - 1);
            self.advance_after_page = false;
            self.reveal_row = None;
            self.reset_scroll = true;
        }
        if create || !open {
            self.collection_name = None;
        }
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &Context, frame: &mut eframe::Frame) {
        if ctx.input(|i| {
            !i.pointer.primary_down() || i.pointer.button_pressed(PointerButton::Primary)
        }) {
            self.drag_started = false;
            self.pending_drag = None;
        } else if ctx.input(|i| i.key_pressed(Key::Escape)) {
            self.pending_drag = None;
        }
        self.events(ctx, frame);
        if ctx.input_mut(|i| i.consume_key(Modifiers::COMMAND, Key::F)) {
            self.selected_collection = None;
            self.search_focus = true;
        }
        let search_editing = ctx.memory(|memory| memory.has_focus(search_input_id()));
        if !self.settings_open && self.collection_name.is_none() && !Popup::is_any_open(ctx) {
            let down = ctx.input_mut(|i| i.consume_key(Modifiers::NONE, Key::ArrowDown));
            let up = ctx.input_mut(|i| i.consume_key(Modifiers::NONE, Key::ArrowUp));
            if (down || up) && !self.rows().is_empty() {
                if search_editing {
                    ctx.memory_mut(|memory| memory.surrender_focus(search_input_id()));
                    self.suppress_suggestions = true;
                }
                self.next(if down { 1 } else { -1 });
            }
            if !search_editing && ctx.input_mut(|i| i.consume_key(Modifiers::NONE, Key::Space)) {
                self.toggle_play();
            }
        }
        self.start_due(ctx);
        self.player_ui(ctx);
        self.sidebar(ctx);
        self.body(ctx, frame);
        self.dialogs(ctx);
        if self.player.playing() {
            ctx.request_repaint_after(Duration::from_millis(33));
        }
    }

    fn save(&mut self, storage: &mut dyn eframe::Storage) {
        if !self.demo {
            eframe::set_value(storage, "preferences", &self.prefs);
        }
    }
}

fn choices(
    ui: &mut Ui,
    id: &str,
    selected: &mut &'static str,
    choices: &[(&'static str, &'static str)],
) -> bool {
    let before = *selected;
    let text = choices
        .iter()
        .find(|(value, _)| value == selected)
        .map_or(choices[0].1, |(_, label)| *label);
    ComboBox::from_id_salt(id)
        .selected_text(text)
        .width(if id == "sort" { 112.0 } else { 80.0 })
        .show_ui(ui, |ui| {
            for (value, label) in choices {
                ui.selectable_value(selected, *value, *label);
            }
        });
    before != *selected
}

fn place_left(ui: &mut Ui, rect: Rect, label: Label) -> Response {
    ui.new_child(
        UiBuilder::new()
            .max_rect(rect)
            .layout(Layout::left_to_right(Align::Center)),
    )
    .add(label)
}

fn title_menu(ui: &mut Ui, id: &str, rect: Rect, contents: impl FnOnce(&mut Ui)) {
    ui.scope_builder(UiBuilder::new().id_salt(id).max_rect(rect), |ui| {
        ui.menu_button("...", contents);
    });
}

fn theme(value: &str) -> ThemePreference {
    match value {
        "Dark" => ThemePreference::Dark,
        "Light" => ThemePreference::Light,
        _ => ThemePreference::System,
    }
}

fn pack_url(pack: &crate::catalog::Pack) -> Option<String> {
    let provider = pack.permalink_base_url.as_deref()?;
    let slug = pack.permalink_slug.as_deref()?;
    let mut url = reqwest::Url::parse("https://splice.com/sounds/packs/").ok()?;
    url.path_segments_mut()
        .ok()?
        .pop_if_empty()
        .push(provider)
        .push(slug);
    Some(url.into())
}

fn grouped(value: u64) -> String {
    let digits = value.to_string();
    let mut groups = digits
        .as_bytes()
        .rchunks(3)
        .map(|chunk| std::str::from_utf8(chunk).unwrap())
        .collect::<Vec<_>>();
    groups.reverse();
    groups.join(",")
}

fn format_time(seconds: u32) -> String {
    format!("{}:{:02}", seconds / 60, seconds % 60)
}

struct Columns {
    wave: f32,
    wave_width: f32,
    time: f32,
    key: f32,
    bpm: f32,
    actions: f32,
}

impl Columns {
    fn new(width: f32) -> Self {
        let actions = width - 112.0;
        let bpm = actions - 41.0;
        let key = bpm - 41.0;
        let time = key - 52.0;
        let wave_width = ((time - 212.0) * 0.5).clamp(0.0, 250.0);
        Self {
            wave: time - wave_width,
            wave_width,
            time,
            key,
            bpm,
            actions,
        }
    }
}

fn search_input_id() -> Id {
    Id::new("browse-search-input")
}

fn offset_to_reveal(offset: f32, height: f32, pitch: f32, row: usize) -> f32 {
    let top = row as f32 * pitch;
    offset.clamp((top + ROW_HEIGHT - height).min(top).max(0.0), top)
}

fn row_scroll() -> ScrollArea {
    ScrollArea::vertical()
        .id_salt("sample-rows")
        .scroll_source(scroll_area::ScrollSource {
            drag: false,
            ..Default::default()
        })
        .auto_shrink([false, false])
}

fn paint_wave(ui: &Ui, rect: Rect, peaks: &[f32], progress: f32) {
    let width = rect.width() / peaks.len() as f32;
    let muted = ui.visuals().weak_text_color();
    let accent = ui.visuals().selection.stroke.color;
    let stroke_width = width.clamp(0.7, 1.8);
    let boundary = rect.left() + rect.width() * progress;
    let mut mesh = Mesh::default();
    for (i, peak) in peaks.iter().enumerate() {
        let x = rect.left() + (i as f32 + 0.5) * width;
        let amplitude = (peak * rect.height() * 0.5).max(0.5);
        let bar = Rect::from_center_size(
            pos2(x, rect.center().y),
            vec2(stroke_width, 2.0 * amplitude),
        );
        mesh.add_colored_rect(bar, if x <= boundary { accent } else { muted });
    }
    ui.painter().add(Shape::mesh(mesh));
}

fn demo_wave(index: usize) -> [f32; 96] {
    std::array::from_fn(|i| {
        let cycle = (i as f32 / (11 + index % 14) as f32).fract();
        let ripple = ((i * 17 + index * 7) % 29) as f32 / 29.0;
        (1.0 - cycle).powi(3) * (0.36 + ripple * 0.58) + 0.04
    })
}

fn demo_page(filters: &Filters, page: u32) -> Page {
    let start = (page - 1) * PAGE_SIZE;
    let rows = (start..start + PAGE_SIZE)
        .map(|i| Sample {
            uuid: format!("demo-{i}"),
            name: format!(
                "{}_{}_{i:05}.wav",
                if filters.term.trim().is_empty() {
                    "drum_and_bass"
                } else {
                    filters.term.trim()
                },
                ["kick", "snare", "tops", "break", "hats"][(i % 5) as usize]
            ),
            duration: Some(2400 + i % 7000),
            bpm: Some(168 + (i % 9) as u16),
            tags: ["drum and bass", "drums", "breaks", "grooves"]
                .into_iter()
                .map(|tag| Tag {
                    uuid: tag.into(),
                    label: tag.into(),
                })
                .collect(),
            ..Sample::default()
        })
        .collect();
    Page {
        rows,
        tags: vec![],
        total: Some(115_323),
        more: page < 2307,
    }
}

fn reveal_file(path: &std::path::Path) -> Result<()> {
    let mut command = if cfg!(target_os = "macos") {
        let mut command = std::process::Command::new("open");
        command.arg("-R").arg(path);
        command
    } else if cfg!(target_os = "windows") {
        let mut command = std::process::Command::new("explorer.exe");
        command.arg(format!("/select,{}", path.display()));
        command
    } else {
        let mut command = std::process::Command::new("xdg-open");
        command.arg(path.parent().unwrap_or(path));
        command
    };
    command
        .spawn()
        .map(|_| ())
        .map_err(|e| format!("Could not reveal the WAV file. {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_search_keeps_matches_ignores_stale_replies_and_deduplicates_pages() {
        let page = |ids: &[&str]| Page {
            rows: ids
                .iter()
                .map(|id| Sample {
                    uuid: id.to_string(),
                    ..Sample::default()
                })
                .collect(),
            tags: vec![],
            total: Some(3),
            more: true,
        };
        let mut search = SearchState::default();
        search.apply(0, 1, Ok(page(&["same", "old"])));
        search.reset(Instant::now());
        search.apply(0, 2, Err("stale error".into()));
        search.apply(1, 1, Ok(page(&["same", "new"])));
        search.apply(1, 2, Ok(page(&["new", "another"])));
        assert_eq!(
            search
                .rows
                .iter()
                .map(|r| r.uuid.as_str())
                .collect::<Vec<_>>(),
            ["same", "new", "another"]
        );
        assert_eq!(search.next_page, 3);
        assert!(search.error.is_none());
    }

    #[test]
    fn keyboard_navigation_scrolls_only_as_far_as_the_next_row_needs() {
        let pitch = ROW_HEIGHT + 7.0;
        assert_eq!(offset_to_reveal(0.0, 300.0, pitch, 2), 0.0);
        assert_eq!(offset_to_reveal(0.0, 300.0, pitch, 5), 83.0);
        assert_eq!(offset_to_reveal(160.0, 300.0, pitch, 1), pitch);
        assert_eq!(offset_to_reveal(160.0, 300.0, pitch, 0), 0.0);
    }

    #[test]
    fn scrolling_renders_only_a_small_window_of_115323_rows() {
        let ctx = Context::default();
        let count = 115_323;
        for offset in [0.0, 250_000.0, 2_500_000.0] {
            let input = RawInput {
                screen_rect: Some(Rect::from_min_size(Pos2::ZERO, vec2(1200.0, 760.0))),
                ..Default::default()
            };
            let _ = ctx.run(input, |ctx| {
                CentralPanel::default().show(ctx, |ui| {
                    row_scroll().vertical_scroll_offset(offset).show_rows(
                        ui,
                        ROW_HEIGHT,
                        count,
                        |ui, range| {
                            assert!(range.len() < 20, "rendered {} rows", range.len());
                            if offset > 0.0 {
                                assert!(range.start > 1_000);
                            }
                            for _ in range {
                                ui.allocate_exact_size(
                                    vec2(ui.available_width(), ROW_HEIGHT),
                                    Sense::hover(),
                                );
                            }
                        },
                    );
                });
            });
        }
    }
}
