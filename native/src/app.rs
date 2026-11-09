use crate::{
    audio::{Clip, Player},
    catalog::{Catalog, Filters, PAGE_SIZE, Result, Sample, Tag},
    media::{self, ImageData},
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
use tokio::{runtime::Runtime, task::JoinHandle};

const DEBOUNCE: Duration = Duration::from_millis(240);
const MEDIA_CONCURRENCY: usize = 6;
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

enum Event {
    Search(u64, u32, Result<Page>),
    Suggest(u64, Vec<String>),
    Media(u64, MediaKey, Result<ImageData>),
    Audio(u64, Result<Clip>),
    Export(String, Result<PathBuf>),
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
    media_cache: LruCache<MediaKey, CachedMedia>,
    media_jobs: HashMap<MediaKey, (u64, Job)>,
    media_ticket: u64,
    selection: Option<Selection>,
    player: Player,
    audio_job: Option<Job>,
    audio_ticket: u64,
    exports: HashMap<String, Job>,
    exported: HashMap<String, PathBuf>,
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
            media_cache: LruCache::new(NonZeroUsize::new(192).unwrap()),
            media_jobs: HashMap::new(),
            media_ticket: 0,
            selection: None,
            player: Player::new(),
            audio_job: None,
            audio_ticket: 0,
            exports: HashMap::new(),
            exported: HashMap::new(),
            status: String::new(),
            demo,
        };
        app.queue_search(false, false);
        Ok(app)
    }

    fn queue_search(&mut self, debounced: bool, suggest: bool) {
        self.selected_collection = None;
        self.suppress_suggestions = !suggest;
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

    fn events(&mut self, ctx: &Context) {
        for _ in 0..32 {
            let Ok(event) = self.rx.try_recv() else {
                return;
            };
            match event {
                Event::Search(ticket, page, reply) => {
                    if ticket == self.search.ticket && page == 1 && reply.is_ok() {
                        self.reset_scroll = true;
                    }
                    self.search.apply(ticket, page, reply);
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
                                selected.clip = Some(Arc::new(clip));
                                let fraction = selected.fraction;
                                self.play_at(fraction, false);
                            }
                        }
                        Err(error) => self.status = error,
                    }
                }
                Event::Export(id, result) => {
                    self.exports.remove(&id);
                    match result {
                        Ok(path) => {
                            self.status = format!("WAV ready · {}", path.display());
                            self.exported.insert(id, path);
                        }
                        Err(error) => self.status = error,
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
        let catalog = self.worker.catalog.clone();
        let trim = self.prefs.trim_delay;
        let demo = self.demo;
        self.audio_job = Some(self.worker.spawn(async move {
            let result = if demo {
                Ok(Clip::demo())
            } else {
                Clip::fetch(&catalog, &row, trim).await
            };
            Event::Audio(ticket, result)
        }));
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
        let rows = self.rows();
        let index = self
            .selection
            .as_ref()
            .and_then(|selected| rows.iter().position(|row| row.uuid == selected.row.uuid));
        let target = index
            .map(|index| index.checked_add_signed(direction))
            .unwrap_or(Some(0));
        if let Some(row) = target.and_then(|index| rows.get(index)).cloned() {
            self.play_row(row, 0.0);
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

    fn queue_export(&mut self, row: Row) {
        if self.demo || self.exports.contains_key(&row.uuid) {
            return;
        }
        let directory = PathBuf::from(self.prefs.directory.trim());
        if !directory.is_absolute() {
            self.status = "Set an absolute export folder in Settings.".into();
            return;
        }
        if self.exports.len() >= 2 {
            self.status =
                "Two WAV previews are being prepared. Try again when one finishes.".into();
            return;
        }
        let catalog = self.worker.catalog.clone();
        let trim = self.prefs.trim_delay;
        let clip = self
            .selection
            .as_ref()
            .filter(|selected| selected.row.uuid == row.uuid)
            .and_then(|selected| selected.clip.clone());
        let id = row.uuid.clone();
        let task_id = id.clone();
        self.status = format!("Preparing WAV · {}", row.filename());
        let job = self.worker.spawn(async move {
            let result = async {
                let clip = match clip {
                    Some(clip) => clip,
                    None => Arc::new(Clip::fetch(&catalog, &row, trim).await?),
                };
                tokio::task::spawn_blocking(move || clip.export(&directory, &row))
                    .await
                    .map_err(|_| "WAV export was interrupted.".to_string())?
            }
            .await;
            Event::Export(task_id, result)
        });
        self.exports.insert(id, job);
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
        let mut show_suggestions = false;
        ui.horizontal(|ui| {
            let width = (ui.available_width() - 110.0).max(170.0);
            let search = ui.add_sized(
                [width, 34.0],
                TextEdit::singleline(&mut self.filters.term)
                    .hint_text("Search samples…")
                    .desired_width(width)
                    .char_limit(240),
            );
            if self.search_focus {
                search.request_focus();
                self.search_focus = false;
            }
            input_changed = search.changed();
            show_suggestions = !self.suppress_suggestions;
            submitted = (search.lost_focus() && ui.input(|i| i.key_pressed(Key::Enter)))
                || ui.add_sized([96.0, 34.0], Button::new("Search")).clicked();
            if show_suggestions && ui.input(|i| i.key_pressed(Key::Escape)) {
                self.suppress_suggestions = true;
                show_suggestions = false;
            }
        });

        if show_suggestions && !input_changed && !submitted && !self.search.suggestions.is_empty() {
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
            changed |= choices(
                ui,
                "key",
                &mut self.filters.key,
                &[
                    ("", "Any key"),
                    ("C", "C"),
                    ("C#", "C♯"),
                    ("D", "D"),
                    ("D#", "D♯"),
                    ("E", "E"),
                    ("F", "F"),
                    ("F#", "F♯"),
                    ("G", "G"),
                    ("G#", "G♯"),
                    ("A", "A"),
                    ("A#", "A♯"),
                    ("B", "B"),
                ],
            );
            changed |= choices(
                ui,
                "mode",
                &mut self.filters.mode,
                &[("", "Any mode"), ("major", "Major"), ("minor", "Minor")],
            );
            ui.label("BPM");
            for (value, hint) in [
                (&mut self.filters.min_bpm, "Min"),
                (&mut self.filters.max_bpm, "Max"),
            ] {
                let bpm = ui.add(
                    TextEdit::singleline(value)
                        .hint_text(hint)
                        .desired_width(44.0)
                        .char_limit(5),
                );
                changed |= bpm.changed();
            }
            ui.separator();
            changed |= choices(ui, "sort", &mut self.filters.sort, SORTS);
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
        });
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
                return;
            }
            let mut scroll = row_scroll();
            if std::mem::take(&mut self.reset_scroll) {
                scroll = scroll.vertical_scroll_offset(0.0);
            }
            let mut visible = Vec::new();
            let mut last = 0;
            scroll.show_rows(ui, ROW_HEIGHT, count, |ui, range| {
                last = range.end;
                visible = self.rows()[range.clone()].to_vec();
                for (i, row) in visible.iter().enumerate() {
                    self.sample_row(ui, frame, row, range.start + i, &columns);
                }
            });
            self.request_visible_media(&visible);
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
    ) {
        let (rect, _) =
            ui.allocate_exact_size(vec2(ui.available_width(), ROW_HEIGHT), Sense::hover());
        let mut row_ui = ui.new_child(UiBuilder::new().id_salt(&row.uuid).max_rect(rect));
        let ui = &mut row_ui;
        let active = self
            .selection
            .as_ref()
            .is_some_and(|s| s.row.uuid == row.uuid);
        if active {
            ui.painter().rect_filled(
                rect,
                4.0,
                ui.visuals().selection.bg_fill.gamma_multiply(0.62),
            );
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
        if ui
            .put(
                at(48.0, 12.0, 32.0, 32.0),
                Button::new(play_label).frame(false),
            )
            .on_hover_text("Play or pause preview")
            .clicked()
        {
            if active {
                self.toggle_play();
            } else {
                self.play_row(row.clone(), 0.0);
            }
        }
        let name_width = (c.wave - 95.0).max(75.0);
        let title = place_left(
            ui,
            at(86.0, 6.0, name_width, 24.0),
            Label::new(RichText::new(row.filename()).strong())
                .truncate()
                .sense(Sense::click()),
        );
        if title.on_hover_text(row.filename()).clicked() {
            self.play_row(row.clone(), 0.0);
        }
        let mut x = 86.0;
        for tag in row.tags.iter().take(4) {
            let width = (tag.label.chars().count() as f32 * 6.8 + 12.0).min(120.0);
            if x + width > c.wave - 5.0 {
                break;
            }
            if place_left(
                ui,
                at(x, 32.0, width, 19.0),
                Label::new(RichText::new(&tag.label).small().weak())
                    .truncate()
                    .sense(Sense::click()),
            )
            .on_hover_text("Filter by tag")
            .clicked()
                && !self.filters.tags.iter().any(|other| other.uuid == tag.uuid)
            {
                self.filters.tags.push(tag.clone());
                self.queue_search(false, false);
            }
            x += width + 4.0;
        }

        if c.wave_width >= 45.0 {
            let wave_rect = at(c.wave, 9.0, c.wave_width - 10.0, 38.0);
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
            let response =
                ui.interact(wave_rect, ui.id().with((&row.uuid, "wave")), Sense::click());
            if response.on_hover_text("Play from this point").clicked()
                && let Some(pos) = ui.input(|i| i.pointer.interact_pos())
            {
                self.play_row(
                    row.clone(),
                    ((pos.x - wave_rect.left()) / wave_rect.width()).clamp(0.0, 1.0),
                );
            }
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
        let ready = self.exported.get(&row.uuid).cloned();
        let busy = self.exports.contains_key(&row.uuid);
        let enabled = !busy && !self.demo;
        let label = if busy {
            "…"
        } else if ready.is_some() {
            "Drag"
        } else {
            "WAV"
        };
        let action = ui
            .scope_builder(
                UiBuilder::new().max_rect(at(c.actions + 32.0, 13.0, 50.0, 29.0)),
                |ui| ui.add_enabled(enabled, Button::new(label).sense(Sense::click_and_drag())),
            )
            .inner;
        let action = action.on_hover_text(if ready.is_some() {
            "Drag to your DAW or click to reveal in Finder"
        } else {
            "Prepare a WAV from the Splice preview"
        });
        if action.drag_started() || action.clicked() {
            if let Some(path) = ready.filter(|path| path.is_file()) {
                if action.drag_started() {
                    if let Err(error) = drag_file(frame, path) {
                        self.status = error;
                    }
                } else if let Err(error) = reveal_file(&path) {
                    self.status = error;
                }
            } else {
                self.exported.remove(&row.uuid);
                self.queue_export(row.clone());
            }
        }
        title_menu(
            ui,
            &row.uuid,
            at(c.actions + 83.0, 13.0, 26.0, 29.0),
            |ui| {
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
            },
        );
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
                if ui.button("⏮").on_hover_text("Previous sample").clicked() {
                    self.next(-1);
                }
                if ui
                    .button(if self.player.playing() { "II" } else { "▶" })
                    .on_hover_text("Play or pause · Space")
                    .clicked()
                {
                    self.toggle_play();
                }
                if ui.button("⏭").on_hover_text("Next sample").clicked() {
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
                        self.exported.clear();
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
                        self.exported.clear();
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
            self.reset_scroll = true;
        }
        if create || !open {
            self.collection_name = None;
        }
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &Context, frame: &mut eframe::Frame) {
        self.events(ctx);
        if ctx.input_mut(|i| i.consume_key(Modifiers::COMMAND, Key::F)) {
            self.selected_collection = None;
            self.search_focus = true;
        }
        if !ctx.wants_keyboard_input()
            && ctx.input_mut(|i| i.consume_key(Modifiers::NONE, Key::Space))
        {
            self.toggle_play();
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
        .width(98.0)
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
        let wave_width = ((time - 275.0) * 0.46).clamp(0.0, 250.0);
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

fn row_scroll() -> ScrollArea {
    ScrollArea::vertical()
        .id_salt("sample-rows")
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

#[cfg(any(target_os = "macos", target_os = "windows"))]
fn drag_file(frame: &eframe::Frame, path: PathBuf) -> Result<()> {
    // A transparent PNG lets Finder or the DAW show its own drag feedback.
    const ICON: &[u8] = &[
        137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 1, 0, 0, 0, 1, 8, 6,
        0, 0, 0, 31, 21, 196, 137, 0, 0, 0, 11, 73, 68, 65, 84, 120, 156, 98, 0, 2, 0, 0, 5, 0, 1,
        13, 45, 180, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66, 96, 130,
    ];
    drag::start_drag(
        frame,
        drag::DragItem::Files(vec![path]),
        drag::Image::Raw(ICON.to_vec()),
        |_, _| {},
        Default::default(),
    )
    .map_err(|error| format!("Could not start dragging. {error}"))
}

#[cfg(not(any(target_os = "macos", target_os = "windows")))]
fn drag_file(_: &eframe::Frame, _: PathBuf) -> Result<()> {
    Err("Use the exported WAV file from your file manager.".into())
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
