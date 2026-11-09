use lru::LruCache;
use reqwest::{StatusCode, Url};
use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};

pub type Result<T> = std::result::Result<T, String>;
pub const PAGE_SIZE: u32 = 50;
const GRAPHQL: &str = "https://surfaces-graphql.splice.com/graphql";
const SEARCH: &str = r#"
query SampleCatalog($query: String, $tags: [ID], $key: String, $mode: String,
  $min: Int, $max: Int, $category: AssetCategorySlug, $pack: GUID,
  $sort: AssetSortType, $order: SortOrder, $seed: String, $page: Int, $limit: Int) {
  assetsSearch(
    filter: {legacy: true, published: true, asset_type_slug: sample, query: $query,
      tag_ids: $tags, key: $key, chord_type: $mode, min_bpm: $min, max_bpm: $max,
      asset_category_slug: $category}
    children: {parent_asset_uuid: $pack}
    legacy: {parent_asset_type: pack}
    pagination: {page: $page, limit: $limit}
    sort: {sort: $sort, order: $order, random_seed: $seed}
  ) {
    items {
      ... on SampleAsset {
        uuid name duration bpm key chord_type asset_category_slug
        tags { uuid label }
        files { asset_file_type_slug url }
        parents(filter: {asset_type_slug: pack}) {
          items {
            ... on PackAsset {
              uuid name permalink_slug permalink_base_url
              files { asset_file_type_slug url }
            }
          }
        }
      }
    }
    tag_summary { tag { uuid label } }
    pagination_metadata { currentPage totalPages }
    response_metadata { records }
  }
}
"#;
const PREVIEW_LOOKUP: &str = r#"
query SamplePreview($id: GUID!) {
  asset(uuid: $id, legacy: {type: sample, searchCatalog: true}) {
    ... on SampleAsset { uuid name files { asset_file_type_slug url } }
  }
}
"#;
const SUGGEST: &str = r#"
query CatalogSuggestions($term: String!) {
  soundsSearchSuggestions(searchTerm: $term, limit: 7, context: "marketplace") {
    results { autocompleteTerm }
  }
}
"#;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Tag {
    pub uuid: String,
    pub label: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct MediaFile {
    pub asset_file_type_slug: Option<String>,
    pub url: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Pack {
    pub uuid: String,
    pub name: String,
    pub permalink_slug: Option<String>,
    pub permalink_base_url: Option<String>,
    #[serde(default, deserialize_with = "null_default")]
    pub files: Vec<MediaFile>,
}

impl Pack {
    pub fn cover(&self) -> Option<&str> {
        ["thumbnail_image", "cover_image", "generated_cover_image"]
            .into_iter()
            .find_map(|kind| file_url(&self.files, kind))
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Parents {
    #[serde(default, deserialize_with = "null_default")]
    items: Vec<Pack>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Sample {
    pub uuid: String,
    pub name: String,
    pub duration: Option<u32>,
    pub bpm: Option<u16>,
    pub key: Option<String>,
    pub chord_type: Option<String>,
    pub asset_category_slug: Option<String>,
    #[serde(default, deserialize_with = "null_default")]
    pub tags: Vec<Tag>,
    #[serde(default, deserialize_with = "null_default")]
    pub files: Vec<MediaFile>,
    #[serde(default, deserialize_with = "null_default")]
    pub parents: Parents,
}

impl Sample {
    pub fn filename(&self) -> &str {
        self.name.rsplit('/').next().unwrap_or(&self.name)
    }

    pub fn pack(&self) -> Option<&Pack> {
        self.parents.items.first()
    }

    pub fn preview(&self) -> Option<&str> {
        file_url(&self.files, "preview_mp3")
    }

    pub fn waveform(&self) -> Option<&str> {
        file_url(&self.files, "waveform")
    }
}

fn file_url<'a>(files: &'a [MediaFile], kind: &str) -> Option<&'a str> {
    files
        .iter()
        .filter(|file| file.asset_file_type_slug.as_deref() == Some(kind))
        .find_map(|file| file.url.as_deref())
}

fn null_default<'de, D, T>(deserializer: D) -> std::result::Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Filters {
    pub term: String,
    pub tags: Vec<Tag>,
    pub category: &'static str,
    pub key: &'static str,
    pub mode: &'static str,
    pub min_bpm: String,
    pub max_bpm: String,
    pub sort: &'static str,
    pub ascending: bool,
    pub seed: String,
    pub pack: Option<(String, String)>,
}

#[derive(Deserialize)]
pub struct SearchPage {
    #[serde(default, deserialize_with = "null_default")]
    pub items: Vec<Sample>,
    #[serde(default, deserialize_with = "null_default")]
    tag_summary: Vec<TagEntry>,
    pagination_metadata: Option<Pagination>,
    response_metadata: Option<Metadata>,
}

#[derive(Deserialize)]
struct TagEntry {
    tag: Option<Tag>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Pagination {
    current_page: Option<u32>,
    total_pages: Option<u32>,
}

#[derive(Deserialize)]
struct Metadata {
    records: Option<u64>,
}

impl SearchPage {
    pub fn tags(&self) -> Vec<Tag> {
        self.tag_summary
            .iter()
            .filter_map(|item| item.tag.clone())
            .collect()
    }

    pub fn total(&self) -> Option<u64> {
        self.response_metadata.as_ref()?.records
    }

    pub fn has_more(&self, requested: u32) -> bool {
        if self.items.is_empty() {
            return false;
        }
        let Some(pagination) = &self.pagination_metadata else {
            return self.items.len() == PAGE_SIZE as usize;
        };
        let current = pagination.current_page.unwrap_or(requested);
        pagination
            .total_pages
            .map_or(self.items.len() == PAGE_SIZE as usize, |total| {
                current < total
            })
    }
}

#[derive(Clone)]
pub struct Catalog {
    http: reqwest::Client,
    refreshed_previews: Arc<Mutex<LruCache<String, String>>>,
}

impl Catalog {
    pub fn new() -> Result<Self> {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| error.to_string())?;
        Ok(Self {
            http,
            refreshed_previews: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(128).unwrap(),
            ))),
        })
    }

    pub async fn search(&self, filters: &Filters, page: u32) -> Result<SearchPage> {
        let min = parse_bpm(&filters.min_bpm)?;
        let max = parse_bpm(&filters.max_bpm)?;
        if min.zip(max).is_some_and(|(min, max)| min > max) {
            return Err("Minimum BPM must be at or below maximum BPM.".into());
        }

        #[derive(Deserialize)]
        struct Data {
            #[serde(rename = "assetsSearch")]
            search: SearchPage,
        }

        let variables = json!({
            "query": optional(filters.term.trim()),
            "category": optional(filters.category),
            "tags": filters.tags.iter().map(|tag| &tag.uuid).collect::<Vec<_>>(),
            "key": optional(filters.key),
            "mode": optional(filters.mode),
            "min": min,
            "max": max,
            "sort": optional(filters.sort).unwrap_or("relevance"),
            "order": if filters.ascending { "ASC" } else { "DESC" },
            "seed": optional(&filters.seed),
            "pack": filters.pack.as_ref().map(|(id, _)| id),
            "page": page,
            "limit": PAGE_SIZE,
        });
        Ok(self.graphql::<Data>(SEARCH, variables).await?.search)
    }

    pub async fn suggestions(&self, term: &str) -> Result<Vec<String>> {
        #[derive(Deserialize)]
        struct Data {
            #[serde(rename = "soundsSearchSuggestions")]
            suggestions: Option<Suggestions>,
        }
        #[derive(Deserialize)]
        struct Suggestions {
            results: Option<Vec<Suggestion>>,
        }
        #[derive(Deserialize)]
        struct Suggestion {
            #[serde(rename = "autocompleteTerm")]
            term: String,
        }
        let response: Data = self.graphql(SUGGEST, json!({ "term": term })).await?;
        Ok(response
            .suggestions
            .and_then(|s| s.results)
            .unwrap_or_default()
            .into_iter()
            .map(|s| s.term)
            .collect())
    }

    async fn graphql<T: DeserializeOwned>(&self, query: &str, variables: Value) -> Result<T> {
        #[derive(Deserialize)]
        struct Response<T> {
            data: Option<T>,
            errors: Option<Vec<GraphQlError>>,
        }
        #[derive(Deserialize)]
        struct GraphQlError {
            message: String,
        }

        let response = self
            .http
            .post(GRAPHQL)
            .header("Accept", "application/json")
            .header("Origin", "https://splice.com")
            .header("Referer", "https://splice.com/")
            .json(&json!({ "query": query, "variables": variables }))
            .send()
            .await
            .map_err(network_error)?;
        if !response.status().is_success() {
            return Err(format!(
                "Splice catalog returned HTTP {}.",
                response.status().as_u16()
            ));
        }

        let bytes = read_limited(response, 6 * 1024 * 1024).await?;
        // Deserialize the envelope first so nullable data can report the GraphQL error.
        let response: Response<Value> = serde_json::from_slice(&bytes)
            .map_err(|_| "Splice returned an invalid catalog response.".to_string())?;
        if let Some(data) = response.data
            && let Ok(value) = serde_json::from_value(data)
        {
            return Ok(value);
        }
        Err(response
            .errors
            .and_then(|errors| errors.into_iter().next())
            .map(|error| format!("Splice: {}", error.message))
            .unwrap_or_else(|| "Splice returned no usable catalog data.".into()))
    }

    pub async fn preview_media(&self, sample: &Sample, limit: usize) -> Result<Vec<u8>> {
        let cached = self
            .refreshed_previews
            .lock()
            .unwrap()
            .get(&sample.uuid)
            .cloned();
        let url = cached
            .as_deref()
            .or_else(|| sample.preview())
            .ok_or("This sample has no audio preview.")?;
        let response = self.request_media(url).await?;
        if !preview_link_needs_refresh(response.status(), response.url()) {
            return read_media(response, limit).await;
        }
        drop(response);

        let fresh_url = self.lookup_preview_url(&sample.uuid).await?;
        let bytes = self.media(&fresh_url, limit).await?;
        self.refreshed_previews
            .lock()
            .unwrap()
            .put(sample.uuid.clone(), fresh_url);
        Ok(bytes)
    }

    async fn lookup_preview_url(&self, uuid: &str) -> Result<String> {
        #[derive(Deserialize)]
        struct Data {
            asset: Option<Sample>,
        }
        let response: Data = self.graphql(PREVIEW_LOOKUP, json!({ "id": uuid })).await?;
        response
            .asset
            .as_ref()
            .filter(|asset| asset.uuid == uuid)
            .and_then(Sample::preview)
            .map(str::to_owned)
            .ok_or_else(|| "Splice could not refresh the audio preview for this sample.".into())
    }

    pub async fn media(&self, url: &str, limit: usize) -> Result<Vec<u8>> {
        read_media(self.request_media(url).await?, limit).await
    }

    async fn request_media(&self, url: &str) -> Result<reqwest::Response> {
        let url = Url::parse(url).map_err(|_| "Invalid media URL.".to_string())?;
        let host = url.host_str().unwrap_or("");
        let allowed = host == "splice.com"
            || host.ends_with(".splice.com")
            || host == "splice-res.cloudinary.com"
            || (host.ends_with(".amazonaws.com")
                && (host.starts_with("spliceproduction.s3.")
                    || url.path().starts_with("/spliceblob.splice.com/")));
        if url.scheme() != "https" || url.port().is_some() || !allowed {
            return Err("Unsupported Splice media URL.".into());
        }
        self.http
            .get(url)
            .header("Referer", "https://splice.com/")
            .send()
            .await
            .map_err(network_error)
    }
}

fn preview_link_needs_refresh(status: StatusCode, url: &Url) -> bool {
    matches!(status, StatusCode::BAD_REQUEST | StatusCode::FORBIDDEN)
        && url
            .query_pairs()
            .any(|(name, _)| matches!(name.as_ref(), "X-Amz-Signature" | "Signature"))
}

async fn read_media(response: reqwest::Response, limit: usize) -> Result<Vec<u8>> {
    if !response.status().is_success() {
        return Err(format!(
            "Splice media returned HTTP {}.",
            response.status().as_u16()
        ));
    }
    read_limited(response, limit).await
}

fn optional(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
}

fn parse_bpm(value: &str) -> Result<Option<u16>> {
    optional(value.trim())
        .map(|value| {
            value
                .parse::<u16>()
                .ok()
                .filter(|&n| n > 0)
                .ok_or_else(|| "Enter a positive whole number for BPM.".into())
        })
        .transpose()
}

fn network_error(error: reqwest::Error) -> String {
    if error.is_timeout() {
        "The request to Splice timed out.".into()
    } else {
        "Could not connect to Splice. Check your connection and retry.".into()
    }
}

async fn read_limited(mut response: reqwest::Response, limit: usize) -> Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err("Splice response exceeded the size limit.".into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(network_error)? {
        if chunk.len() > limit - bytes.len() {
            return Err("Splice response exceeded the size limit.".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_rejected_signed_preview_links_trigger_a_refresh() {
        let signed = Url::parse(
            "https://spliceproduction.s3.us-west-1.amazonaws.com/preview?X-Amz-Signature=example",
        )
        .unwrap();
        let unsigned = Url::parse("https://spliceblob.splice.com/waveform").unwrap();
        assert!(preview_link_needs_refresh(StatusCode::BAD_REQUEST, &signed));
        assert!(preview_link_needs_refresh(StatusCode::FORBIDDEN, &signed));
        assert!(!preview_link_needs_refresh(StatusCode::NOT_FOUND, &signed));
        assert!(!preview_link_needs_refresh(
            StatusCode::BAD_REQUEST,
            &unsigned
        ));
    }
}
