/// One index file plus the CDN base URLs its `dest` paths resolve against.
/// The legacy schema has exactly one; the resource-pack schema has one per
/// pack in the chosen bundle (e.g. `common` + `hd`).
#[derive(Clone, Debug)]
pub struct PackSource {
    pub name: String,
    pub index_url: String,
    pub zip_bases: Vec<String>,
}

#[derive(Clone)]
pub struct Config {
    pub packs: Vec<PackSource>,
}

#[derive(Clone)]
pub struct DownloadOptions {
    pub verify_concurrency: usize,
    pub download_concurrency: usize,
}

impl Default for DownloadOptions {
    fn default() -> Self {
        Self {
            verify_concurrency: 8,
            download_concurrency: 4,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ResourceItem {
    pub dest: String,
    pub md5: Option<String>,
    pub size: Option<u64>,
    /// Index into `Config::packs` this file was listed in.
    pub pack: usize,
}
