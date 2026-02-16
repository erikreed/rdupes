use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use tinyvec::TinyVec;

pub type Hash = [u8; 32];
pub type TVString = TinyVec<[String; 4]>;
pub type FileKey = (u64, u64);

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct PathGroup {
    pub paths: TVString,
    pub inode: u64,
}

pub type SizeMap = HashMap<u64, TinyVec<[PathGroup; 2]>>;

pub struct DupeParams {
    pub min_file_size: u64,
    pub read_concurrency: usize,
    pub disable_mmap: bool,
}

pub struct DupeSet {
    pub paths: Vec<String>,
    pub fsize: u64,
}

impl DupeSet {
    pub fn sort_paths(&mut self, input_paths: &[String]) {
        fn lmatch_count(left: &str, right: &str) -> usize {
            left.bytes()
                .zip(right.bytes())
                .enumerate()
                .find(|(_i, (a, b))| a != b)
                .map(|(i, _)| i)
                .unwrap_or(left.len().min(right.len()))
        }

        self.paths.sort_by_cached_key(|p| {
            input_paths
                .iter()
                .enumerate()
                .max_by_key(|(_i, s)| lmatch_count(s, p))
                .map(|(i, _s)| (i, p.len()))
                .unwrap_or((usize::MAX, usize::MAX))
        });
    }
}

#[derive(Copy, Clone)]
pub enum PathCheckMode {
    Start,
    End,
    Full,
}
