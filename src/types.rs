use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tinyvec::TinyVec;

pub const EDGE_SIZE: usize = 4096;

pub type Hash = [u8; 32];
pub type TVString = TinyVec<[String; 4]>;
pub type FileKey = (u64, u64);

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct PathGroup {
    pub paths: TVString,
    pub inode: u64,
}

pub type SizeMap = HashMap<u64, TinyVec<[PathGroup; 2]>>;

#[derive(Debug)]
pub struct DupeParams {
    pub min_file_size: u64,
    pub read_concurrency: usize,
    pub disable_mmap: bool,
}

#[derive(Debug)]
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

#[derive(Copy, Clone, Debug)]
pub enum PathCheckMode {
    Start,
    End,
    Full,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sorting() {
        let input_paths = vec!["/asd/".into(), "/def/".into(), "/tmp/asd/".into()];
        let mut ds = DupeSet {
            paths: vec![
                "/asd/file1000.txt".to_string(),
                "/def/z".to_string(),
                "/tmp/asd/f123.mp4".to_string(),
                "/def/yyy".to_string(),
                "/def/xx".to_string(),
                "/asd/file1.txt".to_string(),
                "/def/1234".to_string(),
                "/asd/xile100.txt".to_string(),
            ],
            fsize: 0,
        };
        ds.sort_paths(&input_paths);
        assert_eq!(
            vec![
                "/asd/file1.txt".to_string(),
                "/asd/xile100.txt".to_string(),
                "/asd/file1000.txt".to_string(),
                "/def/z".to_string(),
                "/def/xx".to_string(),
                "/def/yyy".to_string(),
                "/def/1234".to_string(),
                "/tmp/asd/f123.mp4".to_string(),
            ],
            ds.paths
        )
    }
}
