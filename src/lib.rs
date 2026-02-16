pub mod types;
pub mod hashing;
pub mod traversal;
pub mod finder;

pub use types::*;
pub use hashing::*;
pub use traversal::*;
pub use finder::*;

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn hard_link_duplicates() {
        let temp = tempfile::tempdir().unwrap();
        let test_dir = temp.path();

        let f1 = test_dir.join("file1").to_string_lossy().to_string();
        let f1_hl = test_dir.join("file1_hl").to_string_lossy().to_string();
        let f2 = test_dir.join("file2").to_string_lossy().to_string();
        let f2_hl = test_dir.join("file2_hl").to_string_lossy().to_string();
        let f3 = test_dir.join("file3").to_string_lossy().to_string();

        std::fs::write(&f1, b"common content").unwrap();
        std::fs::hard_link(&f1, &f1_hl).unwrap();
        std::fs::write(&f2, b"common content").unwrap();
        std::fs::hard_link(&f2, &f2_hl).unwrap();
        std::fs::write(&f3, b"uncommon content").unwrap();

        let df = DupeFinder::new(DupeParams {
            min_file_size: 0,
            read_concurrency: 1,
            disable_mmap: true,
        });

        let size_map = df
            .traverse_paths(vec![test_dir.to_string_lossy().to_string()])
            .await
            .unwrap();
        let (tx, mut rx) = mpsc::channel(10);

        df.check_hashes_and_content(size_map, tx).await.unwrap();

        let mut all_paths = Vec::new();
        while let Some(ds) = rx.recv().await {
            for p in ds.paths {
                all_paths.push(p);
            }
        }
        all_paths.sort();

        let mut expected = vec![f1, f1_hl, f2, f2_hl];
        expected.sort();

        assert_eq!(all_paths, expected);
    }
}
