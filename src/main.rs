use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;

use clap::Parser;
use log::{error, info, LevelFilter};
use mimalloc::MiMalloc;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::time::Instant;

use rdupes::{DupeFinder, DupeParams, DupeSet};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// A duplicate file finder.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Paths to traverse
    paths: Vec<String>,

    /// Save a traversal checkpoint at the path if provided.
    #[arg(short = 's', long)]
    checkpoint_save_path: Option<PathBuf>,

    /// Load a traversal checkpoint at the path if provided. The input paths are ignored if so.
    #[arg(short = 'l', long)]
    checkpoint_load_path: Option<PathBuf>,

    /// Filter for files of at least the minimum size
    #[arg(short, long, default_value_t = 8192)]
    min_file_size: u64,

    /// I/O concurrency to use for reads. For SSDs, a higher value like 128 is reasonable, while
    /// HDDs should be very low, probably 1 if files are very large on average (multi-GB).
    #[arg(short, long, default_value_t = 4)]
    read_concurrency: usize,

    /// Disable memory-mapped I/O for full file hashing. This helps memory benchmarking but may
    /// reduce hash speed for files already in the OS cache.
    #[arg(short, long, default_value_t = false)]
    disable_mmap: bool,

    /// Enable verbose/debug logging
    #[arg(short, long, default_value_t = false)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let log_level = if args.verbose {
        LevelFilter::Debug
    } else {
        LevelFilter::Info
    };
    simple_logger::SimpleLogger::new()
        .with_level(log_level)
        .init()
        .unwrap();

    info!("Parameters: {:?}", args);

    assert!(args.read_concurrency > 0);
    let params = DupeParams {
        min_file_size: args.min_file_size,
        read_concurrency: args.read_concurrency,
        disable_mmap: args.disable_mmap,
    };
    let (dupes_tx, mut dupes_rx) = mpsc::channel::<DupeSet>(32);
    let df = Box::leak(Box::new(DupeFinder::new(params)));

    let now = Instant::now();

    let size_map = {
        if let Some(path) = args.checkpoint_load_path {
            info!("Reading checkpoint from: {path:?}");
            let file = File::open(path)?;
            let reader = BufReader::new(file);
            ciborium::from_reader(reader).expect("Failed to parse checkpoint data")
        } else {
            info!("Traversing paths: {:?}", args.paths);
            df.traverse_paths(args.paths.clone()).await?
        }
    };

    if let Some(path) = args.checkpoint_save_path {
        info!("Saving checkpoint to: {path:?}");
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        let _ = ciborium::into_writer(&size_map, writer)
            .map_err(|e| error!("Failed to save checkpoint data: {e:?}"));
    }

    let task = tokio::task::spawn(df.check_hashes_and_content(size_map, dupes_tx));
    let mut stdout = tokio::io::stdout();
    while let Some(mut ds) = dupes_rx.recv().await {
        ds.sort_paths(&args.paths);
        let source = &ds.paths[0];
        for dest in &ds.paths[1..] {
            stdout
                .write_all(format!("{source}\0{dest}\0").as_ref())
                .await?;
        }
    }

    task.await??;
    info!("Elapsed time: {:.1}s", now.elapsed().as_secs_f32());

    Ok(())
}
