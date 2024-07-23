use clap::Parser;
use log::{info, LevelFilter};
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

    /// Filter for files of at least the minimum size
    #[arg(short, long, default_value_t = 8192)]
    min_file_size: u64,

    /// I/O concurrency to use for reads. For SSDs, a higher value like 128 is reasonable, while
    /// HDDs should be very low, probably 1 if files are very large on average (multi-GB).
    #[arg(short, long, default_value_t = 4)]
    read_concurrency: usize,

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
    };
    let (dupes_tx, mut dupes_rx) = mpsc::channel::<DupeSet>(32);
    let df = Box::new(DupeFinder::new(params));

    let now = Instant::now();
    info!("Traversing paths: {:?}", args.paths);

    let task = tokio::task::spawn(Box::leak(df).traverse_paths(args.paths, dupes_tx));
    let mut stdout = tokio::io::stdout();
    while let Some(mut ds) = dupes_rx.recv().await {
        // TODO: custom sorter
        ds.paths.sort();
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
