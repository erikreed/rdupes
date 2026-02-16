use std::io::{BufReader, BufWriter};

use clap::Parser;
use log::{error, info, LevelFilter};
use mimalloc::MiMalloc;
use rdupes::{DupeFinder, DupeParams};
use std::io::Write;
use tokio::time::Instant;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// A duplicate file finder.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Paths to traverse
    paths: Vec<String>,

    /// Save null delimited source-dupe pairs to the output path. Use '-' for stdout.
    #[arg(short, long)]
    output_path: Option<clio::OutputPath>,

    /// Save a traversal checkpoint at the path if provided. Use '-' for stdout.
    #[arg(short = 's', long)]
    checkpoint_save_path: Option<clio::OutputPath>,

    /// Load a traversal checkpoint at the path if provided.
    #[arg(short = 'l', long)]
    checkpoint_load_path: Option<clio::Input>,

    /// Filter for files of at least the minimum size
    #[arg(short, long, default_value_t = 4096)]
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
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    info!("Parameters: {:?}", args);

    if args.read_concurrency == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "read_concurrency must be greater than 0",
        ));
    }

    let params = DupeParams {
        min_file_size: args.min_file_size,
        read_concurrency: args.read_concurrency,
        disable_mmap: args.disable_mmap,
    };
    let df = DupeFinder::new(params);
    let now = Instant::now();

    let size_map = if let Some(f) = args.checkpoint_load_path {
        info!("Reading checkpoint from: {:?}", f.path());
        let reader = BufReader::new(f);
        ciborium::from_reader(reader).map_err(|e| {
            error!("Failed to parse checkpoint data: {:?}", e);
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?
    } else {
        info!("Traversing paths: {:?}", args.paths);
        df.traverse_paths(args.paths.clone()).await?
    };

    if let Some(path) = args.checkpoint_save_path {
        info!("Saving checkpoint to: {path:?}");
        let file = path.create()?;
        let writer = BufWriter::new(file);
        ciborium::into_writer(&size_map, writer).map_err(|e| {
            error!("Failed to save checkpoint data: {:?}", e);
            std::io::Error::new(std::io::ErrorKind::Other, e)
        })?;
    }

    let mut dupes_rx = df.find_duplicates(size_map);
    let mut out = args
        .output_path
        .map(|f| f.create())
        .transpose()
        .map_err(|e| {
            error!("Unable to open output file: {:?}", e);
            e
        })?;

    while let Some(mut ds) = dupes_rx.recv().await {
        if let Some(ref mut f) = out {
            ds.sort_paths(&args.paths);
            let source = &ds.paths[0];
            for dest in &ds.paths[1..] {
                f.write_all(format!("{source}\0{dest}\0").as_ref())?;
            }
        }
    }

    info!("Elapsed time: {:.1}s", now.elapsed().as_secs_f32());

    Ok(())
}
