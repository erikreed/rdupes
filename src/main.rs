use clap::Parser;
use log::{info, LevelFilter};
use mimalloc::MiMalloc;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::time::Instant;

use rdupes::{traverse_paths, DupeSet};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;
/// A duplicate file finder.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Paths to traverse
    paths: Vec<String>,

    /// Verbose mode
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

    let now = Instant::now();
    info!("Traversing paths: {:?}", args.paths);
    let (dupes_tx, mut dupes_rx) = mpsc::channel::<DupeSet>(32);

    let task = tokio::task::spawn(traverse_paths(args.paths, dupes_tx));
    let mut stdout = tokio::io::stdout();
    while let Some(mut ds) = dupes_rx.recv().await {
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
