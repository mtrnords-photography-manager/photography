#![warn(clippy::missing_const_for_fn)]
#![warn(clippy::suboptimal_flops)]

use std::path::PathBuf;

use async_nats::jetstream::{self, stream};
use async_nats::jetstream::stream::Compression;
use clap::{Parser, Subcommand};
use notify_debouncer_full::notify::{RecursiveMode, Watcher};
use tracing::info;

mod c2pa;
mod importer;
mod jetstream_events;

#[derive(clap::Parser)]
struct Opts {
    #[arg(short, long, default_value = "nats://localhost:4222")]
    nats_url: String,
    #[arg(short, long, default_value = "import")]
    import_folder: PathBuf,
    #[arg(short, long, default_value = "export")]
    export_folder: PathBuf,
    #[arg(short, long, default_value = "c2pa")]
    c2pa_folder: PathBuf,
    #[arg(long, default_value = "true")]
    walk_export_folders: std::primitive::bool,
    #[arg(long, default_value = "true")]
    walk_import_folders: std::primitive::bool,

    // Subcommands
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    C2PACert {},
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    // Set default login to default settings (info, warn, error being shown)
    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter("info")
        .init();

    // Parse command line arguments
    let opts: Opts = Opts::parse();

    // Create import folder if it doesn't exist
    let import_folder = opts.import_folder;
    let export_folder = opts.export_folder;
    let c2pa_folder = opts.c2pa_folder;

    std::fs::create_dir_all(import_folder.clone())?;
    std::fs::create_dir_all(export_folder.clone())?;
    std::fs::create_dir_all(c2pa_folder.clone())?;

    if let Some(Commands::C2PACert {}) = opts.command {
        c2pa::generate_certificates(c2pa_folder.clone())?;
        return Ok(());
    }

    rexiv2::initialize().expect("Unable to initialize rexiv2");

    info!("Starting the application");

    // Setup nats connection
    let nats_url = std::env::var("NATS_URL").unwrap_or(opts.nats_url);

    let client = async_nats::connect(nats_url).await?;

    let jetstream = jetstream::new(client);

    let import_stream = jetstream
        .create_stream(stream::Config {
            name: "IMPORT".to_string(),
            retention: stream::RetentionPolicy::WorkQueue,
            subjects: vec!["import.>".to_string()],
            compression: Some(Compression::S2),
            ..Default::default()
        })
        .await?;
    info!("Nats import stream created");

    info!("Creating C2PA signer");
    let c2pa_signer =
        c2pa::C2PASigner::new(export_folder.clone(), c2pa_folder, import_stream.clone()).await?;
    tokio::spawn(async move {
        c2pa_signer.consumer().await.unwrap();
    });

    info!("Creating importer in other thread");

    let importer = importer::Importer::new(
        import_folder.clone(),
        export_folder,
        import_stream,
        jetstream,
    )
    .await?;
    let importer_clone = importer.clone();
    tokio::spawn(async move {
        importer_clone.nats_consumer().await.unwrap();
    });

    // Create a file listener using notify to listen for new files to import
    let mut debouncer = importer
        .listen_for_files(opts.walk_import_folders, opts.walk_export_folders)
        .await
        .unwrap();
    debouncer
        .watcher()
        .watch(&import_folder, RecursiveMode::NonRecursive)?;
    debouncer
        .cache()
        .add_root(&import_folder, RecursiveMode::NonRecursive);
    info!("Watching for new files in {:?}", import_folder);

    // Wait for ctrl-c to exit
    tokio::signal::ctrl_c().await?;

    Ok(())
}
