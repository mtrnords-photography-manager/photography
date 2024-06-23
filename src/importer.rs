use std::path::PathBuf;
use std::time::Duration;

use async_nats::jetstream;
use async_nats::jetstream::consumer::pull::Config;
use async_nats::jetstream::consumer::Consumer;
use async_nats::jetstream::{AckKind, Context};
use futures::StreamExt;
use notify_debouncer_full::notify::RecommendedWatcher;
use notify_debouncer_full::{new_debouncer, DebounceEventResult, Debouncer, FileIdMap};
use rexiv2::Metadata;
use time::macros::format_description;
use time::PrimitiveDateTime;
use tokio::runtime::Handle;
use tracing::{debug, error, info, trace, warn};

use crate::jetstream_events::Exif;

const ALLOWED_TYPES: [&str; 8] = ["cr2", "cr3", "jpg", "tiff", "dng", "png", "jpeg", "mp4"];

#[derive(Debug, Clone)]
pub(crate) struct Importer {
    // Folder to watch for new files
    import_folder: PathBuf,
    // Export folder to move files to
    export_folder: PathBuf,
    // Nats consumer to work with
    consumer: Consumer<Config>,
    // Jetstream client
    jetstream: Context,
}

impl Importer {
    pub(crate) async fn new(
        import_folder: PathBuf,
        export_folder: PathBuf,
        import_stream: jetstream::stream::Stream,
        jetstream: Context,
    ) -> color_eyre::Result<Self> {
        let consumer = import_stream
            .create_consumer(Config {
                durable_name: Some("processor-1".to_string()),
                filter_subject: "import.file_added".to_string(),
                ..Default::default()
            })
            .await?;

        Ok(Self {
            import_folder,
            export_folder,
            consumer,
            jetstream,
        })
    }

    fn extract_exif<'a>(&self, exif: Metadata) -> Exif<'a> {
        // Get all the exifdata
        let gps_info = exif.get_gps_info();
        let gps_latitude_ref = exif.get_tag_string("Exif.GPSInfo.GPSLatitudeRef");
        let gps_longitude_ref = exif.get_tag_string("Exif.GPSInfo.GPSLongitudeRef");
        let gps_timestamp = exif.get_tag_rational("Exif.GPSInfo.GPSTimeStamp");
        let gps_speed_ref = exif.get_tag_string("Exif.GPSInfo.GPSSpeedRef");
        let gps_speed = exif.get_tag_rational("Exif.GPSInfo.GPSSpeed");
        let gps_direction_ref = exif.get_tag_string("Exif.GPSInfo.GPSImgDirectionRef");
        let gps_direction = exif.get_tag_rational("Exif.GPSInfo.GPSImgDirection");
        let gps_positioning_error = exif.get_tag_rational("Exif.GPSInfo.GPSHPositioningError");
        let exposure_time = exif.get_tag_rational("Exif.Photo.ExposureTime");
        let f_number = exif.get_tag_rational("Exif.Photo.FNumber");
        let digital_zoom_ratio = exif.get_tag_rational("Exif.Photo.DigitalZoomRatio");
        let lens_make = exif.get_tag_string("Exif.Photo.LensMake");
        let lens_model = exif.get_tag_string("Exif.Photo.LensModel");

        Exif {
            gps_version_id: None,
            latitude: gps_info.map(|info| {
                format!(
                    "{}{}",
                    info.latitude.to_string().replace('.', ","),
                    gps_latitude_ref.unwrap_or_default()
                )
                .into()
            }),
            longitude: gps_info.map(|info| {
                format!(
                    "{}{}",
                    info.longitude.to_string().replace('.', ","),
                    gps_longitude_ref.unwrap_or_default()
                )
                .into()
            }),
            altitude_ref: None,
            altitude: gps_info.map(|info| info.altitude.to_string().into()),
            timestamp: gps_timestamp.map(|timestamp| timestamp.to_string().into()),
            speed_ref: gps_speed_ref.map(|speed_ref| speed_ref.into()).ok(),
            speed: gps_speed.map(|speed| speed.to_string().into()),
            direction_ref: gps_direction_ref
                .map(|direction_ref| direction_ref.into())
                .ok(),
            direction: gps_direction.map(|direction| direction.to_string().into()),
            destination_bearing_ref: None,
            destination_bearing: None,
            positioning_error: gps_positioning_error.map(|error| error.to_string().into()),
            exposure_time: exposure_time.map(|time| time.to_string().into()),
            // Cursed
            f_number: f_number
                .map(|f_number| f_number.into_raw().0 as f64 / f_number.into_raw().1 as f64),
            color_space: None,
            digital_zoom_ratio: digital_zoom_ratio.map(|digital_zoom_ratio| {
                digital_zoom_ratio.into_raw().0 as f64 / digital_zoom_ratio.into_raw().1 as f64
            }),
            make: None,
            model: None,
            lens_make: lens_make.map(|lens_make| lens_make.into()).ok(),
            lens_model: lens_model.map(|lens_model| lens_model.into()).ok(),
            lens_specification: None,
        }
    }

    pub(crate) async fn nats_consumer(&self) -> color_eyre::Result<()> {
        let mut messages = self.consumer.messages().await?;
        while let Some(msg) = messages.next().await {
            match msg {
                Ok(msg) => {
                    let event: super::jetstream_events::FileToImport =
                        bincode::deserialize(&msg.payload).unwrap();
                    debug!("Received message: {:#?}", event);

                    // Initial ack to notify that we received the message
                    msg.ack_with(AckKind::Progress).await.unwrap();

                    // Check if the file still is in the import

                    // First get absolute import folder path
                    let import_folder = self.import_folder.canonicalize()?;

                    // Check if the file is still there
                    if !event.path.exists() {
                        error!("[IMPORTER] file does not exist: {:?}", event.path);
                        // We also acknowledge the message to remove it from the queue
                        msg.ack_with(AckKind::Term).await.unwrap();
                        continue;
                    }

                    // Then check if the file is in the import folder
                    if !event.path.starts_with(import_folder.clone()) {
                        error!(
                            "[IMPORTER] file is not in the import folder: {:?}",
                            event.path
                        );
                        // Move the folder into the "failed" subfolder of the import folder
                        let failed_folder = import_folder.join("failed");
                        std::fs::create_dir_all(&failed_folder)?;
                        let new_path = failed_folder.join(event.path.file_name().unwrap());
                        std::fs::rename(&event.path, &new_path)?;

                        // Also move any xmp files with the same name
                        let xmp_path = event.path.with_extension("xmp");
                        if xmp_path.exists() {
                            let new_xmp_path = failed_folder.join(xmp_path.file_name().unwrap());
                            std::fs::rename(&xmp_path, &new_xmp_path)?;
                        }

                        // We also acknowledge the message to remove it from the queue
                        msg.ack_with(AckKind::Term).await.unwrap();
                        continue;
                    }

                    // Another progress ack to notify that we are processing the file
                    msg.ack_with(AckKind::Progress).await.unwrap();

                    let mut result_path = event.path.clone();

                    let mut exif_data = None;
                    // Get exif data
                    {
                        let meta = rexiv2::Metadata::new_from_path(&event.path);

                        match meta {
                            Ok(exif) => {
                                trace!("Got exif data");
                                // Get the date the photo was taken
                                if let Ok(date) = exif.get_tag_string("Exif.Photo.DateTimeOriginal")
                                {
                                    let format = format_description!(
                                        "[year]:[month]:[day] [hour]:[minute]:[second]"
                                    );
                                    match PrimitiveDateTime::parse(&date, &format) {
                                        Ok(datetime) => {
                                            debug!("[IMPORTER] Date taken: {:?}", datetime);
                                            // Ensure the export subfolders exist
                                            // The image should be saved in `export/year/month/day` format
                                            let year = datetime.year().to_string();
                                            let month = datetime.month().to_string();
                                            let day = datetime.day().to_string();
                                            let export_folder = self
                                                .export_folder
                                                .join(&year)
                                                .join(&month)
                                                .join(&day);
                                            std::fs::create_dir_all(&export_folder)?;

                                            // Move the file to the export folder
                                            let new_path =
                                                export_folder.join(event.path.file_name().unwrap());
                                            std::fs::rename(&event.path, &new_path)?;

                                            // Also move any xmp files with the same name
                                            let xmp_path = event.path.with_extension("xmp");
                                            if xmp_path.exists() {
                                                let new_xmp_path = export_folder
                                                    .join(xmp_path.file_name().unwrap());
                                                std::fs::rename(&xmp_path, &new_xmp_path)?;
                                            }

                                            result_path = new_path;
                                        }
                                        Err(e) => {
                                            error!("[IMPORTER] error parsing date: {:?}", e);
                                        }
                                    }
                                } else {
                                    let other_folder = self.export_folder.join("other");
                                    error!(
                                        "[IMPORTER] Date taken not found placing in \"{}\"",
                                        other_folder.display()
                                    );
                                    // Ensure the export "other" subfolder exists
                                    std::fs::create_dir_all(&other_folder)?;

                                    // Move the file to the export folder
                                    let new_path =
                                        other_folder.join(event.path.file_name().unwrap());
                                    std::fs::rename(&event.path, &new_path)?;

                                    // Also move any xmp files with the same name
                                    let xmp_path = event.path.with_extension("xmp");
                                    if xmp_path.exists() {
                                        let new_xmp_path =
                                            other_folder.join(xmp_path.file_name().unwrap());
                                        std::fs::rename(&xmp_path, &new_xmp_path)?;
                                    }

                                    result_path = new_path;
                                }

                                let exif = self.extract_exif(exif);

                                exif_data = Some(exif);

                                info!("Finished processing exif data")
                            }
                            Err(e) => {
                                error!("[IMPORTER] error reading exif data: {:?}", e);
                                let other_folder = self.export_folder.join("other");
                                error!(
                                    "[IMPORTER] Exif data not found placing in \"{}\"",
                                    other_folder.display()
                                );
                                // Ensure the export "other" subfolder exists
                                std::fs::create_dir_all(&other_folder)?;

                                // Move the file to the export folder
                                let new_path = other_folder.join(event.path.file_name().unwrap());
                                std::fs::rename(&event.path, &new_path)?;

                                // Also move any xmp files with the same name
                                let xmp_path = event.path.with_extension("xmp");
                                if xmp_path.exists() {
                                    let new_xmp_path =
                                        other_folder.join(xmp_path.file_name().unwrap());
                                    std::fs::rename(&xmp_path, &new_xmp_path)?;
                                }

                                result_path = new_path;
                            }
                        }
                    };

                    info!(
                        "Finished processing file: {:?}\nQueuing for next steps",
                        result_path
                    );

                    // Add another progress ack to notify that we are done processing the exif
                    msg.ack_with(AckKind::Progress).await.unwrap();

                    // Add a c2pa event to the stream
                    let c2pa_event = super::jetstream_events::C2PASignRequest {
                        path: result_path.canonicalize().unwrap(),
                        exif: exif_data,
                    };
                    let bytes = bincode::serialize(&c2pa_event).unwrap();
                    publish_with_error_printing(
                        &msg.context,
                        "import.sign_request".to_string(),
                        bytes,
                    )
                    .await;

                    // Add another progress ack to notify that we are done processing the exif
                    msg.ack_with(AckKind::Progress).await.unwrap();

                    // TODO: Notify other steps here?

                    // Acknowledge the message to remove it from the queue
                    msg.ack_with(AckKind::Ack).await.unwrap();
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                Err(e) => {
                    error!("[IMPORTER] error receiving message: {:?}", e);
                }
            }
        }
        error!("[IMPORTER] Consumer stopped");
        Ok(())
    }

    async fn file_added(path: PathBuf, jetstream: Context) -> color_eyre::Result<()> {
        // Check if the path is a file
        if !path.is_file() {
            return Ok(());
        }
        // check if cr2, cr3, jpg, tiff, dng or png file
        let extension = path
            .extension()
            .unwrap_or_default()
            .to_string_lossy()
            .to_lowercase();
        if !ALLOWED_TYPES.contains(&extension.as_str()) {
            return Ok(());
        }
        info!("[IMPORTER] New file to import: {:?}", path);

        // Notify the importer to import the file
        let import_event = super::jetstream_events::FileToImport {
            path: path.canonicalize().unwrap(),
        };
        let bytes = bincode::serialize(&import_event).unwrap();

        publish_with_error_printing(&jetstream, "import.file_added".to_string(), bytes).await;

        Ok(())
    }

    fn file_event_handler(res: DebounceEventResult, jetstream: Context, tokio_runtime: Handle) {
        match res {
            Ok(events) => {
                for event in events {
                    // Check if the file was copied to and not removed
                    if event.kind.is_remove() {
                        continue;
                    }
                    for path in event.paths.clone() {
                        let jetstream = jetstream.clone();
                        tokio_runtime.spawn(async move {
                            Importer::file_added(path, jetstream).await.unwrap();
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        });
                    }
                }
            }
            Err(e) => {
                error!("[IMPORTER] watch error: {:?}", e);
            }
        }
    }

    pub(crate) async fn listen_for_files(
        &self,
        walk_import_folders: bool,
        walk_export_folders: bool,
    ) -> color_eyre::Result<Debouncer<RecommendedWatcher, FileIdMap>> {
        // Create a file listener using notify to listen for new files to import
        let jetstream = self.jetstream.clone();
        let tokio_runtime = tokio::runtime::Handle::current();
        let debouncer = new_debouncer(
            Duration::from_secs(2),
            None,
            move |res: DebounceEventResult| {
                Importer::file_event_handler(res, jetstream.clone(), tokio_runtime.clone());
            },
        )?;

        if walk_import_folders {
            warn!("Walking input dir to see if we missed any. Errors on missing files on the consumer are expected");
            walkdir::WalkDir::new(&self.import_folder)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_type().is_file())
                .filter(|e| {
                    let extension = e
                        .path()
                        .extension()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .to_lowercase();
                    ALLOWED_TYPES.contains(&extension.as_str())
                })
                .for_each(|entry| {
                    let path = entry.path().to_path_buf();
                    let jetstream = self.jetstream.clone();
                    tokio::spawn(async move {
                        Importer::file_added(path, jetstream).await.unwrap();
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    });
                });
            warn!("Done walking input dir");
        }
        if walk_export_folders {
            warn!("Walking output dir to ensure signing worked");
            walkdir::WalkDir::new(&self.export_folder)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_type().is_file())
                .filter(|e| {
                    let extension = e
                        .path()
                        .extension()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .to_lowercase();
                    ALLOWED_TYPES.contains(&extension.as_str())
                })
                // Filter any which already have a c2pa file with the same name next to it
                .filter(|e| {
                    let path = e.path();
                    let sidecar_path = path.with_extension("c2pa");
                    !sidecar_path.exists()
                })
                .for_each(|entry| {
                    let path = entry.path().to_path_buf();
                    let jetstream = self.jetstream.clone();
                    let mut exif_data = None;
                    // Get exif data
                    {
                        let meta = rexiv2::Metadata::new_from_path(&path);
                        match meta {
                            Ok(exif) => {
                                trace!("Got exif data");

                                let exif = self.extract_exif(exif);
                                exif_data = Some(exif);

                                debug!("Finished processing exif data")
                            }
                            Err(e) => {
                                error!("[IMPORTER] error reading exif data: {:?}", e);
                            }
                        }
                    };
                    tokio::spawn(async move {
                        // Add a c2pa event to the stream
                        let c2pa_event = super::jetstream_events::C2PASignRequest {
                            path: path.canonicalize().unwrap(),
                            exif: exif_data,
                        };
                        let bytes = bincode::serialize(&c2pa_event).unwrap();
                        publish_with_error_printing(
                            &jetstream,
                            "import.sign_request".to_string(),
                            bytes,
                        )
                        .await;
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    });
                });
        }
        Ok(debouncer)
    }
}

async fn publish_with_error_printing(ctx: &Context, subject: String, data: Vec<u8>) {
    match ctx.publish(subject.clone(), data.into()).await {
        Ok(_) => {}
        Err(e) => {
            error!(
                "[IMPORTER] error publishing message to `{}`: {:?}",
                subject, e
            );
            // Crash the program if we can't publish the message
            std::process::exit(1);
        }
    }
}
