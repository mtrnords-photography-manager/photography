use std::path::PathBuf;
use std::sync::Arc;

use async_nats::jetstream;
use async_nats::jetstream::AckKind;
use async_nats::jetstream::consumer::Consumer;
use async_nats::jetstream::consumer::pull::Config;
use c2pa::Reader;
use futures::StreamExt;
use simple_c2pa::{
    ApplicationInfo, Certificate, ContentCredentials, create_content_credentials_certificate,
    create_root_certificate, ExifData, FileData,
};
use tracing::{debug, error, info};

const APP_NAME: &str = "MTRNord_Photography_Manager";
const USERNAME: &str = "MTRNord";

pub fn generate_certificates(
    c2pa_folder: PathBuf,
) -> color_eyre::Result<(Arc<Certificate>, Arc<Certificate>)> {
    let c2pa_folder = c2pa_folder.join("certificates");
    // Create the folder if it doesn't exist
    std::fs::create_dir_all(&c2pa_folder)?;

    // Generate the certificates
    let root_certificate = create_root_certificate(Some(USERNAME), None)?;
    // Write the root certificate to a file
    let root_certificate_data = root_certificate.get_certificate_bytes()?;
    let root_private_key_data = root_certificate.get_private_key_bytes()?;
    let root_certificate_path = c2pa_folder.join("root_certificate.crt");
    let root_private_key_path = c2pa_folder.join("root_private_key.pem");
    std::fs::write(&root_certificate_path, root_certificate_data)?;
    std::fs::write(&root_private_key_path, root_private_key_data)?;

    // Create the content credentials certificate

    let content_credentials_certificate = create_content_credentials_certificate(
        Some(root_certificate.clone()),
        Some(USERNAME),
        None,
    )?;

    // Write the content credentials certificate to a file
    let content_credentials_certificate_data =
        content_credentials_certificate.get_certificate_bytes()?;
    let content_credentials_private_key_data =
        content_credentials_certificate.get_private_key_bytes()?;
    let content_credentials_certificate_path =
        c2pa_folder.join("content_credentials_certificate.crt");
    let content_credentials_private_key_path =
        c2pa_folder.join("content_credentials_private_key.pem");
    std::fs::write(
        &content_credentials_certificate_path,
        content_credentials_certificate_data,
    )?;
    std::fs::write(
        &content_credentials_private_key_path,
        content_credentials_private_key_data,
    )?;

    Ok((root_certificate, content_credentials_certificate))
}

fn load_certificates(
    c2pa_folder: PathBuf,
) -> color_eyre::Result<(Arc<Certificate>, Arc<Certificate>)> {
    let c2pa_folder = c2pa_folder.join("certificates");
    let root_certificate_path = c2pa_folder.join("root_certificate.crt");
    let root_private_key_path = c2pa_folder.join("root_private_key.pem");
    let content_credentials_certificate_path =
        c2pa_folder.join("content_credentials_certificate.crt");
    let content_credentials_private_key_path =
        c2pa_folder.join("content_credentials_private_key.pem");

    let root_certificate_data = std::fs::read(&root_certificate_path)?;
    let root_private_key_data = std::fs::read(&root_private_key_path)?;
    let content_credentials_certificate_data =
        std::fs::read(&content_credentials_certificate_path)?;
    let content_credentials_private_key_data =
        std::fs::read(&content_credentials_private_key_path)?;

    let root_certificate_filedata = FileData::new(None, Some(root_certificate_data), None);
    let root_private_key_filedata = FileData::new(None, Some(root_private_key_data), None);
    let content_credentials_certificate_filedata =
        FileData::new(None, Some(content_credentials_certificate_data), None);
    let content_credentials_private_key_filedata =
        FileData::new(None, Some(content_credentials_private_key_data), None);

    let root_certificate =
        Certificate::new(root_certificate_filedata, root_private_key_filedata, None);
    let content_credentials_certificate = Certificate::new(
        content_credentials_certificate_filedata,
        content_credentials_private_key_filedata,
        Some(root_certificate.clone()),
    );

    Ok((root_certificate, content_credentials_certificate))
}

pub fn sign_image(
    export_path: PathBuf,
    image_data: Arc<FileData>,
    exif_data: Option<ExifData>,
    content_credentials_certificate: Arc<Certificate>,
) -> color_eyre::Result<()> {
    let app_info = ApplicationInfo::new(APP_NAME.to_string(), "0.1.0".to_string(), None);
    let cc = ContentCredentials::new(content_credentials_certificate, image_data, Some(app_info));

    let gpg_fingerprint = "9768 CA63 F48D 3609 8567 A59D AEDE 3887 B155 1783";
    let gpg_key = "AEDE3887B1551783";

    cc.add_created_assertion()?;
    cc.add_restricted_ai_training_assertions()?;
    cc.add_instagram_assertion(USERNAME, USERNAME)?;
    cc.add_website_assertion("https://mtrnord.blog".to_string())?;
    cc.add_pgp_assertion(gpg_fingerprint, gpg_key)?;

    if let Some(exif_data) = exif_data {
        cc.add_exif_assertion(exif_data)?;
    }

    cc.embed_manifest(Some(export_path.clone()))?;

    // Bugfix: Delete any residual file with the same name and extension in /tmp
    let file_name = export_path.file_name().unwrap().to_str().unwrap();
    let tmp_file = std::env::temp_dir().join(file_name);
    if tmp_file.exists() {
        std::fs::remove_file(tmp_file)?;
    }

    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct C2PASigner {
    // Export folder to move files to
    export_folder: PathBuf,
    // C2PA folder to store certificates
    c2pa_folder: PathBuf,
    // Nats consumer to work with
    consumer: Consumer<Config>,
}

impl C2PASigner {
    pub async fn new(
        export_folder: PathBuf,
        c2pa_folder: PathBuf,
        import_stream: jetstream::stream::Stream,
    ) -> color_eyre::Result<Self> {
        let consumer = import_stream
            .create_consumer(Config {
                durable_name: Some("processor-2".to_string()),
                filter_subject: "import.sign_request".to_string(),
                ..Default::default()
            })
            .await?;

        Ok(Self {
            export_folder,
            c2pa_folder,
            consumer,
        })
    }

    pub async fn consumer(&self) -> color_eyre::Result<()> {
        let mut messages = self.consumer.messages().await?;
        info!("Signing consumer started");
        let (_, content_credentials_certificate) = load_certificates(self.c2pa_folder.clone())?;
        while let Some(msg) = messages.next().await {
            match msg {
                Ok(msg) => {
                    let event: super::jetstream_events::C2PASignRequest =
                        bincode::deserialize(&msg.payload).unwrap();
                    debug!("Received message: {:#?}", event);

                    // Initial ack to notify that we received the message
                    msg.ack_with(AckKind::Progress).await.unwrap();

                    // Check if the file still is in the export

                    // First get absolute export folder path
                    let export_folder = self.export_folder.canonicalize()?;

                    // Check if the file is still there
                    if !event.path.exists() {
                        error!("[C2PA Signer] file does not exist: {:?}", event.path);
                        // We also acknowledge the message to remove it from the queue
                        msg.ack_with(AckKind::Term).await.unwrap();
                        continue;
                    }

                    // Then check if the file is in the import folder
                    if !event.path.starts_with(export_folder.clone()) {
                        error!(
                            "[C2PA Signer] file is not in the export_folder folder: {:?}",
                            event.path
                        );

                        // We also acknowledge the message to remove it from the queue
                        msg.ack_with(AckKind::Term).await.unwrap();
                        continue;
                    }

                    // Ignore if there is a c2pa file with the same name next to it
                    let c2pa_path = event.path.with_extension("c2pa");
                    if c2pa_path.exists() {
                        error!("[C2PA Signer] c2pa file already exists: {:?}", c2pa_path);
                        // We also acknowledge the message to remove it from the queue
                        msg.ack_with(AckKind::Term).await.unwrap();
                        continue;
                    }

                    // Another progress ack to notify that we are processing the file
                    msg.ack_with(AckKind::Progress).await.unwrap();

                    let image_path = &event.path;
                    let cloned_path = image_path.clone();
                    let filename = cloned_path.file_name().unwrap().to_str().unwrap();

                    let exif_data = event.exif.map(|exif| exif.into());

                    if let Ok(reader) = Reader::from_file(image_path) {
                        if let Some(manifest) = reader.active_manifest() {
                            if manifest.claim_generator.contains(APP_NAME) {
                                error!("[C2PA Signer] file already signed by us: {:?}", event.path);
                                // We also acknowledge the message to remove it from the queue
                                msg.ack_with(AckKind::Term).await.unwrap();
                                continue;
                            }
                        }
                    }

                    sign_image(
                        image_path.clone(),
                        FileData::new(Some(image_path.clone()), None, Some(filename.to_string())),
                        exif_data,
                        content_credentials_certificate.clone(),
                    )?;

                    // Acknowledge the message to remove it from the queue
                    msg.ack_with(AckKind::Ack).await.unwrap();
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                Err(e) => {
                    error!("[C2PA Signer] error receiving message: {:?}", e);
                }
            }
        }
        error!("[C2PA Signer] Consumer stopped");
        Ok(())
    }
}
