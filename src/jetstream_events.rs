use std::borrow::Cow;
use std::path::PathBuf;

use simple_c2pa::ExifData;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct FileToImport {
    pub path: PathBuf,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ExifCleanupRequest {
    pub path: PathBuf,
}

// Essentially a transport wrapper for [simple_c2pa::assertions::ExifData]
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct Exif<'a> {
    pub gps_version_id: Option<Cow<'a, str>>,
    pub latitude: Option<Cow<'a, str>>,
    pub longitude: Option<Cow<'a, str>>,
    pub altitude_ref: Option<u8>,
    pub altitude: Option<Cow<'a, str>>,
    pub timestamp: Option<Cow<'a, str>>,
    pub speed_ref: Option<Cow<'a, str>>,
    pub speed: Option<Cow<'a, str>>,
    pub direction_ref: Option<Cow<'a, str>>,
    pub direction: Option<Cow<'a, str>>,
    pub destination_bearing_ref: Option<Cow<'a, str>>,
    pub destination_bearing: Option<Cow<'a, str>>,
    pub positioning_error: Option<Cow<'a, str>>,
    pub exposure_time: Option<Cow<'a, str>>,
    pub f_number: Option<f64>,
    pub color_space: Option<u8>,
    pub digital_zoom_ratio: Option<f64>,
    pub make: Option<Cow<'a, str>>,
    pub model: Option<Cow<'a, str>>,
    pub lens_make: Option<Cow<'a, str>>,
    pub lens_model: Option<Cow<'a, str>>,
    pub lens_specification: Option<Vec<f64>>,
}

impl<'a> From<Exif<'a>> for ExifData<'a> {
    fn from(exif: Exif<'a>) -> Self {
        ExifData {
            gps_version_id: exif.gps_version_id,
            latitude: exif.latitude,
            longitude: exif.longitude,
            altitude_ref: exif.altitude_ref,
            altitude: exif.altitude,
            timestamp: exif.timestamp,
            speed_ref: exif.speed_ref,
            speed: exif.speed,
            direction_ref: exif.direction_ref,
            direction: exif.direction,
            destination_bearing_ref: exif.destination_bearing_ref,
            destination_bearing: exif.destination_bearing,
            positioning_error: exif.positioning_error,
            exposure_time: exif.exposure_time,
            f_number: exif.f_number,
            color_space: exif.color_space,
            digital_zoom_ratio: exif.digital_zoom_ratio,
            make: exif.make,
            model: exif.model,
            lens_make: exif.lens_make,
            lens_model: exif.lens_model,
            lens_specification: exif.lens_specification,
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct C2PASignRequest<'a> {
    pub path: PathBuf,
    pub exif: Option<Exif<'a>>,
}
