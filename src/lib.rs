use std::io;
use std::sync::Arc;

use io::BufWriter;
use io::Write;

use arrow::record_batch::RecordBatch;

use bollard::ClientVersion;
use bollard::Docker;
use bollard::models::Volume;
use bollard::query_parameters::ListVolumesOptions;

pub async fn list_volumes(
    d: &Docker,
    opts: Option<ListVolumesOptions>,
) -> Result<Vec<Volume>, io::Error> {
    let res = d.list_volumes(opts).await.map_err(io::Error::other)?;
    Ok(res.volumes.unwrap_or_default())
}

use arrow::array::ArrayRef;
use arrow::array::builder::StringBuilder;
use arrow::datatypes::{DataType, Field, Schema};

pub fn a_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("driver", DataType::Utf8, false),
        Field::new("mountpoint", DataType::Utf8, false),
        Field::new("created_at", DataType::Utf8, true),
        // Field::new("scope", DataType::Utf8, true),
    ]))
}

pub fn volumes2batch(volumes: Vec<Volume>, schema: Arc<Schema>) -> Result<RecordBatch, io::Error> {
    let mut name_builder = StringBuilder::new();
    let mut driver_builder = StringBuilder::new();
    let mut mountpoint_builder = StringBuilder::new();
    let mut created_at_builder = StringBuilder::new();
    // let mut scope_builder = StringBuilder::new();

    for v in volumes {
        name_builder.append_value(v.name);
        driver_builder.append_value(v.driver);
        mountpoint_builder.append_value(v.mountpoint);
        created_at_builder.append_option(v.created_at);
        // scope_builder.append_option(v.scope);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(name_builder.finish()),
        Arc::new(driver_builder.finish()),
        Arc::new(mountpoint_builder.finish()),
        Arc::new(created_at_builder.finish()),
        // Arc::new(scope_builder.finish()),
    ];

    RecordBatch::try_new(schema, columns).map_err(io::Error::other)
}

pub struct IpcStreamWriter<W>(pub arrow::ipc::writer::StreamWriter<BufWriter<W>>)
where
    W: Write;

impl<W> IpcStreamWriter<W>
where
    W: Write,
{
    pub fn finish(&mut self) -> Result<(), io::Error> {
        self.0.finish().map_err(io::Error::other)
    }

    pub fn flush(&mut self) -> Result<(), io::Error> {
        self.0.flush().map_err(io::Error::other)
    }

    pub fn write_batch(&mut self, b: &RecordBatch) -> Result<(), io::Error> {
        self.0.write(b).map_err(io::Error::other)
    }
}

pub fn batch2writer<W>(b: &RecordBatch, mut wtr: W, sch: &Schema) -> Result<(), io::Error>
where
    W: Write,
{
    let swtr = arrow::ipc::writer::StreamWriter::try_new_buffered(&mut wtr, sch)
        .map_err(io::Error::other)?;
    let mut iw = IpcStreamWriter(swtr);
    iw.write_batch(b)?;
    iw.flush()?;
    iw.finish()?;

    drop(iw);

    wtr.flush()
}

pub fn volumes2writer<W>(
    volumes: Vec<Volume>,
    mut wtr: W,
    sch: Arc<Schema>,
) -> Result<(), io::Error>
where
    W: Write,
{
    let batch = volumes2batch(volumes, sch.clone())?;
    batch2writer(&batch, &mut wtr, &sch)
}

pub async fn list_volumes_and_write<W>(
    d: &Docker,
    mut wtr: W,
    opts: Option<ListVolumesOptions>,
) -> Result<(), io::Error>
where
    W: Write,
{
    let volumes = list_volumes(d, opts).await?;
    let schema = a_schema();
    volumes2writer(volumes, &mut wtr, schema)
}

pub fn unix2docker(
    sock_path: &str,
    timeout_seconds: u64,
    client_version: &ClientVersion,
) -> Result<Docker, io::Error> {
    Docker::connect_with_unix(sock_path, timeout_seconds, client_version).map_err(io::Error::other)
}

pub const DOCKER_UNIX_PATH_DEFAULT: &str = "/var/run/docker.sock";
pub const DOCKER_CON_TIMEOUT_SECONDS_DEFAULT: u64 = 30;
pub const DOCKER_CLIENT_VERSION_DEFAULT: &ClientVersion = bollard::API_DEFAULT_VERSION;
