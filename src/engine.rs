//! A tiny, synchronous, local-filesystem [`Engine`].
//!
//! The kernel decides *what* to read and write; the engine does the actual I/O
//! and data processing through four handlers:
//!
//! * [`LocalStorage`]: list / read / put / head / delete files (here: `std::fs`)
//! * [`LocalJson`]: read and write JSON commit files
//! * [`LocalParquet`]: read and write Parquet data and checkpoint files
//! * the kernel's Arrow `EvaluationHandler`, reused as-is for expressions
//!
//! Only `file://` URLs are supported.
use std::fs;
use std::io::{BufReader, Cursor, ErrorKind, Write};
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::json::ReaderBuilder;
use delta_kernel::engine::arrow_conversion::TryFromArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
use delta_kernel::engine::arrow_utils::{
    RowIndexBuilder, build_json_reorder_indices, fixup_json_read, fixup_parquet_read,
    json_arrow_schema, ordering_needs_row_indexes, parquet_read_plan, parse_json, to_json_bytes,
};
use delta_kernel::engine::parquet_row_group_skipping::ParquetRowGroupSkipping;
use delta_kernel::engine::{reader_options, writer_options};
use delta_kernel::engine_data::FilteredEngineData;
use delta_kernel::parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ParquetRecordBatchReaderBuilder,
};
use delta_kernel::parquet::arrow::arrow_writer::ArrowWriter;
use delta_kernel::schema::{SchemaRef, StructType};
use delta_kernel::transaction::BoundWriteContext;
use delta_kernel::{
    Engine, EngineData, EvaluationHandler, FileDataReadResultIterator, FileMeta, FileSize,
    FileSlice, JsonHandler, KernelError, KernelResult, KernelResultIterator,
    KernelResultIteratorStatic, ParquetFooter, ParquetHandler, PredicateRef, StorageHandler,
};
use delta_kernel_default_engine::{
    build_add_file_metadata, parquet::DataFileMetadata, stats::collect_stats,
};
use url::Url;

/// An [`Engine`] that reads and writes the local filesystem.
pub struct LocalEngine {
    storage: Arc<LocalStorage>,
    json: Arc<LocalJson>,
    parquet: Arc<LocalParquet>,
    evaluation: Arc<ArrowEvaluationHandler>,
}

impl LocalEngine {
    pub fn new() -> Self {
        Self {
            storage: Arc::new(LocalStorage),
            json: Arc::new(LocalJson),
            parquet: Arc::new(LocalParquet),
            evaluation: Arc::new(ArrowEvaluationHandler {}),
        }
    }

    /// Write `batch` as a new Parquet file in the table and return the `add`
    /// action metadata to hand to `Transaction::add_files`.
    ///
    /// Not part of the [`Engine`] trait: the kernel leaves "how do I turn my
    /// data into a data file" entirely to the engine.
    pub fn write_data_file(
        &self,
        batch: RecordBatch,
        context: &BoundWriteContext,
    ) -> KernelResult<Box<dyn EngineData>> {
        // Logical -> physical (column mapping, partition columns, ...).
        let input_schema = StructType::try_from_arrow(batch.schema().as_ref())?;
        let physical = self
            .evaluation
            .new_expression_evaluator(
                Arc::new(input_schema),
                context.logical_to_physical().clone(),
                context.physical_data_schema().clone().into(),
            )?
            .evaluate(&ArrowEngineData::new(batch))?;
        let physical: RecordBatch = (*ArrowEngineData::try_from_engine_data(physical)?).into();

        let stats = collect_stats(
            &physical,
            context.stats_columns(),
            context.physical_data_schema().as_ref(),
        )?;

        let mut dir = context.write_dir();
        if !dir.path().ends_with('/') {
            dir.set_path(&format!("{}/", dir.path()));
        }
        let location = dir.join(&format!("{}.parquet", uuid::Uuid::new_v4()))?;
        let data: Box<dyn EngineData> = Box::new(ArrowEngineData::new(physical));
        self.parquet
            .write_parquet_file(location.clone(), Box::new(std::iter::once(Ok(data))))?;
        let meta = self.storage.head(&location)?;

        build_add_file_metadata(DataFileMetadata::new(meta, stats), context)
    }
}

impl Default for LocalEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl Engine for LocalEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.evaluation.clone()
    }
    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.storage.clone()
    }
    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json.clone()
    }
    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet.clone()
    }
}

// ---------------------------------------------------------------- storage --

pub struct LocalStorage;

fn to_path(url: &Url) -> KernelResult<PathBuf> {
    url.to_file_path()
        .map_err(|()| KernelError::generic(format!("not a local file URL: {url}")))
}

fn to_url(path: &std::path::Path) -> KernelResult<Url> {
    Url::from_file_path(path)
        .map_err(|()| KernelError::generic(format!("invalid path: {}", path.display())))
}

fn file_meta(url: Url, meta: &fs::Metadata) -> FileMeta {
    let modified = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0, |d| d.as_millis() as i64);
    FileMeta {
        location: url,
        last_modified: modified,
        size: meta.len(),
    }
}

fn read_file(url: &Url) -> KernelResult<Bytes> {
    match fs::read(to_path(url)?) {
        Ok(b) => Ok(b.into()),
        Err(e) if e.kind() == ErrorKind::NotFound => Err(KernelError::file_not_found(url)),
        Err(e) => Err(e.into()),
    }
}

fn put_file(url: &Url, data: &[u8], overwrite: bool) -> KernelResult<()> {
    let path = to_path(url)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut options = fs::OpenOptions::new();
    options.write(true);
    // `create_new` fails if the file exists: this is the atomic "create if
    // absent" that makes concurrent commits safe, since only one writer can
    // win a given version. (A crash mid-write can leave a partial file; a
    // production engine would write to a temp file and rename.)
    if overwrite {
        options.create(true).truncate(true);
    } else {
        options.create_new(true);
    }
    let mut file = options.open(&path).map_err(|e| match e.kind() {
        ErrorKind::AlreadyExists => KernelError::FileAlreadyExists(url.to_string()),
        _ => e.into(),
    })?;
    file.write_all(data)?;
    Ok(())
}

impl StorageHandler for LocalStorage {
    /// Files in the same directory as `path` whose name is `>=` its file name,
    /// in lexicographic order (a trailing `/` lists the whole directory).
    fn list_from(&self, path: &Url) -> KernelResult<KernelResultIteratorStatic<FileMeta>> {
        let full = to_path(path)?;
        let (dir, start) = if path.path().ends_with('/') {
            (full, String::new())
        } else {
            let name = full
                .file_name()
                .map_or_else(String::new, |n| n.to_string_lossy().into());
            (full.parent().map(PathBuf::from).unwrap_or_default(), name)
        };
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            // A table that doesn't exist yet simply has no files.
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Box::new(std::iter::empty())),
            Err(e) => return Err(e.into()),
        };
        let mut files = Vec::new();
        for entry in entries {
            let entry = entry?;
            let meta = entry.metadata()?;
            if meta.is_dir() || entry.file_name().to_string_lossy().as_ref() < start.as_str() {
                continue;
            }
            files.push(file_meta(to_url(&entry.path())?, &meta));
        }
        files.sort_by(|a, b| a.location.cmp(&b.location));
        Ok(Box::new(files.into_iter().map(Ok)))
    }

    fn read_files(&self, files: Vec<FileSlice>) -> KernelResult<KernelResultIteratorStatic<Bytes>> {
        Ok(Box::new(files.into_iter().map(|(url, range)| {
            let bytes = read_file(&url)?;
            Ok(match range {
                Some(r) => bytes.slice(r.start as usize..(r.end as usize).min(bytes.len())),
                None => bytes,
            })
        })))
    }

    fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> KernelResult<()> {
        put_file(path, &data, overwrite)
    }

    fn copy_atomic(&self, src: &Url, dest: &Url) -> KernelResult<()> {
        put_file(dest, &read_file(src)?, false)
    }

    fn head(&self, path: &Url) -> KernelResult<FileMeta> {
        match fs::metadata(to_path(path)?) {
            Ok(meta) => Ok(file_meta(path.clone(), &meta)),
            Err(e) if e.kind() == ErrorKind::NotFound => Err(KernelError::file_not_found(path)),
            Err(e) => Err(e.into()),
        }
    }

    fn delete(&self, path: &Url) -> KernelResult<()> {
        match fs::remove_file(to_path(path)?) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

// ------------------------------------------------------------------- json --

pub struct LocalJson;

impl JsonHandler for LocalJson {
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> KernelResult<Box<dyn EngineData>> {
        parse_json(json_strings, output_schema)
    }

    fn read_json_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        _predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        let arrow_schema = Arc::new(json_arrow_schema(&schema)?);
        let reorder = build_json_reorder_indices(&schema)?;
        let mut out = Vec::new();
        for file in files {
            let location = file.location.to_string();
            let reader = ReaderBuilder::new(arrow_schema.clone())
                .with_coerce_primitive(true)
                .build(BufReader::new(Cursor::new(read_file(&file.location)?)))?;
            for batch in reader {
                out.push(
                    fixup_json_read(batch?, &reorder, &location)
                        .map(|d| Box::new(d) as Box<dyn EngineData>),
                );
            }
        }
        Ok(Box::new(out.into_iter()))
    }

    fn write_json_file(
        &self,
        path: &Url,
        data: KernelResultIterator<'_, FilteredEngineData>,
        overwrite: bool,
    ) -> KernelResult<FileSize> {
        let bytes = to_json_bytes(data)?;
        put_file(path, &bytes, overwrite)?;
        Ok(bytes.len() as FileSize)
    }
}

// ---------------------------------------------------------------- parquet --

pub struct LocalParquet;

impl ParquetHandler for LocalParquet {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        let mut out = Vec::new();
        for file in files {
            let data = read_file(&file.location)?;
            let metadata = ArrowReaderMetadata::load(&data, reader_options())?;
            // Which columns to read, and how to reorder / fill in the result.
            let (ordering, mask) = parquet_read_plan(&schema, &metadata)?;
            let mut row_indexes = ordering_needs_row_indexes(&ordering)
                .then(|| RowIndexBuilder::new(metadata.metadata().row_groups()));

            let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(data, metadata);
            if let Some(mask) = mask {
                builder = builder.with_projection(mask);
            }
            // Skip row groups whose min/max stats can't match the predicate.
            if let Some(predicate) = &predicate {
                builder = builder.with_row_group_filter(predicate.as_ref(), row_indexes.as_mut());
            }
            let mut row_indexes = row_indexes.map(|b| b.build()).transpose()?;

            let location = file.location.to_string();
            for batch in builder.build()? {
                out.push(
                    fixup_parquet_read(
                        batch?,
                        &ordering,
                        row_indexes.as_mut(),
                        Some(&location),
                        Some(&schema),
                    )
                    .map(|d| Box::new(d) as Box<dyn EngineData>),
                );
            }
        }
        Ok(Box::new(out.into_iter()))
    }

    fn write_parquet_file(
        &self,
        location: Url,
        data: KernelResultIteratorStatic<Box<dyn EngineData>>,
    ) -> KernelResult<FileSize> {
        let batches = data
            .map(|b| Ok((*ArrowEngineData::try_from_engine_data(b?)?).into()))
            .collect::<KernelResult<Vec<RecordBatch>>>()?;
        let first = batches
            .first()
            .ok_or_else(|| KernelError::generic("cannot write a parquet file with no data"))?;

        let mut buffer = Vec::new();
        let mut writer =
            ArrowWriter::try_new_with_options(&mut buffer, first.schema(), writer_options())?;
        for batch in &batches {
            writer.write(batch)?;
        }
        writer.close()?; // writes the footer
        put_file(&location, &buffer, true)?;
        Ok(buffer.len() as FileSize)
    }

    fn read_parquet_footer(&self, file: &FileMeta) -> KernelResult<ParquetFooter> {
        let data = read_file(&file.location)?;
        let metadata = ArrowReaderMetadata::load(&data, reader_options())?;
        let schema = Arc::new(StructType::try_from_arrow(metadata.schema().as_ref())?);
        Ok(ParquetFooter { schema })
    }
}
