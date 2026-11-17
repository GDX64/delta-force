//! Ingest a generated CSV file (1 GB by default) and report time, CPU and
//! peak memory. The source is a virtual file, generated as it is read, so
//! the memory reported is ours, not the file's.
//!
//! ```text
//! cargo run --release --example ingest_1gb            # 1 GB
//! cargo run --release --example ingest_1gb -- 100MB
//! ```
use std::fmt::Write as _;
use std::ops::Range;
use std::time::Instant;

use bytes::Bytes;
use delta_force::test_util::TestStore;
use delta_force::{DataType, Lake, RowBatch, TableConfig};

const OUT: &str = "target/bench/ingest_1gb";
const HEADER: &[u8] = b"id,name,score,count\n";
/// Every row has the same width, so any byte range can be generated alone:
/// `0000000042,user0042,0.6598,00042\n`
const ROW: u64 = 33;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let size = parse_size(&std::env::args().nth(1).unwrap_or("1GB".into()))?;
    let rows = (size - HEADER.len() as u64) / ROW;
    let size = HEADER.len() as u64 + rows * ROW;

    let _ = std::fs::remove_dir_all(OUT);
    let store = TestStore::on_disk(OUT)?;
    store.create_virtual_file("landing/events.csv", size, generate);
    let lake = Lake::new(store.clone());
    let events = lake
        .create(
            TableConfig::new("events")
                .column("id", DataType::Long)
                .column("name", DataType::String)
                .column("score", DataType::Double)
                .column("count", DataType::Integer),
        )
        .await?;

    let io_before = store.stats();
    let before = process::stats();
    let start = Instant::now();
    let mut batch = RowBatch::new(events.schema());
    batch.push_file(&lake.file("landing/events.csv"));
    events.write(batch).await?;
    let wall = start.elapsed();
    let after = process::stats();

    let cpu = after.cpu - before.cpu;
    let (files, bytes) = parquet_files(OUT);
    println!("source       {}  ({rows} rows)", human(size));
    println!(
        "wall         {:.1} s  ({}/s)",
        wall.as_secs_f64(),
        human((size as f64 / wall.as_secs_f64()) as u64),
    );
    println!(
        "cpu          {:.1} s  ({:.0}%, ~{:.1} cores)",
        cpu.as_secs_f64(),
        100.0 * cpu.as_secs_f64() / wall.as_secs_f64(),
        cpu.as_secs_f64() / wall.as_secs_f64(),
    );
    println!(
        "peak memory  {}  (before ingest: {})",
        human(after.peak_memory),
        human(before.peak_memory)
    );
    println!("output       {} in {files} file(s)", human(bytes));
    let io = store.stats();
    println!(
        "reads        {} in {} get(s)",
        human(io.reads.bytes - io_before.reads.bytes),
        io.reads.gets - io_before.reads.gets
    );
    println!(
        "writes       {} in {} put(s)",
        human(io.writes.bytes - io_before.writes.bytes),
        io.writes.puts - io_before.writes.puts
    );
    for (path, file) in &io.files {
        let old = io_before.files.get(path).copied().unwrap_or_default();
        let (gets, puts) = (file.reads.gets - old.reads.gets, file.writes.puts - old.writes.puts);
        let read = human(file.reads.bytes - old.reads.bytes);
        let written = human(file.writes.bytes - old.writes.bytes);
        if gets > 0 {
            println!("  read   {read:>10} in {gets:>2} get(s)  {path}");
        }
        if puts > 0 {
            println!("  wrote  {written:>10} in {puts:>2} put(s)  {path}");
        }
    }
    Ok(())
}

/// The bytes of `range` of the generated CSV.
fn generate(range: Range<u64>) -> Bytes {
    let header = HEADER.len() as u64;
    let mut out = Vec::with_capacity((range.end - range.start) as usize);
    let mut line = String::new();
    let mut pos = range.start;
    while pos < range.end {
        let line_start = if pos < header {
            line = String::from_utf8_lossy(HEADER).into_owned();
            0
        } else {
            let row = (pos - header) / ROW;
            line.clear();
            let (name, score, count) = (row % 10_000, row * 7919 % 10_000, row % 100_000);
            writeln!(line, "{row:010},user{name:04},0.{score:04},{count:05}").unwrap();
            header + row * ROW
        };
        let from = (pos - line_start) as usize;
        let to = ((range.end - line_start) as usize).min(line.len());
        out.extend_from_slice(&line.as_bytes()[from..to]);
        pos = line_start + to as u64;
    }
    out.into()
}

fn parse_size(text: &str) -> anyhow::Result<u64> {
    let text = text.to_ascii_uppercase();
    let (number, unit) = text.split_at(
        text.find(|c: char| c.is_ascii_alphabetic())
            .unwrap_or(text.len()),
    );
    let unit = match unit {
        "" | "B" => 1,
        "KB" => 1 << 10,
        "MB" => 1 << 20,
        "GB" => 1 << 30,
        _ => anyhow::bail!("unknown size unit {unit:?}; use B, KB, MB or GB"),
    };
    Ok(number.parse::<u64>()? * unit)
}

fn human(bytes: u64) -> String {
    let units = ["B", "KB", "MB", "GB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < units.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.2} {}", units[unit])
}

/// Number and total size of the Parquet files under `dir`.
fn parquet_files(dir: impl AsRef<std::path::Path>) -> (usize, u64) {
    let mut found = (0, 0);
    for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let path = entry.path();
        if path.is_dir() {
            let (n, bytes) = parquet_files(&path);
            found = (found.0 + n, found.1 + bytes);
        } else if path.extension().is_some_and(|e| e == "parquet") {
            found = (
                found.0 + 1,
                found.1 + entry.metadata().map_or(0, |m| m.len()),
            );
        }
    }
    found
}

/// CPU time and peak memory of this process, from the OS.
mod process {
    use std::time::Duration;

    pub struct Stats {
        /// User + kernel time, across all threads.
        pub cpu: Duration,
        /// Peak resident memory (working set) so far, in bytes.
        pub peak_memory: u64,
    }

    #[cfg(windows)]
    pub fn stats() -> Stats {
        use windows_sys::Win32::Foundation::FILETIME;
        use windows_sys::Win32::System::ProcessStatus::{
            GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS,
        };
        use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetProcessTimes};

        let ticks = |t: FILETIME| (t.dwHighDateTime as u64) << 32 | t.dwLowDateTime as u64;
        let zero = FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        };
        let (mut created, mut exited, mut kernel, mut user) = (zero, zero, zero, zero);
        let mut memory: PROCESS_MEMORY_COUNTERS = unsafe { std::mem::zeroed() };
        unsafe {
            let process = GetCurrentProcess();
            GetProcessTimes(process, &mut created, &mut exited, &mut kernel, &mut user);
            let size = size_of::<PROCESS_MEMORY_COUNTERS>() as u32;
            GetProcessMemoryInfo(process, &mut memory, size);
        }
        Stats {
            // FILETIME counts 100 ns intervals.
            cpu: Duration::from_nanos((ticks(kernel) + ticks(user)) * 100),
            peak_memory: memory.PeakWorkingSetSize as u64,
        }
    }

    #[cfg(unix)]
    pub fn stats() -> Stats {
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
        let time = |t: libc::timeval| Duration::new(t.tv_sec as u64, t.tv_usec as u32 * 1000);
        // Linux reports kilobytes, macOS bytes.
        let scale = if cfg!(target_os = "macos") { 1 } else { 1024 };
        Stats {
            cpu: time(usage.ru_utime) + time(usage.ru_stime),
            peak_memory: usage.ru_maxrss as u64 * scale,
        }
    }
}
