#![feature(slice_split_once)]

use anyhow::Result;
use fxhash::FxHashMap;
use serde::{ser::SerializeStruct, Serialize, Serializer};
use std::{
    cmp::{max, min},
    collections::BTreeMap,
    fs::File,
    io::Write,
    sync::mpsc::channel,
};

const MEASUREMENTS_FILE: &str = "./data/measurements.txt";
const OUTPUT_FILE: &str = "./out/aggregate.json";

const ONE_KIB: usize = 1_024;
const ONE_MIB: usize = ONE_KIB * 1_024;
const ONE_GIB: usize = ONE_MIB * 1_024;
const CHUNK_SIZE: usize = ONE_MIB * 256;

#[derive(Debug)]
struct Data<'a> {
    m: FxHashMap<&'a [u8], Measurements>,
}

impl<'a> Data<'a> {
    fn new() -> Self {
        Self {
            m: FxHashMap::<&'a [u8], Measurements>::default(),
        }
    }

    fn record(&mut self, station: &'a [u8], reading: i64) {
        self.m
            .entry(station)
            .and_modify(move |m| m.record(reading))
            .or_insert_with(|| Measurements::new(reading));
    }

    fn merge(&mut self, mut other: Data<'a>) {
        for (station, other) in other.m.drain() {
            self.m
                .entry(station)
                .and_modify(move |m| m.merge(other))
                .or_insert(other);
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Measurements {
    min: i64,
    max: i64,
    sum: i64,
    n: usize,
}

impl Measurements {
    fn new(initial_reading: i64) -> Self {
        Measurements {
            min: initial_reading,
            max: initial_reading,
            sum: initial_reading,
            n: 1,
        }
    }

    fn record(&mut self, reading: i64) {
        self.min = min(self.min, reading);
        self.max = max(self.max, reading);
        self.sum = self.sum.wrapping_add(reading);
        self.n = self.n.wrapping_add(1);
    }

    fn merge(&mut self, other: Measurements) {
        self.min = min(self.min, other.min);
        self.max = max(self.max, other.max);
        self.sum = self.sum.wrapping_add(other.sum);
        self.n = self.n.wrapping_add(other.n);
    }

    fn avg(&self) -> f64 {
        self.sum as f64 / self.n as f64
    }
}
impl Serialize for Measurements {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut x = serializer.serialize_struct("Measurements", 3)?;
        x.serialize_field("min", &(self.min as f64 / 10.0))?;
        x.serialize_field("max", &(self.max as f64 / 10.0))?;
        x.serialize_field("avg", &(self.avg() / 10.0))?;
        x.end()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let start = std::time::Instant::now();

    let file = File::open(MEASUREMENTS_FILE)?;
    let mapped_file = unsafe { memmap2::Mmap::map(&file) }?;
    let data: &[u8] = &*mapped_file;
    println!(
        "Memory mapped {:.2} GiB file",
        data.len() as f64 / ONE_GIB as f64
    );

    let start_chunking = std::time::Instant::now();
    let chunks: Vec<&[u8]> = Chunks::from_source(data, CHUNK_SIZE).collect();
    println!(
        "Collected {} chunks in {}ms",
        chunks.len(),
        start_chunking.elapsed().as_millis()
    );
    println!("Processing {} chunks...", chunks.len());

    // Process chunks
    let start_processing = std::time::Instant::now();
    let (sender, receiver) = channel::<(usize, Data)>();
    async_scoped::TokioScope::scope_and_block(move |scope| {
        for (chunk_idx, chunk) in chunks.into_iter().enumerate() {
            let sender = sender.clone();
            scope.spawn(async move {
                let mut chunk_data = Data::new();
                for line in chunk.split(|it| *it == b'\n') {
                    if !line.is_empty() {
                        let (station, reading) = parse_line(line);
                        chunk_data.record(station, reading);
                    }
                }
                sender.send((chunk_idx, chunk_data)).unwrap();
            });
        }
        drop(sender);
    });
    let mut results = Data::new();
    while let Ok((_chunk_idx, chunk_data)) = receiver.recv() {
        results.merge(chunk_data);
    }
    println!(
        "Processed in {:.4}s",
        start_processing.elapsed().as_secs_f32()
    );

    // Sort
    let start_sorting = std::time::Instant::now();
    let mut data = BTreeMap::<&str, Measurements>::new();
    for (k, v) in results.m.into_iter() {
        data.insert(unsafe { std::str::from_utf8_unchecked(k) }, v);
    }
    println!(
        "Sorted results in {}ms",
        start_sorting.elapsed().as_millis()
    );

    // Write output
    let start_serializing = std::time::Instant::now();
    let serialized = serde_json::to_string_pretty(&data)?;
    let mut out_file = File::create(OUTPUT_FILE)?;
    out_file.write_all(serialized.as_bytes())?;
    println!(
        "Serialized and wrote {} results in {}ms",
        data.len(),
        start_serializing.elapsed().as_millis()
    );

    println!("Completed in {}s", start.elapsed().as_secs_f32());
    Ok(())
}

fn next_chunk(data: &[u8], start: usize, chunk_size: usize) -> &[u8] {
    if data[start..].len() <= chunk_size {
        return &data[start..];
    }
    let mut end: usize = start + chunk_size;
    while end > start && data[end - 1] != b'\n' {
        end -= 1;
    }
    &data[start..end]
}

#[inline(always)]
fn parse_line(line: &[u8]) -> (&[u8], i64) {
    let (station, reading) = line.split_once(|it| *it == b';').expect("expected a ';'");
    (station, parse_reading(reading))
}

#[inline(always)]
fn parse_reading(reading: &[u8]) -> i64 {
    let is_neg = reading[0] == b'-';
    let len = reading.len();
    let (d1, d2, d3) = match (is_neg, len) {
        (false, 3) => (0, reading[0] - b'0', reading[2] - b'0'),
        (false, 4) => (reading[0] - b'0', reading[1] - b'0', reading[3] - b'0'),
        (true, 4) => (0, reading[1] - b'0', reading[3] - b'0'),
        (true, 5) => (reading[1] - b'0', reading[2] - b'0', reading[4] - b'0'),
        _ => unreachable!(),
    };
    let reading = (d1 as i64 * 100) + (d2 as i64 * 10) + (d3 as i64);
    if is_neg {
        -reading
    } else {
        reading
    }
}

struct Chunks<'a> {
    source: &'a [u8],
    start: usize,
    chunk_size: usize,
}

impl<'a> Chunks<'a> {
    fn from_source(source: &'a [u8], chunk_size: usize) -> Self {
        Self {
            source,
            start: 0,
            chunk_size,
        }
    }
}

impl<'a> Iterator for Chunks<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let chunk = next_chunk(self.source, self.start, self.chunk_size);
        match chunk.len() {
            0 => None,
            len => {
                self.start += len;
                Some(chunk)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{next_chunk, parse_line};

    #[test]
    fn test_parse_line() {
        let line = "Station Name;-23.4".as_bytes();
        let (station, reading) = parse_line(line);
        assert_eq!(station, "Station Name".as_bytes());
        assert_eq!(reading, -234);

        let line = "Station Name;-1.5".as_bytes();
        let (station, reading) = parse_line(line);
        assert_eq!(station, "Station Name".as_bytes());
        assert_eq!(reading, -15);

        let line = "Station Name;56.7".as_bytes();
        let (station, reading) = parse_line(line);
        assert_eq!(station, "Station Name".as_bytes());
        assert_eq!(reading, 567);

        let line = "Station Name;99.9".as_bytes();
        let (station, reading) = parse_line(line);
        assert_eq!(station, "Station Name".as_bytes());
        assert_eq!(reading, 999);
    }

    #[test]
    fn test_next_chunk() {
        let data = b"A\nBar\nBaz\n";

        let chunk_1 = next_chunk(data, 0, 7);
        let chunk_2 = next_chunk(data, chunk_1.len(), 7);
        let chunk_3 = next_chunk(data, chunk_1.len() + chunk_2.len(), 7);

        assert_eq!(chunk_1, b"A\nBar\n");
        assert_eq!(chunk_2, b"Baz\n");
        assert_eq!(chunk_3, b"");
    }
}
