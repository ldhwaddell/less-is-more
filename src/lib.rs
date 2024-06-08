use csv::Reader;
use flate2::write::GzEncoder;
use flate2::Compression;
use rayon::prelude::*;
use std::cmp::{max, min};
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::thread;

fn normalized_compression_distance(cx1: usize, cx2: usize, cx1x2: usize) -> usize {
    (cx1x2 - min(cx1, cx2)) / max(cx1, cx2)
}

fn compressed_len(data: &str) -> usize {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data.as_bytes()).unwrap();
    encoder.finish().unwrap().len()
}

fn read_csv<P: AsRef<Path>>(path: P) -> Result<Reader<File>, Box<dyn Error>> {
    let rdr = Reader::from_path(path)?;
    Ok(rdr)
}

fn compress(data: &mut Reader<File>) -> HashMap<String, usize> {
    // Turn the records into a vector for parallel chunking
    let records = data.records().collect::<Result<Vec<_>, _>>().unwrap();

    // Create a closure that processes the records in a chunks and add the output to an internal map
    let process_chunk = |chunk: &[csv::StringRecord]| -> HashMap<String, usize> {
        let mut map = HashMap::new();

        for record in chunk {
            let title = &record[1];
            let compressed_text_len = compressed_len(&record[2]);
            map.insert(title.to_string(), compressed_text_len);
        }

        map
    };

    let chunk_size = records.len() / thread::available_parallelism().unwrap();

    // Process the records in chunks
    let chunk_maps = records
        .par_chunks(chunk_size)
        .map(process_chunk)
        .collect::<Vec<_>>();

    // Combine the chunks:
    let mut map = HashMap::new();

    for chunk_map in chunk_maps {
        map.extend(chunk_map)
    }

    map
}

fn compare(test: HashMap<String, usize>, train: HashMap<String, usize>){
    test.par_iter()

}

pub fn run(test: &str, train: &str) -> Result<(), Box<dyn Error>> {
    let mut test_data = read_csv(test)?;
    let mut train_data = read_csv(train)?;

    let training_map = compress(&mut train_data);
    let test_map = compress(&mut test_data);

    for test_sample in test_map {}

    print!("len: {}", training_map.len());

    Ok(())
}
