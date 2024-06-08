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

#[derive(Debug)]
struct Record {
    class: String,
    title: String,
    text: String,
    compressed_len: usize,
}

impl Record {
    fn new(class: &str, title: &str, text: &str, compressed_len: usize) -> Self {
        Record {
            class: class.into(),
            title: title.into(),
            text: text.into(),
            compressed_len,
        }
    }
}

fn normalized_compression_distance(cx1: &usize, cx2: &usize, cx1x2: &usize) -> usize {
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

fn compress(data: &mut Reader<File>) -> HashMap<String, Record> {
    // Turn the records into a vector for parallel chunking
    let records = data.records().collect::<Result<Vec<_>, _>>().unwrap();

    // Create a closure that processes the records in a chunks and add the output to an internal map
    let process_chunk = |chunk: &[csv::StringRecord]| -> HashMap<String, Record> {
        let mut map = HashMap::new();

        for record in chunk {
            let class = &record[0];
            let title = &record[1];
            let text = &record[2];
            let compressed_text_len = compressed_len(text);

            let record = Record::new(class, title, text, compressed_text_len);

            map.insert(title.to_string(), record);
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

fn compare(
    test: &HashMap<String, Record>,
    train: &HashMap<String, Record>,
    k: &u8,
) -> Vec<(String, Vec<usize>)> {
    // Will hold the test set's title and predicted class
    let predictions: Vec<(String, Vec<usize>)> = Vec::new();

    for (test_title, test_record) in test.iter() {
        let mut distance: Vec<_> = Vec::new();

        for (train_title, train_record) in train.iter() {
            // Combined compressed length
            let combined = format!("{}{}", test_record.text, train_record.text);
            let cx1x2 = compressed_len(&combined);

            // Find NCD
            let cx1 = &test_record.compressed_len;
            let cx2 = &train_record.compressed_len;
            let ncd = normalized_compression_distance(cx1, cx2, &cx1x2);

            // Collect distances along with their corresponding classes
        }
    }

    predictions
}

pub fn run(test: &str, train: &str) -> Result<(), Box<dyn Error>> {
    let mut test_data = read_csv(test)?;
    let mut train_data = read_csv(train)?;

    let train_map = compress(&mut train_data);
    let test_map = compress(&mut test_data);

    let k: u8 = 3;

    // let predictions = compare(&test_map, &train_map, &k);

    // for (title, class) in predictions {
    //     println!("Predicted Classes: {:#?}\nTitle: {}.\n", class, title);
    // }

    Ok(())
}
