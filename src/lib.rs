use csv::Reader;
use flate2::write::GzEncoder;
use flate2::Compression;
use rayon::prelude::*;
use std::cmp::{max, min};
use std::cmp::{Eq, Ord};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::File;
use std::hash::Hash;
use std::io::prelude::*;
use std::path::Path;
use std::thread;

#[derive(Debug, Hash, Eq, PartialEq, Ord, PartialOrd, Clone)]
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

fn normalized_compression_distance(cx1: &usize, cx2: &usize, cx1x2: &usize) -> f64 {
    (cx1x2 - min(cx1, cx2)) as f64 / *max(cx1, cx2) as f64
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

fn compress(data: &mut Reader<File>) -> HashSet<Record> {
    // Turn the records into a vector for parallel chunking
    let records = data.records().collect::<Result<Vec<_>, _>>().unwrap();

    // Create a closure that processes the records in a chunks and add the output to an internal map
    let process_chunk = |chunk: &[csv::StringRecord]| -> HashSet<Record> {
        // let mut map = HashMap::new();
        let mut set = HashSet::new();

        for record in chunk {
            let class = &record[0];
            let title = &record[1];
            let text = &record[2];
            let compressed_text_len = compressed_len(text);

            let record = Record::new(class, title, text, compressed_text_len);

            set.insert(record);
        }

        set
    };

    let chunk_size = records.len() / thread::available_parallelism().unwrap();

    // Process the records in chunks
    let chunk_sets = records
        .par_chunks(chunk_size)
        .map(process_chunk)
        .collect::<Vec<_>>();

    // Combine the chunks:
    let mut set = HashSet::new();

    for chunk_set in chunk_sets {
        set.extend(chunk_set)
    }

    set
}

fn compare(test: &HashSet<Record>, train: &HashSet<Record>, k: &usize) -> Vec<(Record, String)> {
    // Will hold the test set's title and predicted class
    let mut predictions: Vec<(Record, String)> = Vec::new();

    for test_record in test.iter() {
        // Collect the distance from each train record to the single test record
        let mut distance: Vec<(&Record, f64)> = Vec::new();

        for train_record in train.iter() {
            // Combined compressed length
            let combined = format!("{}{}", test_record.text, train_record.text);
            let cx1x2 = compressed_len(&combined);

            // Find NCD
            let cx1 = &test_record.compressed_len;
            let cx2 = &train_record.compressed_len;
            let ncd = normalized_compression_distance(cx1, cx2, &cx1x2);

            // Collect distances along with their corresponding classes
            distance.push((&train_record, ncd));
        }

        // Sort the distances by NCD (smaller is better)
        distance.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        // Pick the top k records from the sorted distances
        let top_k = distance.iter().take(*k).collect::<Vec<_>>();

        // Find the most common class
        let mut class_count = HashMap::new();

        for &(record, _ncd) in top_k.iter() {
            *class_count.entry(record.class.clone()).or_insert(0) += 1;
        }

        // Get the predicted class (most common occurrence of class in the top k)
        let predicted_class = class_count
            .iter()
            .max_by_key(|&(_, count)| count)
            .map(|(class, _)| class.clone())
            .unwrap();

        // Push to prediction the test record, and its predicted class
        predictions.push((test_record.clone(), predicted_class));
    }

    predictions
}

pub fn run(test: &str, train: &str) -> Result<(), Box<dyn Error>> {
    let mut test_data = read_csv(test)?;
    let mut train_data = read_csv(train)?;

    let train_map = compress(&mut train_data);
    let test_map = compress(&mut test_data);

    println!("Trainmap len: {}", train_map.len());
    println!("Testmap len: {}", test_map.len());

    let k: usize = 3;

    let predictions = compare(&test_map, &train_map, &k);

    for (title, class) in predictions {
        println!("Predicted Classes: {:#?}\nTitle: {:#?}.\n", class, title);
    }

    Ok(())
}
