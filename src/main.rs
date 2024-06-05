use less_is_more::run;
use std::process;

fn main() {
    let test = "./data/test.csv";
    let train = "./data/train.csv";

    if let Err(err) = run(test, train) {
        println!("error: {}", err);
        process::exit(1);
    }
}
