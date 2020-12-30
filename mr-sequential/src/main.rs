use std::fs;
use std::io::Write;
use std::path;

pub struct KeyValue {
    key: String,
    value: u32,
}

fn map(_filename: &str, contents: String) -> Vec<KeyValue> {
    let mut kva = Vec::new();

    for word in contents.split(|c: char| !c.is_alphabetic()) {
        if !word.is_empty() {
            kva.push(KeyValue {
                key: word.to_owned(),
                value: 1,
            });
        }
    }

    kva
}

fn reduce(_key: &str, values: &Vec<u32>) -> String {
    values.len().to_string()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: mrsequential inputfiles...\n");
        std::process::exit(1);
    }

    let mut intermediate = Vec::new();
    for file in args.iter().skip(1) {
        let contents = fs::read_to_string(file.as_str()).unwrap();
        let map = map(file.as_str(), contents);
        intermediate.extend(map);
    }

    intermediate.sort_by(|a, b| a.key.cmp(&b.key));

    let mut i = 0;

    if !path::Path::new("output").exists() {
        fs::create_dir("output").unwrap();
    }
    fs::File::create("output/mr-out.out").unwrap();
    let mut outfile = std::fs::OpenOptions::new()
        .append(true)
        .open("output/mr-out.out")
        .unwrap();

    while i < intermediate.len() {
        let mut j = i + 1;
        while j < intermediate.len() && intermediate[j].key == intermediate[i].key {
            j += 1;
        }

        let mut values = Vec::new();
        for k in i..j {
            values.push(intermediate[k].value.clone());
        }
        let output = reduce(&intermediate[i].key, &values);

        writeln!(&mut outfile, "{} {}", intermediate[i].key, output).unwrap();

        i = j;
    }
}
