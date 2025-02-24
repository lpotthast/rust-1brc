# 1BRC challenge in Rust

Generate the measurements.txt file with

    python3 ./create_measurements.py 1_000_000_000

Run the program with

    cargo run --release

Example output (7950X, 16 cores)

    Memory mapped 14.80 GiB file
    Collected 60 chunks in 0ms
    Processing 60 chunks...
    Processed in 1.3324s
    Sorted results in 1ms
    Serialized and wrote 8906 results in 2ms
    Completed in 1.3359969s
