use std::env;
use std::thread;

use std::io::prelude::*;
use std::io::SeekFrom;

use std::fs;
use std::fs::File;
use std::fs::OpenOptions;

use std::time::Instant;

use rand::Rng;

const TEST_IO_SIZE: usize = 4 * 1024;           // test unit is 4KB
const FILE_SIZE: usize = 1024 * 1024 * 1024;    // test file is 1GB
const BUF_SIZE: usize = 128 * 1024;             // write unit is 128KB for test file creation
const IO_DEPTH: usize = 32;                     // IO depth is 32 (for sata)

// Test file should be created before executing benchmark test
// Assume we can't avoid filesystem overhead.
fn create_test_file(filename: String) {
    let mut file = match File::create(&filename) {
        Err(why) => panic!("Can't create {}. Error = {}", filename, why),
        Ok(file) => file,
    };

    // NULL data is enough for speed benchmark
    let buf: [u8; BUF_SIZE] = [0; BUF_SIZE];

    let mut remain_size = FILE_SIZE;

    while remain_size > 0 {
        file.write(&buf).expect("Can't write to test file");

        remain_size -= BUF_SIZE;
    }
}

fn remove_test_file(filename: String) {
    match fs::remove_file(&filename) {
        Err(why) => panic!("Can't remove file {}. Error = {}", filename, why),
        Ok(_) => (),
    }
}

// Execute write test for a given file : Random position, fixed size, total_write==file len
// The file will be shared by different threads but OS will handle sync. issues
fn write_test(filename: String, total_write: usize) {
    let mut file = match OpenOptions::new().write(true).open(&filename) {
        Err(why) => panic!("Can't open {}. Error = {}", filename, why),
        Ok(file) => file,
    };
    
    let mut io_size = total_write;

    // Write will be done for whole range of the file
    let file_start_pos = 0;
    let file_end_pos = FILE_SIZE as u64 - TEST_IO_SIZE as u64 - 1;

    let buf: [u8; TEST_IO_SIZE] = [0xFF; TEST_IO_SIZE];

    while io_size > 0 {
        let rand_pos = rand::thread_rng().gen_range(file_start_pos, file_end_pos);  // random pos.

        file.seek(SeekFrom::Start(rand_pos)).expect("Failed to seek random position for write test");
        file.write(&buf).expect("Failed to write for write test");

        io_size -= TEST_IO_SIZE;
    }
}

// Execute read test for a given file : Random position, fixed size, total_read==file len
// The file will be shared by different threads but OS will handle sync. issues
fn read_test(filename: String, total_read: usize) {
    let mut file = match File::open(&filename) {
        Err(why) => panic!("Can't open {}. Error = {}", filename, why),
        Ok(file) => file,
    };

    let mut io_size = total_read;

    // read will be done for whole range of the file
    let file_start_pos = 0;
    let file_end_pos = FILE_SIZE as u64 - TEST_IO_SIZE as u64 - 1;

    let mut buf: [u8; TEST_IO_SIZE] = [0; TEST_IO_SIZE];

    while io_size > 0 {
        let rand_pos = rand::thread_rng().gen_range(file_start_pos, file_end_pos);  // random pos

        file.seek(SeekFrom::Start(rand_pos)).expect("Failed to seek random position for write test");
        file.read(&mut buf).expect("Failed to read for read test");

        io_size -= TEST_IO_SIZE;
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();

    assert_eq!(args.len(), 2);

    // filename will be used for test threads, if it's a stack variable like &str
    // rust sees it as an error. So a String is allocated for filename
    let mut filename: String = args[1].to_string();

    // Benchmark test is done through a test file
    create_test_file(filename);
    
    filename = args[1].to_string(); // avoid build error by updating it.

    let _ = filename;   // ignore warning for filename is never read

    // Start Random Write test
    println!("4K Random Write Test for QD32 started");

    let start_time = Instant::now();

    let mut write_handles = vec![]; // keep handles to wait for the end of all threads.

    for _i in 1..IO_DEPTH {
        filename = args[1].to_string();
        write_handles.push(thread::spawn(move || write_test(filename, FILE_SIZE / IO_DEPTH)));
    }

    // wait until all threads are done its execution
    for hndl in write_handles {
        let _ = hndl.join().unwrap();
    }

    println!("Time Elapsed is : {:?}", start_time.elapsed());
    println!("");

    // Start Random Read test
    println!("4K Random Read Test for QD32 started");

    let start_time = Instant::now();

    let mut read_handles = vec![];

    for _i in 1..IO_DEPTH {
        filename = args[1].to_string();
        read_handles.push(thread::spawn(move || read_test(filename, FILE_SIZE / IO_DEPTH)));
    }

    // wait until all threads are done its execution
    for hndl in read_handles {
        let _ = hndl.join().unwrap();
    }

    println!("Time Elapsed is : {:?}", start_time.elapsed());

    filename = args[1].to_string();
    remove_test_file(filename)
}
