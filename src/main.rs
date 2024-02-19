use redis_starter_rust::{ start_redis_server, Config };
use std::net::SocketAddr;
use std::str::FromStr;
use std::path::PathBuf;
use std::env;
// use clap::Parser;


fn main() {
    let args: Vec<_> = env::args().collect();
    let config = if args.len() > 4 {
        match (args[1].as_str(), args[3].as_str()) {
            ("--dir", "--dbfilename") => Config {
                dir: PathBuf::from(&args[2]),
                dbfilename: PathBuf::from(&args[4]),
            },
            ("--dbfilename", "--dir") => Config {
                dir: PathBuf::from(&args[4]),
                dbfilename: PathBuf::from(&args[2]),
            },
            _ => panic!("Unkown arguments"),
        }
    } else {
        Config {
            dir: PathBuf::from("."),
            dbfilename: PathBuf::from("default.rdb"),
        }
    };
    dbg!(&config);
    start_redis_server(SocketAddr::from_str("127.0.0.1:6379").expect("hard coded SocketAddr"), config);
}
