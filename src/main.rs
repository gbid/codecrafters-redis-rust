use redis_starter_rust::{ start_redis_server, Config };
use std::net::SocketAddr;
use std::str::FromStr;
use clap::Parser;


fn main() {
    let args = Config::parse();
    dbg!(&args);
    start_redis_server(SocketAddr::from_str("127.0.0.1:6379").expect("hard coded SocketAddr"), args);
}
