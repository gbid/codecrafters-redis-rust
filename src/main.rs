use redis_starter_rust::start_redis_server;
use std::net::SocketAddr;
use std::str::FromStr;

fn main() {
    start_redis_server(SocketAddr::from_str("127.0.0.1:6379").expect("hard coded SocketAddr"));
}
