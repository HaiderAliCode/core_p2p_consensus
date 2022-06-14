use bytes::Bytes;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

type DB = Arc<Mutex<HashMap<String, Bytes>>>;

const LOCAL_ADDR: &str = "localhost:6371";

fn rem_first_and_last(value: &str) -> (String, &str) {
    let mut chars = value.chars();
    let first_char = chars.next();
    chars.next_back();
    (first_char.unwrap().to_string(), chars.as_str())
}

#[tokio::main]
async fn main() {
    start_node().await;
    let listener = TcpListener::bind(LOCAL_ADDR).await.unwrap();
    loop {
        let (mut socket, _) = listener.accept().await.unwrap();

        tokio::spawn(async move {
            let db = Arc::new(Mutex::new(HashMap::new()));
            let (reader, _) = socket.split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();

            let cloned_db = db.clone();
            loop {
                tokio::select! {
                    inp = reader.read_line(&mut line) =>{
                        if inp.unwrap() == 0 {
                            break;
                        }

                        let (cmd, msg) = rem_first_and_last(&line);
                        if cmd == "1"{
                            process(msg.to_string(), &cloned_db);
                        }
                        line.clear();

                    }
                }
            }
        });
    }
}

// async fn parse_command(socket: TcpStream) -> String {
//     let mut line = String::new();
//     let mut buffed_stream = BufReader::new(socket);
//     let err = buffed_stream
//         .read_line(&mut line)
//         .await
//         .expect("error occured");
//     println!("written data {}", err);

//     line
// }

fn process<'a>(cmd: String, db: &'a DB) {
    let splitted_command: Vec<String> = cmd.trim().split(' ').map(String::from).collect();
    if (splitted_command.len() == 2) && (splitted_command[0] == "GET") {
        let ref key = &splitted_command[1];
        let db = db.lock().unwrap();
        if let Some(val) = db.get(*key) {
            println!("got val: {:?}", val);
        } else {
            println!("no value found");
        }
    }
    if (splitted_command.len() == 3) && (splitted_command[0] == "SET") {
        let ref key = &splitted_command[1];
        let ref val = &splitted_command[2];
        let mut db = db.lock().unwrap();
        db.insert(key.to_string(), (*val).clone().into());
        let val = db.get(*key);
        println!("reading value from db after writing {:?}", val);
        // replicate_changes(key.to_string(), val.clone().into());
    }
}

// async fn process(socket: TcpStream, db: DB) {
//     use mini_redis::Command::{self, Get, Set};

//     let mut connection = Connection::new(socket);

//     while let Some(frame) = connection.read_frame().await.unwrap() {
//         println!("Got {:?}", frame);
//         let response = match Command::from_frame(frame).unwrap() {
//             Set(cmd) => {
//                 let mut db = db.lock().unwrap();
//                 let key = cmd.key().to_string().clone();
//                 let val = cmd.value().clone();
//                 db.insert(key, val);
//                 replicate_changes(cmd.key().to_string().clone(), cmd.value().clone());
//                 Frame::Simple("OK".to_string())
//             }
//             Get(cmd) => {
//                 let db = db.lock().unwrap();
//                 if let Some(value) = db.get(cmd.key()) {
//                     Frame::Bulk(value.clone())
//                 } else {
//                     Frame::Null
//                 }
//             }
//             _ => panic!("unimplemented"),
//         };
//         // println!("Got {:?}", frame);

//         //Respond with error
//         connection.write_frame(&response).await.unwrap();
//     }
// }

// fn replicate_changes(key: String, val: Bytes) {
//     let servers = ["localhost:6371"];
//     for server in servers {
//         let new_key = key.clone();
//         let new_val = val.clone();
//         tokio::spawn(async move {
//             let mut client = client::connect(server).await.unwrap();
//             let _ = client.set(&new_key, new_val).await;
//         });
//     }
// }

// fn add_server_to_ls(server_ip: &str) {}
async fn start_node() {
    let mut tcp_stream = TcpStream::connect("localhost:6370").await.unwrap();
    tcp_stream.write(b"0localhost:6371").await;
}
