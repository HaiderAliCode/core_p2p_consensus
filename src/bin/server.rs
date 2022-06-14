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

const LOCAL_ADDR: &str = "localhost:6370";

fn rem_first_and_last(value: &str) -> (String, &str) {
    let mut chars = value.chars();
    let first_char = chars.next();
    // chars.next_back();
    (first_char.unwrap().to_string(), chars.as_str())
}

type ServerLsType = Arc<Mutex<Vec<String>>>;
#[tokio::main]
async fn main() {
    let listener = TcpListener::bind(LOCAL_ADDR).await.unwrap();

    // let db = Arc::new(Mutex::new(HashMap::new()));
    loop {
        let (mut socket, _) = listener.accept().await.unwrap();

        tokio::spawn(async move {
            let db = Arc::new(Mutex::new(HashMap::new()));
            let server_ls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

            let (reader, mut s_writer) = socket.split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();
            let cloned_db = db.clone();
            // let new_server_ls = server_ls.clone();
            loop {
                tokio::select! {
                    inp = reader.read_line(&mut line) =>{
                        let new_server_ls = server_ls.clone();
                        if inp.unwrap() == 0 {
                            break;
                        }

                        let (cmd, msg) = rem_first_and_last(&line);
                        if cmd == "0" {
                            let mut server_l = new_server_ls.lock().unwrap();
                            server_l.push(msg.to_string());
                            println!("added item in server list {:?}", server_l);

                        }else if cmd == "1"{
                            let mut returned_val = process(msg.to_string(), &cloned_db, new_server_ls);
                            returned_val.push_str("\n");
                            //sending client data here
                            let val = s_writer.write_all(returned_val.as_bytes()).await.unwrap();

                            println!("returned val {:?} {:?}", val, returned_val.as_bytes());
                        }
                        line.clear();

                    }
                }
            }
        });
    }
}

fn process<'a>(cmd: String, db: &'a DB, new_server_ls: ServerLsType) -> String {
    let splitted_command: Vec<String> = cmd.trim().split(' ').map(String::from).collect();
    let mut got_val: String = String::new();
    if (splitted_command.len() == 2) && (splitted_command[0] == "GET") {
        let ref key = &splitted_command[1];
        let db = db.lock().unwrap();
        if let Some(val) = db.get(*key) {
            got_val = std::str::from_utf8(val.as_ref()).unwrap().to_string();
        } else {
            got_val = String::from("no value found");
        }
    }
    if (splitted_command.len() == 3) && (splitted_command[0] == "SET") {
        let ref key = &splitted_command[1];
        let ref val = &splitted_command[2];
        let mut db = db.lock().unwrap();
        db.insert(key.to_string(), (*val).clone().into());
        let val = db.get(*key);
        got_val = std::str::from_utf8(val.unwrap().as_ref())
            .unwrap()
            .to_string();

        replicate_changes(*key, val.unwrap(), new_server_ls);
    }
    got_val
}

async fn replicate_changes<'a>(key: &String, val: &Bytes, new_server_ls: ServerLsType) {
    let server_ls_local = new_server_ls.lock().unwrap();
    for server in server_ls_local.iter() {
        let new_key = key.clone();
        let new_val = val.clone();
        let server_clone = server.clone();
        tokio::spawn(async move {
            let mut client = TcpStream::connect(server_clone).await.unwrap();
            let compiling_msg = format!(
                "1PUT {} {}",
                new_key,
                std::str::from_utf8(&new_val).unwrap()
            );

            client.write_all(compiling_msg.as_bytes()).await;
        });
    }
}

// fn add_server_to_ls(server_ip: &str) {}
async fn start_node() {
    let mut tcp_stream = TcpStream::connect("localhost:6370").await.unwrap();
    tcp_stream.write(b"0localhost:6371").await;
}
