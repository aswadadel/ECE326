/*
 * server.rs
 *
 * Implementation of EasyDB database server
 *
 * University of Toronto
 * 2019
 */

use std::net::TcpListener;
use std::net::TcpStream;
use std::io::Write;
use std::io;
use packet::Command;
use packet::Response;
use packet::Network;
use schema::Table;
use database;
use database::Database;
use std::thread;
use std::sync::{Arc, Mutex};

// fn single_threaded(listener: TcpListener, table_schema: Vec<Table>, verbose: bool)
// {
//     /* 
//      * you probably need to use table_schema somewhere here or in
//      * Database::new 
//      */
//     let mut db = Database::new(table_schema);

//     for stream in listener.incoming() {
//         let stream = stream.unwrap();
        
//         if verbose {
//             println!("Connected to {}", stream.peer_addr().unwrap());
//         }
        
//         match handle_connection(stream, &mut db) {
//             Ok(()) => {
//                 if verbose {
//                     println!("Disconnected.");
//                 }
//             },
//             Err(e) => eprintln!("Connection error: {:?}", e),
//         };
//     }
// }

fn multi_threaded(listener: TcpListener, table_schema: Vec<Table>, verbose: bool)
{
    // TODO: implement me
    let db = Arc::new(Mutex::new(Database::new(table_schema)));

    for stream in listener.incoming() {
        let stream = stream.unwrap();

        if verbose {
            println!("Connected to {}", stream.peer_addr().unwrap());
        }

        let db_clone = db.clone();

        thread::spawn(move ||{
            match handle_connection(stream, db_clone) {
                Ok(()) => {
                    if verbose {
                        println!("Disconnected.");
                    }
                },
                Err(e) => eprintln!("Connection error: {:?}", e),
            };
        });
    }
}

/* Sets up the TCP connection between the database client and server */
pub fn run_server(table_schema: Vec<Table>, ip_address: String, verbose: bool)
{
    let listener = match TcpListener::bind(ip_address) {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("Could not start server: {}", e);
            return;
        },
    };
    
    println!("Listening: {:?}", listener);
    
    /*
     * TODO: replace with multi_threaded
     */
    multi_threaded(listener, table_schema, verbose);
}

impl Network for TcpStream {}

/* Receive the request packet from ORM and send a response back */
fn handle_connection(mut stream: TcpStream, db: Arc<Mutex<Database>>) 
    -> io::Result<()> 
{
    /* 
     * Tells the client that the connction to server is successful.
     * TODO: respond with SERVER_BUSY when attempting to accept more than
     *       4 simultaneous clients.
     */
    {
        let mut db_guard = db.lock().unwrap();
        // let count = &mut guard.conn_count;
        // println!("{}", count);
        if db_guard.conn_count >= 4 { 
            stream.respond(&Response::Error(Response::SERVER_BUSY))?;
            return Ok(())
        };
        db_guard.conn_count += 1;
    }

    stream.respond(&Response::Connected)?;

    loop {
        let request = match stream.receive() {
            Ok(request) => request,
            Err(e) => {
                /* respond error */
                stream.respond(&Response::Error(Response::BAD_REQUEST))?;
                return Err(e);
            },
        };
        
        /* we disconnect with client upon receiving Exit */
        if let Command::Exit = request.command {
            let mut guard = db.lock().unwrap();
            guard.conn_count -= 1;
            break;
        }
        
        /* Send back a response */
        let mut guard = db.lock().unwrap();
        let response = database::handle_request(request, &mut guard);
        stream.respond(&response)?;
        drop(guard);

        stream.flush()?;
    }

    Ok(())
}

