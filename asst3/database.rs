/*
 * database.rs
 *
 * Implementation of EasyDB database internals
 *
 * University of Toronto
 * 2019
 */

use packet::{Command, Request, Response, Value};
 
 
/* OP codes for the query command */
pub const OP_AL: i32 = 1;
pub const OP_EQ: i32 = 2;
pub const OP_NE: i32 = 3;
pub const OP_LT: i32 = 4;
pub const OP_GT: i32 = 5;
pub const OP_LE: i32 = 6;
pub const OP_GE: i32 = 7;

use std::collections::HashMap;
use schema::Table;



pub struct Row {
    version: i64,
    pk: i64,
    values: Vec<Value>,
    refers: Value
}
impl Row {
    fn new(version: i64, pk: i64, value: Vec<Value>) -> Row {
        return Row { version: version, pk: pk, values: value, refers: Value::Foreign(pk)};
    }
}
pub struct TableContent {
    table_map: HashMap<i64,Row>
}
impl TableContent {
    fn new() -> TableContent {
        let new_map: HashMap<i64,Row> = HashMap::new();
        return TableContent { table_map: new_map }
    }
}
/* You can implement your Database structure here
 * Q: How you will store your tables into the database? */
pub struct Database {
    table_defintions: HashMap<i32,Table>,
    tables: HashMap<i32,TableContent>
 }
 impl Database {
     pub fn new(schemas: Vec<Table>) -> Database {
        let mut table_defs: HashMap<i32,Table> = HashMap::new();
        let mut table_maps: HashMap<i32,TableContent> = HashMap::new();
            for table in schemas {
                let id_copy = table.t_id.clone();
               table_defs.insert(table.t_id,table);
               let new_table = TableContent::new();
               table_maps.insert(id_copy,new_table);
            }
            return Database { table_defintions: table_defs, tables: table_maps}
     }
 }


/* Receive the request packet from client and send a response back */
pub fn handle_request(request: Request, db: & mut Database) 
    -> Response  
{           
    /* Handle a valid request */
    let result = match request.command {
        Command::Insert(values) => 
            handle_insert(db, request.table_id, values),
        Command::Update(id, version, values) => 
             handle_update(db, request.table_id, id, version, values),
        Command::Drop(id) => handle_drop(db, request.table_id, id),
        Command::Get(id) => handle_get(db, request.table_id, id),
        Command::Query(column_id, operator, value) => 
            handle_query(db, request.table_id, column_id, operator, value),
        /* should never get here */
        Command::Exit => Err(Response::UNIMPLEMENTED),
    };
    
    /* Send back a response */
    match result {
        Ok(response) => response,
        Err(code) => Response::Error(code),
    }
}

/*
 * TODO: Implment these EasyDB functions
 */
 
fn handle_insert(db: & mut Database, table_id: i32, values: Vec<Value>) 
    -> Result<Response, i32> 
{
    // Create row and insert into hashTable?
    println!("entered insert onto tableID = {}", table_id);
    let version = 1;
    //Error Checking
    println!("tables length = {} ",db.tables.len());
    
    //Error Checking on Tables
    if table_id < 1 || table_id > db.tables.len() as i32 {
        println!("found a bad table");
        return Err(Response::BAD_TABLE);
    }

    let table_definition = db.table_defintions.get(&table_id).unwrap();
    //Error Checking for bad number of entries
    if table_definition.t_cols.len() != values.len() {
        return Err(Response::BAD_ROW);
    }
    //Error Checking on row contents (values)
    for index in 0..values.len() {
        let required_type = table_definition.t_cols[index].c_type.clone();
        println!("required type be like {}",required_type);
        //println!("actual type be like {}",&values[index]);
        match &values[index] {
            Value::Null => if required_type != Value::NULL {
                println!("found an invalid Null");
                return Err(Response::BAD_VALUE);
            },
            Value::Integer(num) => if required_type != Value::INTEGER {
                println!("found an invalid Integer");
                return Err(Response::BAD_VALUE);
            },
            Value::Float(num) => if required_type != Value::FLOAT {
                println!("found an invalid Float");
                return Err(Response::BAD_VALUE);
            },
            Value::Text(text) => if required_type != Value::STRING {
                println!("found an invalid Text");
                return Err(Response::BAD_VALUE);
            },
            Value::Foreign(key) => if required_type != Value::FOREIGN { // look up the foreign
                println!("Found an invalid Foreign");
                return Err(Response::BAD_VALUE);
            }
            else
            {
                //Immigration
                println!("Entered Immigration");
                /*
                 * This is a lot like when I cross the border and 
                 * the US customs officer asking me what the address of the hotel im staying in 
                 * even though no one in the 21st century knows any addresses other than their
                 * own or their parents house. Like dude I'm just trying to go to get cheap liquor.
                 * And even then, as if the guy even knows where I'm going, the dude is just gonna
                 * google maps it anyway just like me except he's legally allowed to be a pain the ass.
                 */
                let lookup_table_id = table_definition.t_cols[index].c_ref;

                if db.tables.contains_key(&lookup_table_id)
                {
                    let lookup_table = db.tables.get(&lookup_table_id).unwrap();
                    let lookup_table_id_64 = lookup_table_id.clone() as i64;
                    if lookup_table.table_map.contains_key(&key) == false
                    {
                        println!("Got the impostor");
                        return Err(Response::BAD_FOREIGN);
                    }
                }
                else
                {
                    println!("Got the impostor table");
                    return Err(Response::BAD_FOREIGN);
                }
            }

        }
    }
    // Getting targetTable and setting up row ID after checks passed
    let target_table = db.tables.get_mut(&table_id).unwrap(); 
    let row_id = target_table.table_map.len() as i64 + 1;

    let row_to_insert = Row::new(version,row_id,values);
    target_table.table_map.insert(row_id,row_to_insert);
    println!("inserted rowID  = {} into table {}",row_id,table_id);
    let resp = Ok(Response::Insert(row_id,version));
    println!("exiting insert");
    return resp
}

fn handle_update(db: & mut Database, table_id: i32, object_id: i64, 
    version: i64, values: Vec<Value>) -> Result<Response, i32> 
{
    println!("entered update");
    //Error Checking on Tables
    if table_id < 1 || table_id > db.tables.len() as i32 {
        println!("found a bad table");
        return Err(Response::BAD_TABLE);
    }

    let table_definition = db.table_defintions.get(&table_id).unwrap();
    //Error Checking for bad number of entries
    if table_definition.t_cols.len() != values.len() {
        return Err(Response::BAD_ROW);
    }
    //Error Checking on row contents (values)
    for index in 0..values.len() {
        let required_type = table_definition.t_cols[index].c_type.clone();
        println!("required type be like {}",required_type);
        //println!("actual type be like {}",&values[index]);
        match &values[index] {
            Value::Null => if required_type != Value::NULL {
                println!("found an invalid Null");
                return Err(Response::BAD_VALUE);
            },
            Value::Integer(num) => if required_type != Value::INTEGER {
                println!("found an invalid Integer");
                return Err(Response::BAD_VALUE);
            },
            Value::Float(num) => if required_type != Value::FLOAT {
                println!("found an invalid Float");
                return Err(Response::BAD_VALUE);
            },
            Value::Text(text) => if required_type != Value::STRING {
                println!("found an invalid Text");
                return Err(Response::BAD_VALUE);
            },
            Value::Foreign(key) => if required_type != Value::FOREIGN { // look up the foreign
                println!("Found an invalid Foreign");
                return Err(Response::BAD_VALUE);
            }
            else
            {
                let lookup_table_id = table_definition.t_cols[index].c_ref;
                if db.tables.contains_key(&lookup_table_id)
                {
                    let lookup_table = db.tables.get(&lookup_table_id).unwrap();
                    let lookup_table_id_64 = lookup_table_id.clone() as i64;
                    if lookup_table.table_map.contains_key(&key) == false
                    {
                        println!("Got the impostor");
                        return Err(Response::BAD_FOREIGN);    
                    }
                }
                else
                {
                    println!("Got the impostor table");
                    return Err(Response::BAD_FOREIGN);
                }
            }
        }
    }
    // Getting the targetTable
    let target_table = db.tables.get_mut(&table_id).unwrap(); 

    //Error checking for invalid gets
    if target_table.table_map.contains_key(&object_id) == false
    {
        return Err(Response::NOT_FOUND)
    }

    //Copying and returning
    let target_row  = target_table.table_map.get(&object_id).unwrap();
    let mut row_version = target_row.version;
    if version != row_version && version != 0
    {
        return Err(Response::TXN_ABORT);    
    }
    row_version = row_version + 1;
    let row_to_insert = Row::new(row_version,object_id,values);
    target_table.table_map.insert(object_id,row_to_insert);
    //println!("inserted rowID  = {} into table {}",object_id,table_id);
    let resp = Ok(Response::Update(row_version));
    //println!("exiting update");
    return resp
}

fn handle_drop(db: & mut Database, table_id: i32, object_id: i64) 
    -> Result<Response, i32>
{
    println!("entered insert onto tableID = {}", table_id);
    let version = 1;
    //Error Checking
    println!("tables length = {} ",db.tables.len());
    
    //Error Checking on Tables
    if table_id < 1 || table_id > db.tables.len() as i32 {
        println!("found a bad table");
        return Err(Response::BAD_TABLE);
    }

    // Getting the targetTable
    let target_table = db.tables.get(&table_id).unwrap(); 

    //Error checking for invalid gets
    if target_table.table_map.contains_key(&object_id) == false
    {
        return Err(Response::NOT_FOUND)
    }



    let resp = Ok(Response::Drop);
    return resp
}

fn handle_get(db: & Database, table_id: i32, object_id: i64) 
    -> Result<Response, i32>
{
    // Create row and insert into hashTable?
    println!("entered get onto tableID = {}", table_id);
   
    //Error Checking
    println!("tables length = {} ",db.tables.len());
   
    //Error Checking on Tables
    if table_id < 1 || table_id > db.tables.len() as i32 {
        println!("found a bad table");
        return Err(Response::BAD_TABLE);
    }

    //Error Checking on row format


    //Error Checking on row contents (values)
    //    for index in 0..values.len() {
    //    }

    // Getting the targetTable
    let target_table = db.tables.get(&table_id).unwrap(); 

    //Error checking for invalid gets
    if target_table.table_map.contains_key(&object_id) == false
    {
        return Err(Response::NOT_FOUND)
    }
    //Copying and returning
    let target_row  = target_table.table_map.get(&object_id).unwrap();
    let version = target_row.version;
    let row_values = &target_row.values;
    let resp = Ok(Response::Get(version,&row_values));
    return resp
}

fn handle_query(db: & Database, table_id: i32, column_id: i32,
    operator: i32, other: Value) 
    -> Result<Response, i32>
{
    Err(Response::UNIMPLEMENTED)
}

