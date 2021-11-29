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
use std::collections::HashSet;
use schema::{Table, Column};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Row {
    version: i64,
    pk: i64,
    values: Vec<Value>,
    // contains a vector of other (table_id, row_id) that reference this row
    refs: HashSet<(i32, i64)>
}
impl Row {
    fn new(version: i64, pk: i64, value: Vec<Value>) -> Row {
        return Row { version: version, pk: pk, values: value, refs: HashSet::new() };
    }
}
pub struct TableContent {
    table_map: HashMap<i64,Row>,
    last_index: i64
}
impl TableContent {
    fn new() -> TableContent {
        let new_map: HashMap<i64,Row> = HashMap::new();
        return TableContent { table_map: new_map, last_index: 1i64 }
    }
}
/* You can implement your Database structure here
 * Q: How you will store your tables into the database? */
pub struct Database {
    table_defintions: HashMap<i32,Mutex<Table>>,
    tables: HashMap<i32,Mutex<TableContent>>,
    pub conn_count: Mutex<i32>
 }
 impl Database {
     pub fn new(schemas: Vec<Table>) -> Database {
        let mut table_defs: HashMap<i32,Mutex<Table>> = HashMap::new();
        let mut table_maps: HashMap<i32,Mutex<TableContent>> = HashMap::new();
        for table in schemas {
            let id_copy = table.t_id.clone();
            table_defs.insert(table.t_id,Mutex::new(table));
            let new_table = Mutex::new(TableContent::new());
            table_maps.insert(id_copy,new_table);
        };
        let thread_c = Mutex::new(0i32);
        return Database { table_defintions: table_defs, tables: table_maps, conn_count: thread_c}
     }
 }


/* Receive the request packet from client and send a response back */
pub fn handle_request(request: Request, db: Arc<Database>) 
    -> Response
{           
    /* Handle a valid request */
    let result = match request.command {
        Command::Insert(values) => 
            handle_insert(db.clone(), request.table_id, values),
        Command::Update(id, version, values) => 
             handle_update(db.clone(), request.table_id, id, version, values),
        Command::Drop(id) => handle_drop(db.clone(), request.table_id, id),
        Command::Get(id) => handle_get(db.clone(), request.table_id, id),
        Command::Query(column_id, operator, value) => 
            handle_query(db.clone(), request.table_id, column_id, operator, value),
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
 
fn handle_insert(db: Arc<Database>, table_id: i32, values: Vec<Value>) 
    -> Result<Response, i32> 
{
    // Create row and insert into hashTable?
    let version = 1;
    //Error Checking
    
    //Error Checking on Tables
    if table_id < 1 || table_id > db.tables.len() as i32 {
        return Err(Response::BAD_TABLE);
    }

    let table_definition = db.table_defintions.get(&table_id).unwrap().lock().unwrap();
    //Error Checking for bad number of entries
    if table_definition.t_cols.len() != values.len() {
        return Err(Response::BAD_ROW);
    }
    //Error Checking on row contents (values)
    for index in 0..values.len() {
        let required_type = table_definition.t_cols[index].c_type.clone();
        match &values[index] {
            Value::Null => if required_type != Value::NULL {
                return Err(Response::BAD_VALUE);
            },
            Value::Integer(num) => if required_type != Value::INTEGER {
                return Err(Response::BAD_VALUE);
            },
            Value::Float(num) => if required_type != Value::FLOAT {
                return Err(Response::BAD_VALUE);
            },
            Value::Text(text) => if required_type != Value::STRING {
                return Err(Response::BAD_VALUE);
            },
            Value::Foreign(key) => {
                if required_type != Value::FOREIGN { // look up the foreign
                    return Err(Response::BAD_VALUE);
                }
                else {
                    let lookup_table_id = table_definition.t_cols[index].c_ref;
                    if let Some(table) = db.tables.get(&lookup_table_id) {
                        if let None = table.lock().unwrap().table_map.get(&key) {
                            return Err(Response::BAD_FOREIGN);
                        }
                    } else { 
                        return Err(Response::BAD_FOREIGN);
                    }
                }
            }
        }
    }
    // Getting targetTable and setting up row ID after checks passed
    let mut target_table = db.tables.get(&table_id).unwrap().lock().unwrap(); 

    let row_id: i64 = target_table.last_index;
    target_table.last_index += 1i64;
    {
        let refs: Vec<(usize, Column)> = {
            table_definition.t_cols.iter().cloned()
            .filter(|col| col.c_type == Value::FOREIGN)
            .enumerate().collect()
        };

        refs.iter().for_each(|(id, col)| {
            if let Value::Foreign(foreign_row_id) = values[*id] {
                if let Some(deb) = db.tables.get(&col.c_ref) {
                    if let Some(tb) = deb.lock().unwrap().table_map.get_mut(&foreign_row_id) {
                        tb.refs.insert((table_id, row_id));
                    }
                }
            };
        });
    }

    let row_to_insert = Row::new(version,row_id,values);
    target_table.table_map.insert(row_id,row_to_insert);
    let resp = Ok(Response::Insert(row_id,version));
    return resp
}

fn handle_update(db: Arc<Database>, table_id: i32, object_id: i64, 
    version: i64, values: Vec<Value>) -> Result<Response, i32> 
{
    
    //Error Checking on Tables
    if table_id < 1 || table_id > db.tables.len() as i32 {
        return Err(Response::BAD_TABLE);
    }

    let table_definition = db.table_defintions.get(&table_id).unwrap().lock().unwrap();
    //Error Checking for bad number of entries
    if table_definition.t_cols.len() != values.len() {
        return Err(Response::BAD_ROW);
    }
    //Error Checking on row contents (values)
    for index in 0..values.len() {
        let required_type = table_definition.t_cols[index].c_type.clone();
        match &values[index] {
            Value::Null => if required_type != Value::NULL {
                return Err(Response::BAD_VALUE);
            },
            Value::Integer(num) => if required_type != Value::INTEGER {
                return Err(Response::BAD_VALUE);
            },
            Value::Float(num) => if required_type != Value::FLOAT {
                return Err(Response::BAD_VALUE);
            },
            Value::Text(text) => if required_type != Value::STRING {
                return Err(Response::BAD_VALUE);
            },
            Value::Foreign(key) => if required_type != Value::FOREIGN { // look up the foreign
                return Err(Response::BAD_VALUE);
            }
            else
            {
                let lookup_table_id = table_definition.t_cols[index].c_ref;
                if db.tables.contains_key(&lookup_table_id)
                {
                    let lookup_table = db.tables.get(&lookup_table_id).unwrap().lock().unwrap();
                    let lookup_table_id_64 = lookup_table_id.clone() as i64;
                    if lookup_table.table_map.contains_key(&key) == false
                    {
                        return Err(Response::BAD_FOREIGN);    
                    }
                }
                else
                {
                    return Err(Response::BAD_FOREIGN);
                }
            }
        }
    }
    // Getting the targetTable
    let mut target_table = db.tables.get(&table_id).unwrap().lock().unwrap(); 

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
    let resp = Ok(Response::Update(row_version));
    return resp
}

fn refs_flattener(db: Arc<Database>, cur_table: i32, cur_row: i64)
-> Rc<RefCell<HashSet<(i32, i64)>>>
{
    let cur_value = (cur_table, cur_row);
    let cur_hashset = Rc::new(RefCell::new(HashSet::new()));

    if let Some(tb) = db.tables.get(&cur_table) {
        if let Some(row) = tb.lock().unwrap().table_map.get(&cur_row) {
            if row.refs.is_empty() {
                (*cur_hashset.borrow_mut()).insert(cur_value);
                return cur_hashset.clone();
            };
        } else { return Rc::new(RefCell::new(HashSet::new())); };
    } else { return Rc::new(RefCell::new(HashSet::new())); };
    
    (*cur_hashset.borrow_mut()).extend(db.tables.get(&cur_table).unwrap().lock().unwrap()
        .table_map.get(&cur_row).unwrap()
        .refs.clone()
    );
    
    let iters = cur_hashset.borrow().clone();

    for (new_table, new_row) in iters {
        let new_refs = refs_flattener(db.clone(), new_table, new_row);
        (*cur_hashset.borrow_mut()).extend((*new_refs.borrow()).clone());
    }
    (*cur_hashset.borrow_mut()).insert(cur_value);
    cur_hashset.clone()
}

fn handle_drop(db: Arc<Database>, table_id: i32, object_id: i64) 
    -> Result<Response, i32>
{
    //Error Checking on Tables
    if table_id < 1 || table_id > db.tables.len() as i32 {
        return Err(Response::BAD_TABLE);
    };

    //Error checking for invalid gets
    if db.tables.get(&table_id).unwrap().lock().unwrap().table_map.contains_key(&object_id) == false {
        return Err(Response::NOT_FOUND);
    };

    let ref_tree = refs_flattener(db.clone(), table_id, object_id);

    for (ref_table, ref_row) in (*ref_tree.borrow()).clone() {
        if let Some(tb) = db.tables.get(&ref_table) {
            tb.lock().unwrap().table_map.remove(&ref_row);
        };
    };
    db.tables.get(&table_id).unwrap().lock().unwrap().table_map.remove(&object_id);

    return Ok(Response::Drop);
}

fn handle_get(db: Arc<Database>, table_id: i32, object_id: i64) 
    -> Result<Response, i32>
{
    // Create row and insert into hashTable?
   
    //Error Checking
   
    //Error Checking on Tables
    if table_id < 1 || table_id > db.tables.len() as i32 {
        return Err(Response::BAD_TABLE);
    }

    //Error Checking on row format


    //Error Checking on row contents (values)
    //    for index in 0..values.len() {
    //    }

    // Getting the targetTable
    let target_table = db.tables.get(&table_id).unwrap().lock().unwrap(); 

    //Error checking for invalid gets
    if target_table.table_map.contains_key(&object_id) == false
    {
        return Err(Response::NOT_FOUND)
    }
    //Copying and returning
    let target_row  = target_table.table_map.get(&object_id).unwrap();
    let version = target_row.version;
    let row_values = target_row.values.clone();
    return Ok(Response::Get(version,row_values));
    // return resp
}

fn handle_query(db: Arc<Database>, table_id: i32, column_id: i32,
    operator: i32, other: Value) 
    -> Result<Response, i32>
{
    let column_index = (column_id-1) as usize;
    // check table_id is valid
    if let None = db.tables.get(&table_id) {
        return Err(Response::BAD_TABLE);
    };
    //check column_id valid
    if let None = db.table_defintions.get(&table_id).unwrap().lock().unwrap()
    .t_cols.get(column_index) {
        if column_id != 0 { 
            return Err(Response::BAD_QUERY)
        } 
    };
    // check column-type mismatch
    let col_type = if column_id != 0 
    { db.table_defintions.get(&table_id).unwrap().lock().unwrap()
        .t_cols.get(column_index).unwrap().c_type }
    else { Value::FOREIGN };

    if !(match other {
        Value::Null => operator==OP_AL,
        Value::Integer(_) => col_type == Value::INTEGER,
        Value::Float(_) => col_type == Value::FLOAT,
        Value::Text(_) => col_type == Value::STRING,
        Value::Foreign(_) => col_type == Value::FOREIGN && [OP_EQ, OP_NE].contains(&operator),
    }) { return Err(Response::BAD_QUERY)};

    // burrow rows
    let rows = &db.tables.get(&table_id).unwrap().lock().unwrap().table_map;

    // do the scanning
    let results_ids: Vec<i64> = match operator {
        OP_AL => {
            if column_id != 0 { return Err(Response::BAD_QUERY)};
            rows.keys().cloned().collect()
        }
        OP_EQ => {
            if column_id == 0 { 
                rows.keys().filter(|k| Value::Foreign(*k.clone() as i64)==other).cloned().collect()
            } else {
                rows.iter().filter_map(|(id, row)| {
                    if row.values[column_index] == other { Some(id.clone() as i64)}
                    else { None }
                }).collect()
            }
        }
        OP_NE => {
            if column_id == 0 { 
                rows.keys().filter(|k| Value::Foreign(*k.clone() as i64)!=other).cloned().collect()
            } else {
                rows.iter().filter_map(|(id, row)| {
                    if row.values[column_index] != other { Some(id.clone() as i64)}
                    else { None }
                }).collect()
            }
        }
        OP_LT => {
            rows.iter().filter_map(|(id, row)| {
                if row.values[column_index] < other { Some(id.clone() as i64)}
                else { None }
            }).collect()
        }
        OP_GT => {
            rows.iter().filter_map(|(id, row)| {
                if row.values[column_index] > other { Some(id.clone() as i64)}
                else { None }
            }).collect()
        }
        OP_LE => {
            rows.iter().filter_map(|(id, row)| {
                if row.values[column_index] <= other { Some(id.clone() as i64)}
                else { None }
            }).collect()
        }
        OP_GE => {
            rows.iter().filter_map(|(id, row)| {
                if row.values[column_index] >= other { Some(id.clone() as i64)}
                else { None }
            }).collect()
        }
        _ => return Err(Response::BAD_QUERY)
    };
    Ok(Response::Query(results_ids))
}

