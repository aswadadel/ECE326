# ECE326 Labs
### Assignment 1: EasyDB Database Driver
A Python database client which verifies a user-defined schema, performs CRUD opertions, and abstracts encoding/decoding. <br>
Entry file: [\_\_init\_\_.py](https://github.com/aswadadel/ECE326/blob/master/asst1/easydb/__init__.py)

### Assignment 2: Object-Relational Mapping (ORM)
An ORM framework which provides even more abstraction than the previous assignment. 
Allows the user to map database tables to Python classes using metaclasses and descriptors. <br>
Entry file: [\_\_init\_\_.py](https://github.com/aswadadel/ECE326/blob/master/asst2/orm/__init__.py)

### Assignment 3: In-Memory Database
An EasyDB server written in Rust (used in previous assignments) which is concurrent and memory-safe. 
The key to to use a Mutex on every table but an Atomic Reference Counter (ARC) on the database object. <br>
Entry file: [main.rs](https://github.com/aswadadel/ECE326/blob/master/asst3/main.rs)

### Assignment 4: Remote Procedure Call (PRC)
A type-safe RPC framework in C++. The framework can only be type-safe via template metaprogramming and dynamic dispatch. <br>
File: [rpcxx.h](https://github.com/aswadadel/ECE326/blob/master/asst4/rpcxx.h)
