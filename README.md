
# SharkDB

SharkDB is a lightweight relational database prototype designed to help users learn and implement core database functionalities such as lexical analysis, syntax parsing, and query execution. Built with Rust, SharkDB offers an extensible platform for database enthusiasts to explore database internals.

---

## Features

- **SQL Support**: Supports basic SQL commands such as `CREATE TABLE`, `INSERT`, and `SELECT`.
- **Data Types**: Handles common data types, including `INT`, `FLOAT`, `TEXT`, and `BOOLEAN`.
- **Modular Architecture**: Extensible Rust-based architecture for adding new features or experimenting with database concepts.
- **Lightweight Design**: Simplified implementation ideal for educational and learning purposes.

---

## Getting Started

Follow the steps below to set up and run SharkDB on your local machine.

### Prerequisites

- **Rust**: Ensure Rust is installed. Download it from [rust-lang.org](https://www.rust-lang.org/).
- **Git**: Make sure Git is installed on your system.

### Installation Steps

#### Clone the repository
```bash
git clone https://github.com/YZhao-prog/SharkDB.git
cd SharkDB
```

#### Build the project
```bash
cargo build
```

#### Run the project
```bash
cargo run
```

---

## Usage

Here are some example SQL commands supported by SharkDB:

### 1. Create a Table
```sql
CREATE TABLE users (id INT, name TEXT, active BOOLEAN);
```

### 2. Insert Data
```sql
INSERT INTO users VALUES (1, 'Alice', TRUE);
INSERT INTO users VALUES (2, 'Bob', FALSE);
```

### 3. Query Data
```sql
SELECT * FROM users;
```

---

## Project Structure

- **Lexer**: Responsible for tokenizing SQL queries.
- **Parser**: Converts tokens into an abstract syntax tree (AST).
- **Executor**: Executes parsed queries and manages the data.
- **Storage**: Handles data persistence in memory or on disk.

---

## Contributing

Contributions are welcome! If you'd like to contribute:
1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Submit a pull request with a clear explanation of your changes.

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

Special thanks to the Rust community for providing an excellent ecosystem and to all contributors who make this project possible.
