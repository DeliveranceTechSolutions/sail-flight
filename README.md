# Sail Arrow Flight SQL Server

A Rust-based Arrow Flight SQL server that accepts JDBC connections.

## 🚀 Quick Start

### 1. Build and Run the Server

```bash
# Build the project
cargo build

# Run the server
cargo run
```

The server will start on `localhost:32010` and display:
```
🚀 Starting Sail Arrow Flight SQL Server...
📍 Server listening on [::1]:32010
🔗 JDBC clients can connect to: jdbc:arrow-flight-sql://localhost:32010
```

### 2. Test with JDBC Client

#### Option A: Using Arrow Flight SQL JDBC Driver

1. Download the Arrow Flight SQL JDBC driver:
   ```bash
   # You can find it in Maven Central or build from source
   wget https://repo1.maven.org/maven2/org/apache/arrow/flight-sql-jdbc/[VERSION]/flight-sql-jdbc-[VERSION].jar
   ```

2. Compile and run the test:
   ```bash
   cd test
   javac -cp "flight-sql-jdbc-*.jar" TestConnection.java
   java -cp ".:flight-sql-jdbc-*.jar" TestConnection
   ```

#### Option B: Using DBeaver or other JDBC tools

1. Download DBeaver Community Edition
2. Create a new connection with:
   - **Driver**: Arrow Flight SQL
   - **URL**: `jdbc:arrow-flight-sql://localhost:32010`
   - **Port**: `32010`

## 🏗️ Architecture

- **Server**: Rust + Tonic (gRPC) + Arrow Flight
- **Protocol**: Arrow Flight SQL over gRPC
- **Client**: Any JDBC-compatible tool

## 📋 Features

- ✅ Arrow Flight SQL protocol support
- ✅ JDBC client compatibility
- ✅ SQL query processing
- ✅ Prepared statement support (stub)
- ✅ Demo data responses

## 🔧 Development

### Project Structure
```
src/
├── main.rs          # Server entry point
├── connect.rs       # Flight SQL service implementation
└── generated/       # Generated protobuf code
    ├── mod.rs
    ├── arrow.flight.protocol.rs
    └── arrow.flight.protocol.sql.rs
```

### Adding Your Actor System

To integrate your actor/message system, modify the `get_flight_info` method in `src/connect.rs`:

```rust
async fn get_flight_info(&self, request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
    let descriptor = request.into_inner();
    
    if let Ok(cmd) = CommandStatementQuery::decode(descriptor.cmd.as_ref()) {
        // TODO: Send SQL to your actor system
        // let result = self.actor_system.send(SqlQuery(cmd.query)).await?;
        
        // Return FlightInfo with result
        // ...
    }
}
```

## 🐛 Troubleshooting

### Common Issues

1. **Port already in use**: Change the port in `main.rs`
2. **JDBC driver not found**: Download the correct Arrow Flight SQL JDBC driver
3. **Connection refused**: Make sure the server is running

### Logs

The server provides detailed logging:
- 📝 SQL queries received
- 🎫 Ticket processing
- ⚡ Actions received

## 📚 Resources

- [Arrow Flight SQL Documentation](https://arrow.apache.org/docs/format/FlightSql.html)
- [Arrow Flight SQL JDBC Driver](https://github.com/apache/arrow/tree/main/java/flight/flight-sql-jdbc)
- [Tonic (gRPC for Rust)](https://github.com/hyperium/tonic) 