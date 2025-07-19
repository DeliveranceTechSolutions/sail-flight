# Sail Flight SQL Test Application

This Java application tests the Sail Arrow Flight SQL Server by connecting via JDBC and running various SQL queries.

## Prerequisites

- Java 11 or higher
- Maven 3.6 or higher
- Sail Flight SQL Server running on `localhost:32010`

## Quick Start

1. **Start the Sail Flight SQL Server** (in another terminal):
   ```bash
   cd sail/crates/sail-flight-sql-server
   cargo run -- flight server --verbose
   ```

2. **Run the Java test application**:
   ```bash
   ./run-test.sh
   ```

## What This Test Does

The application performs the following tests:

### 1. Connection Test
- Loads the Arrow Flight SQL JDBC driver
- Establishes connection to the server
- Retrieves database metadata

### 2. Basic SQL Queries
- Simple SELECT with literals
- SELECT with calculations (arithmetic)
- SELECT with functions (CURRENT_TIMESTAMP)

### 3. Demo Table Queries
- SELECT all rows from the demo table
- SELECT with WHERE clause filtering
- SELECT with aggregation functions (COUNT, AVG, MAX)
- SELECT with ORDER BY

### 4. Complex SQL Queries
- SELECT with CASE statements
- SELECT with GROUP BY and aggregation
- SELECT with subqueries

### 5. Metadata Queries
- List all tables
- Get column information
- Get primary key information

## Expected Results

The server should:
1. Accept JDBC connections via Arrow Flight SQL protocol
2. Process SQL queries and return results
3. Provide a demo table with sample data (id, name, value columns)
4. Support various SQL operations and functions

## Troubleshooting

### Connection Issues
- Ensure the Sail Flight SQL Server is running on port 32010
- Check that the server started without errors
- Verify network connectivity to localhost:32010

### Build Issues
- Ensure Java 11+ is installed: `java -version`
- Ensure Maven is installed: `mvn -version`
- Check that all dependencies can be downloaded

### Runtime Issues
- Check the logs for detailed error messages
- Verify that the Arrow Flight SQL JDBC driver is compatible
- Ensure the server supports the SQL operations being tested

## Architecture

```
Java Application (JDBC Client)
    ↓ (Arrow Flight SQL over gRPC)
Sail Flight SQL Server (Rust)
    ↓ (SQL Processing)
DataFusion Engine
    ↓ (Demo Data)
Arrow Record Batches
```

## Files

- `src/main/java/com/sail/flight/test/SailFlightSqlTest.java` - Main test application
- `pom.xml` - Maven project configuration
- `src/main/resources/logback.xml` - Logging configuration
- `run-test.sh` - Test execution script
- `README.md` - This documentation

## Dependencies

- **Arrow Flight SQL JDBC Driver** - For JDBC connectivity
- **Arrow Flight Core** - Core Arrow Flight functionality
- **SLF4J + Logback** - Logging framework
- **Jackson** - JSON processing (for future use)
- **JUnit** - Unit testing framework 