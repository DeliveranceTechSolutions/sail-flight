package com.sail.flight.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

/**
 * Test application for Sail Arrow Flight SQL Server
 * 
 * This application demonstrates:
 * 1. Connecting to the Sail Flight SQL server via JDBC
 * 2. Running various SQL queries
 * 3. Verifying that queries are properly handled by the server
 * 4. Testing different data types and operations
 */
public class SailFlightSqlTest {
    
    private static final Logger logger = LoggerFactory.getLogger(SailFlightSqlTest.class);
    
    // Connection configuration
    private static final String JDBC_URL = "jdbc:arrow-flight-sql://localhost:32010";
    private static final String DRIVER_CLASS = "org.apache.arrow.flight.sql.jdbc.FlightSqlDriver";
    
    public static void main(String[] args) {
        logger.info("Starting Sail Flight SQL Test Application");
        
        try {
            testConnection();
            testBasicQueries();
            testDemoTableQueries();
            testComplexQueries();
            testMetadataQueries();
            
            logger.info("All tests completed successfully!");
            
        } catch (Exception e) {
            logger.error("Test failed with error: {}", e.getMessage(), e);
            System.exit(1);
        }
    }
    
    /**
     * Test basic connection to the server
     */
    private static void testConnection() throws Exception {
        logger.info("Testing connection to Sail Flight SQL Server...");
        
        Class.forName(DRIVER_CLASS);
        logger.info("Arrow Flight SQL JDBC driver loaded");
        
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            logger.info("Successfully connected to: {}", JDBC_URL);
            
            DatabaseMetaData metaData = conn.getMetaData();
            logger.info("Database: {} {}", metaData.getDatabaseProductName(), metaData.getDatabaseProductVersion());
            logger.info("Driver: {} {}", metaData.getDriverName(), metaData.getDriverVersion());
            
        } catch (SQLException e) {
            logger.error("Connection failed: {}", e.getMessage());
            throw e;
        }
    }
    
    /**
     * Test basic SQL queries
     */
    private static void testBasicQueries() throws Exception {
        logger.info("Testing basic SQL queries...");
        
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             Statement stmt = conn.createStatement()) {
            
            logger.info("Test 1: SELECT literal");
            try (ResultSet rs = stmt.executeQuery("SELECT 'Hello from Sail!' as message")) {
                while (rs.next()) {
                    String message = rs.getString("message");
                    logger.info("   Result: {}", message);
                }
            }
            
            logger.info("Test 2: SELECT with calculations");
            try (ResultSet rs = stmt.executeQuery("SELECT 1 + 2 as sum, 3 * 4 as product")) {
                while (rs.next()) {
                    int sum = rs.getInt("sum");
                    int product = rs.getInt("product");
                    logger.info("   Result: sum={}, product={}", sum, product);
                }
            }
            
            logger.info("Test 3: SELECT with functions");
            try (ResultSet rs = stmt.executeQuery("SELECT CURRENT_TIMESTAMP as current_time")) {
                while (rs.next()) {
                    Timestamp currentTime = rs.getTimestamp("current_time");
                    logger.info("   Result: current_time={}", currentTime);
                }
            }
            
        } catch (SQLException e) {
            logger.error("Basic queries failed: {}", e.getMessage());
            throw e;
        }
    }
    
    /**
     * Test queries against the demo table
     */
    private static void testDemoTableQueries() throws Exception {
        logger.info("🔍 Testing demo table queries...");
        
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             Statement stmt = conn.createStatement()) {
            
            logger.info("Test 1: SELECT * FROM demo_table");
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM demo_table")) {
                int rowCount = 0;
                while (rs.next()) {
                    rowCount++;
                    int id = rs.getInt("id");
                    String name = rs.getString("name");
                    double value = rs.getDouble("value");
                    logger.info("   Row {}: id={}, name={}, value={}", rowCount, id, name, value);
                }
                logger.info("   Total rows: {}", rowCount);
            }
            
            logger.info("Test 2: SELECT with WHERE clause");
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM demo_table WHERE value > 15.0")) {
                int rowCount = 0;
                while (rs.next()) {
                    rowCount++;
                    int id = rs.getInt("id");
                    String name = rs.getString("name");
                    double value = rs.getDouble("value");
                    logger.info("   Row {}: id={}, name={}, value={}", rowCount, id, name, value);
                }
                logger.info("   Rows with value > 15.0: {}", rowCount);
            }
            
            logger.info("Test 3: SELECT with aggregation");
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count, AVG(value) as avg_value, MAX(value) as max_value FROM demo_table")) {
                while (rs.next()) {
                    int count = rs.getInt("count");
                    double avgValue = rs.getDouble("avg_value");
                    double maxValue = rs.getDouble("max_value");
                    logger.info("   Result: count={}, avg_value={}, max_value={}", count, avgValue, maxValue);
                }
            }
            
            logger.info("Test 4: SELECT with ORDER BY");
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM demo_table ORDER BY value DESC")) {
                int rowCount = 0;
                while (rs.next()) {
                    rowCount++;
                    int id = rs.getInt("id");
                    String name = rs.getString("name");
                    double value = rs.getDouble("value");
                    logger.info("   Row {}: id={}, name={}, value={}", rowCount, id, name, value);
                }
            }
            
        } catch (SQLException e) {
            logger.error("Demo table queries failed: {}", e.getMessage());
            throw e;
        }
    }
    
    /**
     * Test more complex SQL queries
     */
    private static void testComplexQueries() throws Exception {
        logger.info("🔍 Testing complex SQL queries...");
        
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             Statement stmt = conn.createStatement()) {
            
            logger.info("Test 1: SELECT with CASE statement");
            try (ResultSet rs = stmt.executeQuery(
                "SELECT id, name, value, " +
                "CASE WHEN value > 20 THEN 'High' WHEN value > 15 THEN 'Medium' ELSE 'Low' END as category " +
                "FROM demo_table")) {
                while (rs.next()) {
                    int id = rs.getInt("id");
                    String name = rs.getString("name");
                    double value = rs.getDouble("value");
                    String category = rs.getString("category");
                    logger.info("   Row: id={}, name={}, value={}, category={}", id, name, value, category);
                }
            }
            
            logger.info("Test 2: SELECT with GROUP BY");
            try (ResultSet rs = stmt.executeQuery(
                "SELECT " +
                "CASE WHEN value > 20 THEN 'High' WHEN value > 15 THEN 'Medium' ELSE 'Low' END as category, " +
                "COUNT(*) as count, AVG(value) as avg_value " +
                "FROM demo_table " +
                "GROUP BY CASE WHEN value > 20 THEN 'High' WHEN value > 15 THEN 'Medium' ELSE 'Low' END")) {
                while (rs.next()) {
                    String category = rs.getString("category");
                    int count = rs.getInt("count");
                    double avgValue = rs.getDouble("avg_value");
                    logger.info("   Category: {}, count: {}, avg_value: {}", category, count, avgValue);
                }
            }
            
            logger.info("Test 3: SELECT with subquery");
            try (ResultSet rs = stmt.executeQuery(
                "SELECT * FROM demo_table WHERE value > (SELECT AVG(value) FROM demo_table)")) {
                int rowCount = 0;
                while (rs.next()) {
                    rowCount++;
                    int id = rs.getInt("id");
                    String name = rs.getString("name");
                    double value = rs.getDouble("value");
                    logger.info("   Row {}: id={}, name={}, value={}", rowCount, id, name, value);
                }
                logger.info("   Rows above average: {}", rowCount);
            }
            
        } catch (SQLException e) {
            logger.error("Complex queries failed: {}", e.getMessage());
            throw e;
        }
    }
    
    /**
     * Test metadata queries
     */
    private static void testMetadataQueries() throws Exception {
        logger.info("Testing metadata queries...");
        
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            
            logger.info("Test 1: Getting table information");
            DatabaseMetaData metaData = conn.getMetaData();
            
            try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    String tableType = tables.getString("TABLE_TYPE");
                    logger.info("   Table: {} (Type: {})", tableName, tableType);
                }
            }
            
            logger.info("Test 2: Getting column information for demo_table");
            try (ResultSet columns = metaData.getColumns(null, null, "demo_table", "%")) {
                while (columns.next()) {
                    String columnName = columns.getString("COLUMN_NAME");
                    String dataType = columns.getString("TYPE_NAME");
                    int columnSize = columns.getInt("COLUMN_SIZE");
                    logger.info("   Column: {} (Type: {}, Size: {})", columnName, dataType, columnSize);
                }
            }
            
            logger.info("Test 3: Getting primary key information");
            try (ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, "demo_table")) {
                while (primaryKeys.next()) {
                    String columnName = primaryKeys.getString("COLUMN_NAME");
                    short keySeq = primaryKeys.getShort("KEY_SEQ");
                    logger.info("   Primary Key: {} (Sequence: {})", columnName, keySeq);
                }
            }
            
        } catch (SQLException e) {
            logger.error("Metadata queries failed: {}", e.getMessage());
            throw e;
        }
    }
} 