package com.sail.flight.test;

import java.sql.*;

/**
 * Simple test application for Sail Arrow Flight SQL Server
 */
public class SimpleTest {
    
    public static void main(String[] args) {
        System.out.println("Starting Simple Sail Flight SQL Test");
        
        try {
            Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");
            System.out.println("Arrow Flight SQL JDBC driver loaded");
            
            String url = "jdbc:arrow-flight-sql://localhost:32010?useEncryption=false";
            System.out.println("Connecting to: " + url);
            
            try (Connection conn = DriverManager.getConnection(url)) {
                System.out.println("Successfully connected!");
                
                DatabaseMetaData metaData = conn.getMetaData();
                StringBuilder sb = new StringBuilder();
                StringBuilder database = sb.append("Database: ").append(metaData.getDatabaseProductName()).append(" ").append(metaData.getDatabaseProductVersion());
                System.out.println(database.toString());
                sb.setLength(0);
                StringBuilder driver = sb.append("Driver: ").append(metaData.getDriverName()).append(" ").append(metaData.getDriverVersion());
                System.out.println(driver.toString());
                
                try (Statement stmt = conn.createStatement()) {
                    System.out.println("Testing simple SELECT query...");
                    try (ResultSet rs = stmt.executeQuery("SELECT 'Hello from Sail!' as message")) {
                        while (rs.next()) {
                            String message = rs.getString("message");
                            System.out.println("   Result: " + message);
                        }
                    }
                    
                    System.out.println("Testing demo table query...");
                    try (ResultSet rs = stmt.executeQuery("SELECT * FROM demo_table")) {
                        int rowCount = 0;
                        while (rs.next()) {
                            rowCount++;
                            int id = rs.getInt("id");
                            String name = rs.getString("name");
                            double value = rs.getDouble("value");
                            System.out.println("   Row " + rowCount + ": id=" + id + ", name=" + name + ", value=" + value);
                        }
                        System.out.println("   Total rows: " + rowCount);
                    }
                    
                    System.out.println("Testing aggregation query...");
                    try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count, AVG(value) as avg_value FROM demo_table")) {
                        while (rs.next()) {
                            int count = rs.getInt("count");
                            double avgValue = rs.getDouble("avg_value");
                            System.out.println("   Result: count=" + count + ", avg_value=" + avgValue);
                        }
                    }
                }
                
            } catch (SQLException e) {
                System.err.println("SQL Error: " + e.getMessage());
                e.printStackTrace();
            }
            
        } catch (ClassNotFoundException e) {
            System.err.println("Driver not found: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            e.printStackTrace();
        }
        
        System.out.println("Test completed!");
    }
} 