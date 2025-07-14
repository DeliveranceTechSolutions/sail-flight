# 🔧 DBeaver Setup Guide for Arrow Flight SQL

## 📥 **Step 1: Download Arrow Flight SQL JDBC Driver**

### **Option A: Manual Download**
1. Go to [Maven Central - Arrow Flight SQL JDBC](https://mvnrepository.com/artifact/org.apache.arrow/flight-sql-jdbc)
2. Click on the latest version (e.g., 14.0.2)
3. Download the `.jar` file (e.g., `flight-sql-jdbc-14.0.2.jar`)

### **Option B: Build from Source**
```bash
# Clone Arrow repository
git clone https://github.com/apache/arrow.git
cd arrow/java/flight/flight-sql-jdbc

# Build the driver
mvn clean package

# Find the JAR in target/ directory
```

### **Option C: Use Maven**
```bash
# If you have Maven installed
mvn dependency:get -Dartifact=org.apache.arrow:flight-sql-jdbc:14.0.2
```

## 🔧 **Step 2: Add Driver to DBeaver**

1. **Open DBeaver**
2. **Go to Database → Driver Manager**
3. **Click "New"**
4. **Fill in the details:**
   - **Driver Name**: `Arrow Flight SQL`
   - **Driver Class**: `org.apache.arrow.flight.sql.jdbc.FlightSqlDriver`
   - **URL Template**: `jdbc:arrow-flight-sql://{host}:{port}`
   - **Default Port**: `32010`

5. **Add the JAR file:**
   - Click "Add File" or "Add Folder"
   - Navigate to where you downloaded the `flight-sql-jdbc-*.jar`
   - Select the JAR file

6. **Click "OK" to save**

## 🔗 **Step 3: Create Connection**

1. **Click "New Database Connection"**
2. **Select "Arrow Flight SQL" driver**
3. **Fill in connection details:**
   - **Host**: `127.0.0.1`
   - **Port**: `32010`
   - **URL**: `jdbc:arrow-flight-sql://127.0.0.1:32010`

4. **Test Connection** (it should work!)

## 🐛 **Troubleshooting**

### **If driver class not found:**
- Make sure you downloaded the correct JAR file
- Check that the JAR is properly added in Driver Manager
- Try restarting DBeaver

### **If connection fails:**
- Verify your server is running: `./target/debug/sail flight server`
- Check the port: `netstat -tlnp | grep 32010`
- Try different host: `0.0.0.0` instead of `127.0.0.1`

### **Alternative: Use Generic JDBC Driver**
If the Arrow Flight SQL driver doesn't work, you can use the generic JDBC driver:

1. **Driver Manager → New**
2. **Driver Name**: `Generic JDBC`
3. **Driver Class**: `org.apache.arrow.flight.sql.jdbc.FlightSqlDriver`
4. **URL Template**: `jdbc:arrow-flight-sql://{host}:{port}`
5. **Add the JAR file**

## 📋 **Alternative Tools**

### **Option 1: Use Tableau**
- Tableau has built-in Arrow Flight SQL support
- Connection string: `jdbc:arrow-flight-sql://127.0.0.1:32010`

### **Option 2: Use Python with pyarrow**
```python
import pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:32010")
# Test connection
```

### **Option 3: Use Java directly**
```java
Class.forName("org.apache.arrow.flight.sql.jdbc.FlightSqlDriver");
Connection conn = DriverManager.getConnection(
    "jdbc:arrow-flight-sql://127.0.0.1:32010"
);
```

## 🎯 **Quick Test**

Once you have the driver set up, you should be able to:
1. Connect to your server
2. See the connection in DBeaver
3. Execute SQL queries (they'll return demo data for now)

## 📚 **Resources**

- [Arrow Flight SQL Documentation](https://arrow.apache.org/docs/format/FlightSql.html)
- [Arrow Flight SQL JDBC Driver Source](https://github.com/apache/arrow/tree/main/java/flight/flight-sql-jdbc)
- [DBeaver Documentation](https://dbeaver.io/docs/) 