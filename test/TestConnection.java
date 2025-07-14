import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestConnection {
    public static void main(String[] args) {
        try {
            System.out.println("🔗 Testing Arrow Flight SQL connection...");
            
            // Load the Arrow Flight SQL driver
            Class.forName("org.apache.arrow.flight.sql.jdbc.FlightSqlDriver");
            
            // Connect to your server
            String url = "jdbc:arrow-flight-sql://localhost:32010";
            Connection conn = DriverManager.getConnection(url);
            
            System.out.println("✅ Connected successfully!");
            
            // Test a simple query
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 'Hello from JDBC!' as message");
            
            while (rs.next()) {
                String message = rs.getString("message");
                System.out.println("📊 Query result: " + message);
            }
            
            rs.close();
            stmt.close();
            conn.close();
            
            System.out.println("✅ Test completed successfully!");
            
        } catch (Exception e) {
            System.err.println("❌ Connection failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
} 