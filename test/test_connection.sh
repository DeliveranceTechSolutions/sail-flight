#!/bin/bash

echo "🔗 Testing Sail Arrow Flight SQL Server connection..."

# Test if the server is listening
if nc -z localhost 32010; then
    echo "✅ Server is listening on port 32010"
else
    echo "❌ Server is not listening on port 32010"
    exit 1
fi

# Test basic TCP connection
echo "📡 Testing TCP connection..."
if timeout 5 bash -c "</dev/tcp/localhost/32010"; then
    echo "✅ TCP connection successful"
else
    echo "❌ TCP connection failed"
    exit 1
fi

echo "🎉 Basic connection test passed!"
echo ""
echo "📋 Next steps:"
echo "1. Install Arrow Flight SQL JDBC driver"
echo "2. Use DBeaver or similar tool to connect"
echo "3. Test with: jdbc:arrow-flight-sql://localhost:32010" 