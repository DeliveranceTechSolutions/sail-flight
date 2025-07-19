#!/bin/bash

echo "Building Sail Flight SQL Test Application..."

# Build the project
mvn clean compile

if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

echo "Build successful!"

echo "Running Sail Flight SQL Test Application..."
echo "Connecting to: jdbc:arrow-flight-sql://localhost:32010"
echo ""

# Run the application
mvn exec:java -Dexec.mainClass="com.sail.flight.test.SailFlightSqlTest"

echo ""
echo "Test completed!" 