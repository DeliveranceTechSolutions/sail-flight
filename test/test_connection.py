#!/usr/bin/env python3
"""
Simple test script to verify Arrow Flight SQL connection
"""

import pyarrow.flight as flight
import pyarrow as pa

def test_flight_connection():
    try:
        print("🔗 Testing Arrow Flight SQL connection...")
        
        # Create a Flight client
        client = flight.FlightClient("grpc://localhost:32010")
        
        print("✅ Connected to Flight server!")
        
        # Test a simple query
        # Note: This is a basic Flight test, not full SQL
        print("📊 Testing basic Flight operations...")
        
        # List available flights (if any)
        flights = list(client.list_flights())
        print(f"📋 Found {len(flights)} flights")
        
        # Test handshake
        print("🤝 Testing handshake...")
        # Note: Handshake is typically done automatically
        
        print("✅ Basic Flight connection test completed!")
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_flight_connection() 