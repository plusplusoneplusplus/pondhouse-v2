#!/usr/bin/env python3
"""
Pond gRPC Client

A command-line client for interacting with the Pond gRPC service.
This script allows you to perform Get, Put, Delete, and Scan operations.

Usage:
  python pond_client.py get <key>
  python pond_client.py put <key> <value>
  python pond_client.py delete <key>
  python pond_client.py scan [--start=<start_key>] [--end=<end_key>] [--limit=<limit>]
  python pond_client.py --help

Options:
  -h, --help            Show this help message and exit
  --host=<host>         Server host [default: localhost]
  --port=<port>         Server port [default: 50051]
  --start=<start_key>   Start key for scan operation
  --end=<end_key>       End key for scan operation
  --limit=<limit>       Maximum number of entries to return in scan operation
"""

import os
import sys
import argparse
import grpc
from google.protobuf.json_format import MessageToJson

# Add the proto build directory to the Python path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "proto", "build", "proto"))

# Import the generated gRPC modules
try:
    import pond_service_pb2 as pb2
    import pond_service_pb2_grpc as pb2_grpc
except ImportError:
    print("Error: Could not import the generated gRPC modules.")
    print("Make sure you've built the proto files and they're in the correct location.")
    print("Expected path: ../../proto/build/proto/")
    sys.exit(1)

class PondClient:
    """Client for interacting with the Pond gRPC service."""

    def __init__(self, host="localhost", port=50051):
        """Initialize the client with server address."""
        self.channel = grpc.insecure_channel(f"{host}:{port}")
        self.stub = pb2_grpc.PondServiceStub(self.channel)

    def get(self, key):
        """Get a value by key."""
        request = pb2.GetRequest(key=key.encode())
        try:
            response = self.stub.Get(request)
            if response.found:
                print(f"Key: {key}")
                print(f"Value: {response.value.decode()}")
            else:
                print(f"Key not found: {key}")
                if response.error:
                    print(f"Error: {response.error}")
            return response
        except grpc.RpcError as e:
            print(f"RPC Error: {e.code()}: {e.details()}")
            return None

    def put(self, key, value):
        """Put a key-value pair."""
        request = pb2.PutRequest(key=key.encode(), value=value.encode())
        try:
            response = self.stub.Put(request)
            if response.success:
                print(f"Successfully stored key: {key}")
            else:
                print(f"Failed to store key: {key}")
                if response.error:
                    print(f"Error: {response.error}")
            return response
        except grpc.RpcError as e:
            print(f"RPC Error: {e.code()}: {e.details()}")
            return None

    def delete(self, key):
        """Delete a key."""
        request = pb2.DeleteRequest(key=key.encode())
        try:
            response = self.stub.Delete(request)
            if response.success:
                print(f"Successfully deleted key: {key}")
            else:
                print(f"Failed to delete key: {key}")
                if response.error:
                    print(f"Error: {response.error}")
            return response
        except grpc.RpcError as e:
            print(f"RPC Error: {e.code()}: {e.details()}")
            return None

    def scan(self, start_key="", end_key="", limit=0):
        """Scan a range of keys."""
        request = pb2.ScanRequest(
            start_key=start_key.encode() if start_key else b"",
            end_key=end_key.encode() if end_key else b"",
            limit=limit
        )
        try:
            count = 0
            print("Scan Results:")
            print("-" * 40)
            for response in self.stub.Scan(request):
                if response.error:
                    print(f"Error: {response.error}")
                    break
                
                if response.key:  # Only print if there's a key (not just a has_more indicator)
                    key = response.key.decode()
                    value = response.value.decode()
                    print(f"Key: {key}")
                    print(f"Value: {value}")
                    print("-" * 40)
                    count += 1
                
                if not response.has_more:
                    break
            
            print(f"Total entries: {count}")
            return count
        except grpc.RpcError as e:
            print(f"RPC Error: {e.code()}: {e.details()}")
            return 0

def main():
    """Parse command line arguments and execute the appropriate command."""
    parser = argparse.ArgumentParser(description="Pond gRPC Client")
    parser.add_argument("--host", default="localhost", help="Server host")
    parser.add_argument("--port", default=50051, type=int, help="Server port")
    
    subparsers = parser.add_subparsers(dest="command", help="Command")
    
    # Get command
    get_parser = subparsers.add_parser("get", help="Get a value by key")
    get_parser.add_argument("key", help="Key to get")
    
    # Put command
    put_parser = subparsers.add_parser("put", help="Put a key-value pair")
    put_parser.add_argument("key", help="Key to put")
    put_parser.add_argument("value", help="Value to put")
    
    # Delete command
    delete_parser = subparsers.add_parser("delete", help="Delete a key")
    delete_parser.add_argument("key", help="Key to delete")
    
    # Scan command
    scan_parser = subparsers.add_parser("scan", help="Scan a range of keys")
    scan_parser.add_argument("--start", help="Start key for scan")
    scan_parser.add_argument("--end", help="End key for scan")
    scan_parser.add_argument("--limit", type=int, default=0, help="Maximum number of entries to return")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    client = PondClient(args.host, args.port)
    
    if args.command == "get":
        client.get(args.key)
    elif args.command == "put":
        client.put(args.key, args.value)
    elif args.command == "delete":
        client.delete(args.key)
    elif args.command == "scan":
        client.scan(args.start or "", args.end or "", args.limit)

if __name__ == "__main__":
    main() 