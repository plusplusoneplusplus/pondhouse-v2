# Pond gRPC Client

This directory contains a Python client for interacting with the Pond gRPC service.

## Prerequisites

Before using the client, make sure you have the following installed:

1. Python 3.6 or higher
2. gRPC Python packages:
   ```
   pip install grpcio grpcio-tools protobuf
   ```

## Building the Proto Files

The client requires the generated Python code from the proto files. If these haven't been generated yet, you can build them with:

```bash
# From the project root directory
python -m grpc_tools.protoc -I./src/proto --python_out=./src/proto/build/proto --grpc_python_out=./src/proto/build/proto ./src/proto/pond_service.proto
```

Make sure the `build/proto` directory exists:

```bash
mkdir -p ./src/proto/build/proto
```

## Using the Client

The client supports the following operations:

### Get a value by key

```bash
python pond_client.py get <key>
```

Example:
```bash
python pond_client.py get my_key
```

### Put a key-value pair

```bash
python pond_client.py put <key> <value>
```

Example:
```bash
python pond_client.py put my_key "Hello, world!"
```

### Delete a key

```bash
python pond_client.py delete <key>
```

Example:
```bash
python pond_client.py delete my_key
```

### Scan a range of keys

```bash
python pond_client.py scan [--start=<start_key>] [--end=<end_key>] [--limit=<limit>]
```

Example:
```bash
# Scan all keys
python pond_client.py scan

# Scan with a start key
python pond_client.py scan --start=a

# Scan with a start and end key
python pond_client.py scan --start=a --end=z

# Limit the number of results
python pond_client.py scan --limit=10
```

### Connecting to a different server

By default, the client connects to `localhost:50051`. You can specify a different host and port:

```bash
python pond_client.py --host=192.168.1.100 --port=8080 get my_key
```

## Troubleshooting

If you encounter issues:

1. Make sure the Pond gRPC server is running
2. Verify that the proto files have been correctly built
3. Check that the Python path in the client script correctly points to the generated modules
4. Ensure you have the required Python packages installed

## Error Messages

- "Could not import the generated gRPC modules": Make sure you've built the proto files and they're in the correct location
- "RPC Error": The client couldn't connect to the server or the server returned an error 