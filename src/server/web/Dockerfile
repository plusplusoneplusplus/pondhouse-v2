FROM python:3.9-slim

WORKDIR /app

# Install protoc
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy proto files and generate Python code
COPY proto /app/proto
RUN python -m grpc_tools.protoc \
    -I/app/proto \
    --python_out=/app \
    --grpc_python_out=/app \
    /app/proto/pond_service.proto

# Copy application files
COPY main.py .
COPY templates templates/

# Create static directory
RUN mkdir static

EXPOSE 80

CMD ["python", "main.py"] 