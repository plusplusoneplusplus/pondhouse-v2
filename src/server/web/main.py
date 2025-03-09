from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import grpc
import os
import sys
from typing import Optional, List, Dict
from dataclasses import dataclass
from datetime import datetime

# Import generated gRPC modules
import pond_service_pb2 as pb2
import pond_service_pb2_grpc as pb2_grpc

app = FastAPI()

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Setup templates
templates = Jinja2Templates(directory="templates")

# gRPC client setup
GRPC_HOST = os.getenv("GRPC_HOST", "pond-server")
GRPC_PORT = os.getenv("GRPC_PORT", "8080")

@dataclass
class KeyValue:
    key: str
    value: Optional[str] = None

@dataclass
class DirectoryInfo:
    exists: bool
    is_directory: bool
    num_files: int
    total_size: int
    error: Optional[str] = None

@dataclass
class FileSystemEntry:
    path: str
    is_directory: bool

@dataclass
class ColumnInfo:
    name: str
    type: str

@dataclass
class TableMetadata:
    name: str
    location: str
    columns: List[ColumnInfo]
    partition_columns: List[str]
    properties: Dict[str, str]
    last_updated_ms: int
    error: Optional[str] = None

def normalize_path(path: str) -> str:
    """Normalize path by handling empty paths and removing trailing slashes."""
    if not path or path == "/":
        return "."
    # Remove trailing slash if present but preserve root path
    if path.endswith("/") and len(path) > 1:
        path = path[:-1]
    return path

class PondClient:
    def __init__(self, host: str, port: str):
        self.channel = grpc.insecure_channel(f"{host}:{port}")
        self.stub = pb2_grpc.PondServiceStub(self.channel)

    def get(self, key: str) -> tuple[bool, str, Optional[str]]:
        try:
            request = pb2.GetRequest(key=key.encode())
            response = self.stub.Get(request)
            if response.found:
                return True, response.value.decode(), None
            return False, "", response.error or "Key not found"
        except Exception as e:
            return False, "", str(e)

    def put(self, key: str, value: str) -> tuple[bool, Optional[str]]:
        try:
            request = pb2.PutRequest(key=key.encode(), value=value.encode())
            response = self.stub.Put(request)
            if response.success:
                return True, None
            return False, response.error
        except Exception as e:
            return False, str(e)

    def delete(self, key: str) -> tuple[bool, Optional[str]]:
        try:
            request = pb2.DeleteRequest(key=key.encode())
            response = self.stub.Delete(request)
            if response.success:
                return True, None
            return False, response.error
        except Exception as e:
            return False, str(e)

    def scan(self, start_key: str = "", end_key: str = "", limit: int = 0) -> tuple[list[KeyValue], Optional[str]]:
        try:
            request = pb2.ScanRequest(
                start_key=start_key.encode() if start_key else b"",
                end_key=end_key.encode() if end_key else b"",
                limit=limit
            )
            
            keys = []
            for response in self.stub.Scan(request):
                if response.error:
                    return [], response.error
                
                if response.key:
                    key = response.key.decode()
                    value = response.value.decode()
                    keys.append(KeyValue(key=key, value=value))
                
                if not response.has_more:
                    break
            
            return keys, None
        except Exception as e:
            return [], str(e)

    def list_detailed_files(self, path: str = "", recursive: bool = False) -> tuple[List[FileSystemEntry], Optional[str]]:
        try:
            normalized_path = normalize_path(path)
            request = pb2.ListDetailedFilesRequest(path=normalized_path, recursive=recursive)
            response = self.stub.ListDetailedFiles(request)
            
            if response.success:
                entries = []
                for entry in response.entries:
                    entries.append(FileSystemEntry(
                        path=entry.path,
                        is_directory=entry.is_directory
                    ))
                return entries, None
            return [], response.error
        except Exception as e:
            return [], str(e)
            
    def get_directory_info(self, path: str) -> DirectoryInfo:
        try:
            normalized_path = normalize_path(path)
            request = pb2.DirectoryInfoRequest(path=normalized_path)
            response = self.stub.GetDirectoryInfo(request)
            
            if response.success:
                return DirectoryInfo(
                    exists=response.exists,
                    is_directory=response.is_directory,
                    num_files=response.num_files,
                    total_size=response.total_size
                )
            return DirectoryInfo(
                exists=False, 
                is_directory=False, 
                num_files=0, 
                total_size=0, 
                error=response.error
            )
        except Exception as e:
            return DirectoryInfo(
                exists=False, 
                is_directory=False, 
                num_files=0, 
                total_size=0,
                error=str(e)
            )

    def execute_sql(self, sql_query: str) -> tuple[bool, Optional[str], Optional[str]]:
        """
        Execute a SQL query for table management. Currently only supports CREATE TABLE statements.
        
        Example: CREATE TABLE users (id INT, name STRING, email STRING)
        
        Returns a tuple of (success, message, error)
        """
        try:
            request = pb2.ExecuteSQLRequest(sql_query=sql_query)
            response = self.stub.ExecuteSQL(request)
            
            if response.success:
                return True, response.message, None
            return False, None, response.error
        except Exception as e:
            return False, None, str(e)
    
    def list_tables(self) -> tuple[List[str], Optional[str]]:
        """
        List all tables in the catalog.
        
        Returns a tuple of (table_names, error)
        """
        try:
            request = pb2.ListTablesRequest()
            response = self.stub.ListTables(request)
            
            if response.success:
                return list(response.table_names), None
            return [], response.error
        except Exception as e:
            return [], str(e)
    
    def get_table_metadata(self, table_name: str) -> TableMetadata:
        """
        Get metadata for a specific table.
        
        Returns TableMetadata object
        """
        try:
            request = pb2.GetTableMetadataRequest(table_name=table_name)
            response = self.stub.GetTableMetadata(request)
            
            if response.success:
                meta = response.metadata
                columns = [
                    ColumnInfo(name=col.name, type=col.type)
                    for col in meta.columns
                ]
                
                return TableMetadata(
                    name=meta.name,
                    location=meta.location,
                    columns=columns,
                    partition_columns=list(meta.partition_columns),
                    properties=dict(meta.properties),
                    last_updated_ms=meta.last_updated_ms
                )
            else:
                return TableMetadata(
                    name=table_name,
                    location="",
                    columns=[],
                    partition_columns=[],
                    properties={},
                    last_updated_ms=0,
                    error=response.error
                )
        except Exception as e:
            return TableMetadata(
                name=table_name,
                location="",
                columns=[],
                partition_columns=[],
                properties={},
                last_updated_ms=0,
                error=str(e)
            )

# Create a global client instance
pond_client = PondClient(GRPC_HOST, GRPC_PORT)

# Add a timestamp filter for Jinja2
@app.on_event("startup")
async def startup_event():
    # Add custom filters to Jinja2 templates
    templates.env.filters["timestamp_to_date"] = lambda ts: datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "active_page": "kv"
        }
    )

@app.post("/get", response_class=HTMLResponse)
async def get_value(request: Request, key: str = Form(...)):
    found, value, error = pond_client.get(key)
    
    result = None
    if error:
        result = f"Error: {error}"
    elif found:
        result = f"Value for key '{key}': {value}"
    else:
        result = f"Key '{key}' not found"
    
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "result": result,
            "active_page": "kv"
        }
    )

@app.post("/put", response_class=HTMLResponse)
async def put_value(request: Request, key: str = Form(...), value: str = Form(...)):
    success, error = pond_client.put(key, value)
    
    result = None
    if error:
        result = f"Error: {error}"
    else:
        result = f"Key '{key}' set successfully"
    
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "result": result,
            "active_page": "kv"
        }
    )

@app.post("/delete", response_class=HTMLResponse)
async def delete_value(request: Request, key: str = Form(...)):
    success, error = pond_client.delete(key)
    
    result = None
    if error:
        result = f"Error: {error}"
    else:
        result = f"Key '{key}' deleted successfully"
    
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "result": result,
            "active_page": "kv"
        }
    )

@app.post("/list", response_class=HTMLResponse)
async def list_keys(
    request: Request,
    start_key: str = Form(""),
    end_key: str = Form(""),
    limit: Optional[int] = Form(None)
):
    keys, error = pond_client.scan(start_key, end_key, limit if limit else 0)
    
    result = None
    if error:
        result = f"Error: {error}"
    else:
        result = f"Found {len(keys)} keys"
    
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "result": result,
            "keys": keys,
            "active_page": "kv"
        }
    )

@app.get("/files", response_class=HTMLResponse)
async def file_explorer(request: Request, path: str = ""):
    normalized_path = normalize_path(path)
    entries, error = pond_client.list_detailed_files(normalized_path, False)
    dir_info = pond_client.get_directory_info(normalized_path)
    
    result = None
    if error:
        result = f"Error listing files: {error}"
    elif dir_info.error:
        result = f"Error getting directory info: {dir_info.error}"
    
    return templates.TemplateResponse(
        "files.html",
        {
            "request": request,
            "path": path,  # Keep original path for UI
            "entries": entries,
            "dir_info": dir_info,
            "result": result,
            "active_page": "files"
        }
    )

@app.post("/files/list", response_class=HTMLResponse)
async def list_files(
    request: Request,
    path: str = Form(""),
    recursive: bool = Form(False)
):
    normalized_path = normalize_path(path)
    entries, error = pond_client.list_detailed_files(normalized_path, recursive)
    dir_info = pond_client.get_directory_info(normalized_path)
    
    result = None
    if error:
        result = f"Error listing files: {error}"
    elif dir_info.error:
        result = f"Error getting directory info: {dir_info.error}"
    
    return templates.TemplateResponse(
        "files.html",
        {
            "request": request,
            "path": path,  # Keep original path for UI
            "entries": entries,
            "dir_info": dir_info,
            "result": result,
            "active_page": "files"
        }
    )

@app.get("/catalog", response_class=HTMLResponse)
async def catalog_home(request: Request):
    tables, error = pond_client.list_tables()
    
    result = None
    if error:
        result = f"Error listing tables: {error}"
    
    return templates.TemplateResponse(
        "catalog.html",
        {
            "request": request,
            "tables": tables,
            "result": result,
            "active_page": "catalog"
        }
    )

@app.post("/catalog/execute-sql", response_class=HTMLResponse)
async def execute_sql(request: Request, sql_query: str = Form(...)):
    success, message, error = pond_client.execute_sql(sql_query)
    
    # Get the updated list of tables
    tables, list_error = pond_client.list_tables()
    
    result = None
    if error:
        result = f"Error executing SQL: {error}"
    elif message:
        result = message
    else:
        result = "SQL executed successfully"
    
    return templates.TemplateResponse(
        "catalog.html",
        {
            "request": request,
            "tables": tables,
            "result": result,
            "active_page": "catalog"
        }
    )

@app.get("/catalog/table/{table_name}", response_class=HTMLResponse)
async def table_details(request: Request, table_name: str):
    metadata = pond_client.get_table_metadata(table_name)
    
    return templates.TemplateResponse(
        "table_details.html",
        {
            "request": request,
            "metadata": metadata,
            "active_page": "catalog"
        }
    )

if __name__ == "__main__":
    import argparse
    import uvicorn
    import os
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="PondHouse Web Server")
    parser.add_argument("--port", type=int, default=8000, help="Web server port (default: 8000)")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host to bind to (default: 0.0.0.0)")
    args = parser.parse_args()
    
    # Environment variables can override command line arguments
    port = int(os.environ.get("WEB_PORT", args.port))
    
    print(f"Starting web server on {args.host}:{port}")
    uvicorn.run(app, host=args.host, port=port) 