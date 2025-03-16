from fastapi import FastAPI, Request, Form, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import grpc
import os
import sys
from typing import Optional, List, Dict
from dataclasses import dataclass, field
from datetime import datetime
import json

# Import generated gRPC modules
import pond_service_pb2 as pb2
import pond_service_pb2_grpc as pb2_grpc
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Add CORS middleware for development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
class DataFile:
    path: str
    content_length: int
    partition_values: Dict[str, str]
    record_count: int

@dataclass
class TableMetadata:
    name: str
    location: str
    columns: List[ColumnInfo]
    partition_columns: List[str]
    properties: Dict[str, str]
    last_updated_time: int
    data_files: List[DataFile] = field(default_factory=list)
    partition_spec: List[Dict[str, any]] = field(default_factory=list)
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

    def get(self, key: str, table: str = "default") -> tuple[bool, bool, Optional[str], Optional[str]]:
        """
        Get a value for a key from the KV store.
        
        Returns a tuple of (success, found, value, error)
        """
        try:
            request = pb2.GetRequest(key=key.encode(), table_name=table)
            response = self.stub.Get(request)
            
            if response.error:
                return False, False, None, response.error
            
            value = response.value.decode() if response.found else None
            return True, response.found, value, None
        except Exception as e:
            return False, False, None, str(e)

    def put(self, key: str, value: str, table: str = "default") -> tuple[bool, Optional[str]]:
        """
        Put a key-value pair into the KV store.
        
        Returns a tuple of (success, error)
        """
        try:
            request = pb2.PutRequest(key=key.encode(), value=value.encode(), table_name=table)
            response = self.stub.Put(request)
            
            if response.error:
                return False, response.error
            
            return True, None
        except Exception as e:
            return False, str(e)

    def delete(self, key: str, table: str = "default") -> tuple[bool, Optional[str]]:
        """
        Delete a key from the KV store.
        
        Returns a tuple of (success, error)
        """
        try:
            request = pb2.DeleteRequest(key=key.encode(), table_name=table)
            response = self.stub.Delete(request)
            
            if response.error:
                return False, response.error
            
            return True, None
        except Exception as e:
            return False, str(e)

    def scan(self, start_key: str = "", end_key: str = "", limit: int = 0, table: str = "default") -> tuple[list[KeyValue], Optional[str]]:
        """
        Scan keys from the KV store.
        
        Returns a tuple of (key_values, error)
        """
        try:
            request = pb2.ScanRequest(
                start_key=start_key.encode() if start_key else b"",
                end_key=end_key.encode() if end_key else b"",
                limit=limit,
                table_name=table
            )
            
            keys = []
            error = None
            
            for response in self.stub.Scan(request):
                if response.error:
                    error = response.error
                    break
                
                key = response.key.decode()
                value = response.value.decode() if response.value else None
                
                keys.append(KeyValue(key=key, value=value))
            
            return keys, error
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

    async def execute_sql(self, sql_query: str) -> tuple[bool, Optional[str], Optional[str]]:
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
                
                data_files = [
                    DataFile(
                        path=file.path,
                        content_length=file.content_length,
                        partition_values=dict(file.partition_values),
                        record_count=file.record_count
                    )
                    for file in meta.data_files
                ]
                
                partition_spec = []
                for field in meta.partition_spec.fields:
                    spec = {
                        "source_id": field.source_id,
                        "field_id": field.field_id,
                        "name": field.name,
                        "transform": field.transform
                    }
                    if field.transform_param:
                        spec["transform_param"] = field.transform_param
                    partition_spec.append(spec)
                
                return TableMetadata(
                    name=meta.name,
                    location=meta.location,
                    columns=columns,
                    partition_columns=list(meta.partition_columns),
                    properties=dict(meta.properties),
                    last_updated_time=meta.last_updated_time,
                    data_files=data_files,
                    partition_spec=partition_spec
                )
            else:
                return TableMetadata(
                    name=table_name,
                    location="",
                    columns=[],
                    partition_columns=[],
                    properties={},
                    last_updated_time=0,
                    data_files=[],
                    partition_spec=[],
                    error=response.error
                )
        except Exception as e:
            return TableMetadata(
                name=table_name,
                location="",
                columns=[],
                partition_columns=[],
                properties={},
                last_updated_time=0,
                data_files=[],
                partition_spec=[],
                error=str(e)
            )

    def ingest_json_data(self, table_name: str, json_data: str) -> tuple[bool, int, Optional[str]]:
        """
        Ingest JSON data into a table.
        
        Returns a tuple of (success, rows_ingested, error)
        """
        try:
            request = pb2.IngestJsonDataRequest(table_name=table_name, json_data=json_data)
            response = self.stub.IngestJsonData(request)
            
            if response.success:
                return True, response.rows_ingested, None
            return False, 0, response.error_message
        except Exception as e:
            return False, 0, str(e)

    def get_kv_tables(self) -> tuple[List[str], Optional[str]]:
        """
        List all tables in the KV store that can be used for KV operations.
        
        Returns a tuple of (table_names, error)
        """
        try:
            request = pb2.ListKVTablesRequest()
            response = self.stub.ListKVTables(request)
            
            if response.success:
                # Sort tables with "default" first
                kv_tables = list(response.table_names)
                
                # Always include the default table even if not returned by the server
                if "default" not in kv_tables:
                    kv_tables.append("default")
                
                # Sort alphabetically
                kv_tables.sort()
                
                # Ensure default is first
                if "default" in kv_tables:
                    kv_tables.remove("default")
                    kv_tables.insert(0, "default")
                    
                return kv_tables, None
            return ["default"], response.error  # Always include default table even on error
        except Exception as e:
            return ["default"], str(e)  # Always include default table even on exception
    
    def create_kv_table(self, table_name: str) -> tuple[bool, Optional[str]]:
        """
        Create a new KV table.
        
        Returns a tuple of (success, error)
        """
        try:
            request = pb2.CreateKVTableRequest(table_name=table_name)
            response = self.stub.CreateKVTable(request)
            
            if response.success:
                return True, None
            return False, response.error
        except Exception as e:
            return False, str(e)

    def read_parquet_file(self, file_path: str, batch_size: int = 1000, columns: List[str] = None,
                         start_row: int = 0, num_rows: int = 0) -> tuple[List[Dict[str, str]], int, Optional[str]]:
        """
        Read content from a parquet file.
        
        Args:
            file_path: Path to the parquet file
            batch_size: Number of rows to read in each batch
            columns: Optional list of column names to read
            start_row: Starting row number for pagination
            num_rows: Maximum number of rows to read (0 means read all)
            
        Returns:
            Tuple of (rows, total_rows, error)
            where rows is a list of dictionaries with column names as keys
        """
        try:
            request = pb2.ReadParquetFileRequest(
                file_path=file_path,
                batch_size=batch_size,
                columns=columns or [],
                start_row=start_row,
                num_rows=num_rows
            )
            
            rows = []
            total_rows = 0
            error = None
            
            for response in self.stub.ReadParquetFile(request):
                if not response.success:
                    return [], 0, response.error
                
                total_rows = response.total_rows
                
                # Convert columnar data to row-based format
                if response.columns:
                    num_rows_in_batch = len(response.columns[0].values)
                    for row_idx in range(num_rows_in_batch):
                        row = {}
                        for col in response.columns:
                            if col.nulls[row_idx]:
                                row[col.name] = None
                            else:
                                row[col.name] = col.values[row_idx]
                        rows.append(row)
            
            return rows, total_rows, None
        except Exception as e:
            return [], 0, str(e)

    async def update_partition_spec(self, table_name: str, partition_spec: List[Dict[str, any]]) -> tuple[bool, Optional[str]]:
        """
        Update the partition specification for a table.
        
        Returns a tuple of (success, error)
        """
        try:
            request = pb2.UpdatePartitionSpecRequest(
                table_name=table_name,
                partition_spec=pb2.PartitionSpec(fields=[
                    pb2.PartitionField(
                        source_id=field["source_id"],
                        field_id=field["field_id"],
                        name=field["name"],
                        transform=field["transform"],
                        transform_param=str(field["transform_param"]) if "transform_param" in field else ""
                    ) for field in partition_spec
                ])
            )
            response = self.stub.UpdatePartitionSpec(request)
            
            if response.success:
                return True, None
            return False, response.error
        except Exception as e:
            return False, str(e)

    def execute_query(self, sql_query: str, batch_size: int = 1000) -> tuple[List[Dict[str, str]], int, Optional[str]]:
        """
        Execute a SQL query and return the results.
        
        Args:
            sql_query: The SQL query to execute
            batch_size: Number of rows to fetch per batch
            
        Returns:
            Tuple of (rows, total_rows, error)
            where rows is a list of dictionaries with column names as keys
        """
        try:
            request = pb2.ExecuteQueryRequest(
                sql_query=sql_query,
                batch_size=batch_size
            )
            
            rows = []
            total_rows = 0
            error = None
            column_names = []
            
            for response in self.stub.ExecuteQuery(request):
                if not response.success:
                    return [], 0, response.error
                
                total_rows = response.total_rows
                
                # Get column names from first response
                if not column_names and response.columns:
                    column_names = [col.name for col in response.columns]
                
                # Convert columnar data to row-based format
                if response.columns:
                    num_rows_in_batch = len(response.columns[0].values)
                    for row_idx in range(num_rows_in_batch):
                        row = {}
                        for col in response.columns:
                            if col.nulls[row_idx]:
                                row[col.name] = None
                            else:
                                row[col.name] = col.values[row_idx]
                        rows.append(row)
            
            return rows, total_rows, None
        except Exception as e:
            return [], 0, str(e)

# Create a global client instance
pond_client = PondClient(GRPC_HOST, GRPC_PORT)

# Add a timestamp filter for Jinja2
@app.on_event("startup")
async def startup_event():
    # Add custom filters to Jinja2 templates
    templates.env.filters["timestamp_to_date"] = lambda ts: datetime.fromtimestamp(ts / 1000000).strftime('%Y-%m-%d %H:%M:%S') if ts else "N/A"
    
    # Add a file size formatter
    def filesizeformat(value):
        """Format the value as a 'human-readable' file size (i.e. 13 KB, 4.1 MB, 102 bytes, etc)."""
        value = float(value)
        if value < 1024:
            return f"{value:.0f} bytes"
        elif value < 1024 * 1024:
            return f"{value/1024:.1f} KB"
        elif value < 1024 * 1024 * 1024:
            return f"{value/(1024*1024):.1f} MB"
        else:
            return f"{value/(1024*1024*1024):.1f} GB"
    
    templates.env.filters["filesizeformat"] = filesizeformat

@app.get("/", response_class=HTMLResponse)
async def home(request: Request, table: str = "default"):
    # Get list of KV tables
    tables, error = pond_client.get_kv_tables()
    
    result = None
    if error:
        result = f"Error listing tables: {error}"
    
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "active_page": "kv",
            "current_table": table,
            "tables": tables,
            "result": result
        }
    )

@app.post("/create-table", response_class=HTMLResponse)
async def create_table(request: Request, table_name: str = Form(...)):
    success, error = pond_client.create_kv_table(table_name)
    
    result = None
    if error:
        result = f"Error creating table: {error}"
    else:
        result = f"Table '{table_name}' created successfully"
    
    # Get updated list of tables
    tables, list_error = pond_client.get_kv_tables()
    if list_error:
        result = f"Error listing tables: {list_error}"
    
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "active_page": "kv",
            "current_table": table_name if success else "default",
            "tables": tables,
            "result": result
        }
    )

@app.post("/get", response_class=HTMLResponse)
async def get_value(request: Request, table: str = Form(...), key: str = Form(...)):
    success, found, value, error = pond_client.get(key, table)
    
    result = None
    keys = None
    
    if error:
        result = f"Error: {error}"
    elif not success:
        result = f"Failed to get value for key '{key}'"
    elif not found:
        result = f"Key '{key}' not found"
    else:
        result = f"Key '{key}' found"
        keys = [KeyValue(key=key, value=value)]
    
    # Get list of KV tables for the dropdown
    tables, _ = pond_client.get_kv_tables()
    
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "result": result,
            "keys": keys,
            "active_page": "kv",
            "current_table": table,
            "tables": tables
        }
    )

@app.post("/put", response_class=HTMLResponse)
async def put_value(request: Request, table: str = Form(...), key: str = Form(...), value: str = Form(...)):
    success, error = pond_client.put(key, value, table)
    
    result = None
    if error:
        result = f"Error: {error}"
    elif not success:
        result = f"Failed to put value for key '{key}'"
    else:
        result = f"Value for key '{key}' saved successfully"
    
    # Get list of KV tables for the dropdown
    tables, _ = pond_client.get_kv_tables()
    
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "result": result,
            "active_page": "kv",
            "current_table": table,
            "tables": tables
        }
    )

@app.post("/delete", response_class=HTMLResponse)
async def delete_value(request: Request, table: str = Form(...), key: str = Form(...)):
    success, error = pond_client.delete(key, table)
    
    result = None
    if error:
        result = f"Error: {error}"
    elif not success:
        result = f"Failed to delete key '{key}'"
    else:
        result = f"Key '{key}' deleted successfully"
    
    # Get list of KV tables for the dropdown
    tables, _ = pond_client.get_kv_tables()
    
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "result": result,
            "active_page": "kv",
            "current_table": table,
            "tables": tables
        }
    )

@app.post("/list", response_class=HTMLResponse)
async def list_keys(
    request: Request,
    table: str = Form(...),
    start_key: str = Form(""),
    end_key: str = Form(""),
    limit: Optional[int] = Form(None)
):
    keys, error = pond_client.scan(start_key, end_key, limit if limit else 0, table)
    
    result = None
    if error:
        result = f"Error: {error}"
    else:
        result = f"Found {len(keys)} keys"
    
    # Get list of KV tables for the dropdown
    tables, _ = pond_client.get_kv_tables()
    
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "result": result,
            "keys": keys,
            "active_page": "kv",
            "current_table": table,
            "tables": tables
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

@app.post("/catalog/execute-query", response_class=HTMLResponse)
async def execute_query(request: Request):
    form = await request.form()
    sql_query = form.get("sql_query", "").strip()
    sql_query = " ".join(line.strip() for line in sql_query.splitlines() if line.strip())
    
    try:
        # Check if it's a CREATE TABLE statement
        if sql_query.upper().startswith("CREATE TABLE"):
            success, message, error = await pond_client.execute_sql(sql_query)
            if error:
                raise Exception(error)
            
            # Redirect to catalog page after successful table creation
            return RedirectResponse(url="/catalog", status_code=303)
        
        # For other queries (SELECT, etc)
        rows, total_rows, error = pond_client.execute_query(sql_query)
        
        if error:
            return templates.TemplateResponse(
                "catalog.html",
                {
                    "request": request,
                    "active_page": "catalog",
                    "result": f"Error executing query: {error}",
                    "tables": pond_client.list_tables()[0]
                }
            )
        
        # Get column names from first row
        columns = []
        if rows:
            columns = list(rows[0].keys())
        
        return templates.TemplateResponse(
            "query_results.html",
            {
                "request": request,
                "active_page": "catalog",
                "rows": rows,
                "columns": columns,
                "total_rows": total_rows,
                "query": sql_query
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "catalog.html",
            {
                "request": request,
                "active_page": "catalog",
                "result": f"Error: {str(e)}",
                "tables": pond_client.list_tables()[0]
            }
        )

@app.post("/catalog/create-table")
async def create_table(request: Request):
    form = await request.form()
    table_name = form.get("table_name", "")
    location = form.get("location", "")
    
    try:
        # Get column information
        column_names = form.getlist("column_names[]")
        column_types = form.getlist("column_types[]")
        column_nullables = form.getlist("column_nullables[]")
        
        # Build schema
        schema = []
        for i, name in enumerate(column_names):
            sql_type = column_types[i]
                
            schema.append({
                "name": name,
                "type": sql_type,  # Use original SQL type
                "nullable": str(i) in column_nullables or f"{i}" in column_nullables or "1" in column_nullables
            })
        
        # Get partition information
        partition_columns = form.getlist("partition_columns[]")
        partition_transforms = form.getlist("partition_transforms[]")
        transform_params = form.getlist("transform_params[]")
        
        # Build partition spec
        partition_spec = []
        for i, col in enumerate(partition_columns):
            if not col:  # Skip empty selections
                continue
                
            # Find the source_id (index) of the column in the schema
            source_id = next(
                (idx for idx, s in enumerate(schema) if s["name"] == col),
                None
            )
            if source_id is None:
                raise ValueError(f"Partition column {col} not found in schema")
                
            transform = partition_transforms[i]
            field = {
                "source_id": source_id,
                "field_id": i,  # Use index as field_id
                "name": col,
                "transform": transform,
            }
            
            # Add transform_param for BUCKET and TRUNCATE
            if transform in ["BUCKET", "TRUNCATE"]:
                param = transform_params[i]
                if param and param.isdigit() and int(param) > 0:
                    field["transform_param"] = int(param)
                else:
                    raise ValueError(f"Invalid transform parameter for {transform}")
            
            partition_spec.append(field)
        
        # Create SQL for table creation
        columns_sql = []
        for col in schema:
            nullable_str = "" if col["nullable"] else " NOT NULL"
            columns_sql.append(f"{col['name']} {col['type']}{nullable_str}")
        
        sql = f"CREATE TABLE {table_name} ({', '.join(columns_sql)})"
        
        # Execute table creation - Fix: properly await the result
        success, message, error = await pond_client.execute_sql(sql)
        if error:
            raise Exception(error)
            
        # Update partition spec if needed
        if partition_spec:
            success, error = await pond_client.update_partition_spec(table_name, partition_spec)
            if error:
                raise Exception(f"Table created but failed to set partition spec: {error}")
        
        return RedirectResponse(url="/catalog", status_code=303)
        
    except Exception as e:
        tables, _ = pond_client.list_tables()
        return templates.TemplateResponse(
            "catalog.html",
            {
                "request": request,
                "tables": tables,
                "result": f"Error: {str(e)}",
                "active_page": "catalog"
            }
        )

@app.get("/catalog/table/{table_name}", response_class=HTMLResponse)
async def table_details(request: Request, table_name: str, sort_by: str = None, order: str = None):
    metadata = pond_client.get_table_metadata(table_name)
    
    # Sort data files if requested
    if metadata.data_files and sort_by:
        reverse = order == "desc"
        if sort_by == "path":
            metadata.data_files.sort(key=lambda x: x.path, reverse=reverse)
        elif sort_by == "size":
            metadata.data_files.sort(key=lambda x: x.content_length, reverse=reverse)
        elif sort_by == "records":
            metadata.data_files.sort(key=lambda x: x.record_count, reverse=reverse)
    
    # Calculate totals for summary
    total_size = sum(file.content_length for file in metadata.data_files) if metadata.data_files else 0
    total_records = sum(file.record_count for file in metadata.data_files) if metadata.data_files else 0
    
    return templates.TemplateResponse(
        "table_details.html",
        {
            "request": request,
            "metadata": metadata,
            "active_page": "catalog",
            "sort_by": sort_by,
            "order": order,
            "total_size": total_size,
            "total_records": total_records,
            "upload_result": None  # Initialize upload_result as None
        }
    )

@app.post("/catalog/table/{table_name}/ingest-json", response_class=HTMLResponse)
async def ingest_json_data(
    request: Request, 
    table_name: str, 
    json_data: str = Form(None),
    json_file: UploadFile = File(None),
    sort_by: str = Form(None),
    order: str = Form(None)
):
    # Track result for displaying feedback to the user
    upload_result = {
        "success": False,
        "message": "",
        "rows_ingested": 0
    }
    
    # Get data from either the textarea or the uploaded file
    data_to_ingest = None
    
    if json_file and json_file.filename:
        # Read data from the uploaded file
        contents = await json_file.read()
        try:
            data_to_ingest = contents.decode('utf-8')
        except UnicodeDecodeError:
            upload_result["message"] = "Error: The uploaded file is not a valid UTF-8 encoded text file."
    elif json_data:
        # Use the data from the textarea
        data_to_ingest = json_data
    else:
        # No data provided
        upload_result["message"] = "Error: No JSON data provided. Please enter JSON data or upload a file."
    
    # Process data if available
    if data_to_ingest:
        # Validate JSON format before sending to the server
        try:
            # Parse the JSON to validate it (we'll still send the original string)
            json.loads(data_to_ingest)
            
            # Send to the pond service
            success, rows_ingested, error = pond_client.ingest_json_data(table_name, data_to_ingest)
            
            if success:
                upload_result["success"] = True
                upload_result["rows_ingested"] = rows_ingested
                upload_result["message"] = f"Successfully ingested {rows_ingested} rows into {table_name}."
            else:
                upload_result["message"] = f"Error: {error}"
        except json.JSONDecodeError as e:
            upload_result["message"] = f"Error: Invalid JSON format - {str(e)}"
    
    # Get updated metadata (will show newly added files)
    metadata = pond_client.get_table_metadata(table_name)
    
    # Sort data files if requested
    if metadata.data_files and sort_by:
        reverse = order == "desc"
        if sort_by == "path":
            metadata.data_files.sort(key=lambda x: x.path, reverse=reverse)
        elif sort_by == "size":
            metadata.data_files.sort(key=lambda x: x.content_length, reverse=reverse)
        elif sort_by == "records":
            metadata.data_files.sort(key=lambda x: x.record_count, reverse=reverse)
    
    # Calculate totals for summary
    total_size = sum(file.content_length for file in metadata.data_files) if metadata.data_files else 0
    total_records = sum(file.record_count for file in metadata.data_files) if metadata.data_files else 0
    
    return templates.TemplateResponse(
        "table_details.html",
        {
            "request": request,
            "metadata": metadata,
            "active_page": "catalog",
            "sort_by": sort_by,
            "order": order,
            "total_size": total_size,
            "total_records": total_records,
            "upload_result": upload_result
        }
    )

@app.get("/catalog/table/{table_name}/file/{file_path:path}", response_class=HTMLResponse)
async def view_parquet_file(
    request: Request,
    table_name: str,
    file_path: str,
    page: int = 1,
    rows_per_page: int = 100
):
    # Get table metadata to verify the file belongs to this table
    metadata = pond_client.get_table_metadata(table_name)
    if metadata.error:
        return templates.TemplateResponse(
            "table_details.html",
            {
                "request": request,
                "metadata": metadata,
                "active_page": "catalog",
                "error": f"Error loading table metadata: {metadata.error}"
            }
        )
    
    # Find the file in the table's data files
    file_info = None
    for df in metadata.data_files:
        if df.path == file_path:
            file_info = df
            break
    
    if not file_info:
        return templates.TemplateResponse(
            "table_details.html",
            {
                "request": request,
                "metadata": metadata,
                "active_page": "catalog",
                "error": f"File {file_path} not found in table {table_name}"
            }
        )
    
    # Calculate pagination
    start_row = (page - 1) * rows_per_page
    
    # Read the parquet file content with pagination
    rows, total_rows, error = pond_client.read_parquet_file(
        file_path=file_path,
        batch_size=rows_per_page,
        start_row=start_row,
        num_rows=rows_per_page
    )
    
    if error:
        return templates.TemplateResponse(
            "table_details.html",
            {
                "request": request,
                "metadata": metadata,
                "active_page": "catalog",
                "error": f"Error reading parquet file: {error}"
            }
        )
    
    # Calculate pagination info
    total_pages = (total_rows + rows_per_page - 1) // rows_per_page if total_rows > 0 else 1
    
    # Get column names from the first row
    columns = []
    if rows:
        columns = list(rows[0].keys())
    
    return templates.TemplateResponse(
        "parquet_viewer.html",
        {
            "request": request,
            "active_page": "catalog",
            "table_name": table_name,
            "file_path": file_path,
            "file_info": file_info,
            "rows": rows,
            "columns": columns,
            "current_page": page,
            "total_pages": total_pages,
            "total_rows": total_rows,
            "rows_per_page": rows_per_page
        }
    )

@app.post("/catalog/table/{table_name}/update-partition-spec")
async def update_partition_spec(request: Request, table_name: str):
    form = await request.form()
    
    try:
        # Get partition information from form
        columns = form.getlist("columns[]")
        transforms = form.getlist("transforms[]")
        transform_params = form.getlist("transform_params[]")
        
        # Get table metadata to get column indices
        metadata = pond_client.get_table_metadata(table_name)
        if metadata.error:
            raise ValueError(f"Error getting table metadata: {metadata.error}")
        
        # Build partition spec
        partition_spec = []
        for i, col in enumerate(columns):
            if not col:  # Skip empty selections
                continue
                
            # Find the source_id (index) of the column in the schema
            source_id = next(
                (idx for idx, c in enumerate(metadata.columns) if c.name == col),
                None
            )
            if source_id is None:
                raise ValueError(f"Partition column {col} not found in schema")
                
            transform = transforms[i].upper()
            field = {
                "source_id": source_id,
                "field_id": i,  # Use index as field_id
                "name": col,
                "transform": transform,
            }
            
            # Add transform_param for BUCKET and TRUNCATE
            if transform in ["BUCKET", "TRUNCATE"]:
                param = transform_params[i]
                if param and param.isdigit() and int(param) > 0:
                    field["transform_param"] = int(param)
                else:
                    raise ValueError(f"Invalid transform parameter for {transform}")
            
            partition_spec.append(field)
        
        # Update partition spec
        success, error = pond_client.update_partition_spec(table_name, partition_spec)
        if not success:
            raise ValueError(f"Failed to update partition spec: {error}")
        
        return RedirectResponse(url=f"/catalog/table/{table_name}", status_code=303)
    except Exception as e:
        # Get updated metadata
        metadata = pond_client.get_table_metadata(table_name)
        
        # Calculate totals for summary
        total_size = sum(file.content_length for file in metadata.data_files) if metadata.data_files else 0
        total_records = sum(file.record_count for file in metadata.data_files) if metadata.data_files else 0
        
        return templates.TemplateResponse(
            "table_details.html",
            {
                "request": request,
                "metadata": metadata,
                "active_page": "catalog",
                "total_size": total_size,
                "total_records": total_records,
                "upload_result": {
                    "success": False,
                    "message": f"Error updating partition spec: {str(e)}"
                }
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
    
    print(f"Starting web server on {args.host}:{port} (debug mode)")
    uvicorn.run(
        "main:app",
        host=args.host,
        port=port,
        reload=True,               # Auto-reload on code changes
        log_level="debug",         # Show detailed logs
        proxy_headers=True,        # For better error reporting
        server_header=False        # Disable default server header
    ) 