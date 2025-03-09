from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import grpc
import os
import sys
from typing import Optional, List
from dataclasses import dataclass

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

    def list_files(self, path: str = "", recursive: bool = False) -> tuple[List[str], Optional[str]]:
        try:
            request = pb2.ListFilesRequest(path=path, recursive=recursive)
            response = self.stub.ListFiles(request)
            
            if response.success:
                return list(response.file_paths), None
            return [], response.error
        except Exception as e:
            return [], str(e)
            
    def get_directory_info(self, path: str) -> DirectoryInfo:
        try:
            request = pb2.DirectoryInfoRequest(path=path)
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

# Create a global client instance
pond_client = PondClient(GRPC_HOST, GRPC_PORT)

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "result": None}
    )

@app.post("/get", response_class=HTMLResponse)
async def get_value(request: Request, key: str = Form(...)):
    found, value, error = pond_client.get(key)
    if found:
        result = f"Value for key '{key}': {value}"
    else:
        result = f"Error: {error}"
    
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "result": result}
    )

@app.post("/put", response_class=HTMLResponse)
async def put_value(request: Request, key: str = Form(...), value: str = Form(...)):
    success, error = pond_client.put(key, value)
    if success:
        result = f"Successfully stored key: {key}"
    else:
        result = f"Error: {error}"
    
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "result": result}
    )

@app.post("/delete", response_class=HTMLResponse)
async def delete_value(request: Request, key: str = Form(...)):
    success, error = pond_client.delete(key)
    if success:
        result = f"Successfully deleted key: {key}"
    else:
        result = f"Error: {error}"
    
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "result": result}
    )

@app.post("/list", response_class=HTMLResponse)
async def list_keys(
    request: Request,
    start_key: str = Form(""),
    end_key: str = Form(""),
    limit: Optional[int] = Form(None)
):
    keys, error = pond_client.scan(start_key, end_key, limit or 0)
    if error:
        result = f"Error: {error}"
    else:
        result = f"Found {len(keys)} keys"
    
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "result": result,
            "keys": keys
        }
    )

@app.get("/files", response_class=HTMLResponse)
async def file_explorer(request: Request, path: str = ""):
    files, error = pond_client.list_files(path)
    dir_info = pond_client.get_directory_info(path)
    
    result = None
    if error:
        result = f"Error listing files: {error}"
    elif dir_info.error:
        result = f"Error getting directory info: {dir_info.error}"
    
    return templates.TemplateResponse(
        "files.html",
        {
            "request": request,
            "path": path,
            "files": files,
            "dir_info": dir_info,
            "result": result
        }
    )

@app.post("/files/list", response_class=HTMLResponse)
async def list_files(
    request: Request,
    path: str = Form(""),
    recursive: bool = Form(False)
):
    files, error = pond_client.list_files(path, recursive)
    dir_info = pond_client.get_directory_info(path)
    
    if error:
        result = f"Error: {error}"
    else:
        result = f"Found {len(files)} files/directories"
    
    return templates.TemplateResponse(
        "files.html",
        {
            "request": request,
            "path": path,
            "files": files,
            "dir_info": dir_info,
            "result": result
        }
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=80) 