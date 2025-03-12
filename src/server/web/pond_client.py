import grpc
from typing import List, Dict
from server.web import pond_pb2, pond_pb2_grpc

class PondClient:
    def __init__(self, host: str = "localhost", port: int = 50051):
        self.channel = grpc.insecure_channel(f"{host}:{port}")
        self.stub = pond_pb2_grpc.PondServiceStub(self.channel)
        
    async def execute_sql(self, sql: str) -> None:
        """Execute a SQL statement."""
        request = pond_pb2.ExecuteSQLRequest(sql=sql)
        try:
            await self.stub.ExecuteSQL(request)
        except grpc.RpcError as e:
            raise Exception(f"Failed to execute SQL: {e.details()}")
            
    async def list_tables(self) -> List[str]:
        """List all tables in the catalog."""
        request = pond_pb2.ListTablesRequest()
        try:
            response = await self.stub.ListTables(request)
            return response.table_names
        except grpc.RpcError as e:
            raise Exception(f"Failed to list tables: {e.details()}")
            
    async def update_partition_spec(self, table_name: str, partition_spec: List[Dict]) -> None:
        """Update the partition specification for a table."""
        # Convert the partition spec to protobuf format
        pb_partition_fields = []
        for field in partition_spec:
            pb_field = pond_pb2.PartitionField(
                source_id=field["source_id"],
                field_id=field["field_id"],
                name=field["name"],
                transform=field["transform"]
            )
            if "transform_param" in field:
                pb_field.transform_param = field["transform_param"]
            pb_partition_fields.append(pb_field)
            
        request = pond_pb2.UpdatePartitionSpecRequest(
            table_name=table_name,
            partition_spec=pond_pb2.PartitionSpec(fields=pb_partition_fields)
        )
        
        try:
            await self.stub.UpdatePartitionSpec(request)
        except grpc.RpcError as e:
            raise Exception(f"Failed to update partition spec: {e.details()}") 