from pydantic import BaseModel, Field

class QueryRequest(BaseModel):
    query: str = Field(..., description="The input query string.")
    top_k: int = Field(5, description="Number of top similar chunks to retrieve")
    
class QueryResponse(BaseModel):
    results: list = Field(..., description="List of retrieved chunks with their metadata and distances.")