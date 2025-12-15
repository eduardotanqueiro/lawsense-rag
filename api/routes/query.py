from fastapi import APIRouter, HTTPException
from api.schemas.query import QueryRequest, QueryResponse

from api.models.llm_loader import load_llm_agent


router = APIRouter(prefix="/query", tags=["Query"])

@router.post("", response_model=QueryResponse)
async def query_endpoint(request: QueryRequest):


    agent = load_llm_agent()

    msgs = []

    for event in agent.stream(
        {"messages": [{"role": "user", "content": request.query}]},
        stream_mode="values",
    ):
        msgs.append(event)
        event["messages"][-1].pretty_print()


    # Get the response to the user
    # print(f"------------- MSG FINAL: ---------------\n{msgs[-1]['messages'][-1].content}")

    response = QueryResponse(
        response=msgs[-1]["messages"][-1].content,
        retrieved_chunks=[],
    )

    return response