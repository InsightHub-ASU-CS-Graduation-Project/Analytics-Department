from pydantic import BaseModel
from fastapi import APIRouter

from Services import PageBuilder



class PageRequest(BaseModel):
    filters: dict = {}
    rules: list = []
    top_n: int | None = None
    orient: str = 'records'
    frontend_kwargs: dict = {}


class DynamicPageRouter:
    def __init__(self, path: str, builder: PageBuilder):
        self.router = APIRouter()
        self.builder = builder
        
        self.router.add_api_route(path, self.handle_request, methods = ["POST"])

    async def handle_request(self, request: PageRequest) -> dict[str, any]:
        data = self.builder.build(request)
        
        return {"status": "success", "data": data}