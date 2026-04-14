from typing import Any
from fastapi import APIRouter
from pydantic import BaseModel, Field

from Services import PageBuilder



class PageRequest(BaseModel):
    """
    Request payload for dynamic page endpoints.

    Notes:
        This model carries common query controls used by analyzer-backed page
        widgets.
    """

    filters: dict[str, Any] = Field(default_factory = dict)
    rules: list[dict[str, Any]] = Field(default_factory = list)
    top_n: int | None = None
    orient: str = 'records'
    frontend_kwargs: dict[str, Any] = Field(default_factory = dict)


class DynamicPageRouter:
    """
    Create a dynamic API route that builds page data from a shared builder.
    """

    def __init__(self, path: str, builder: PageBuilder, methods: list[str] | None = None):
        """
        Initialize router configuration and register the API endpoint.

        Args:
            path (str): Endpoint path relative to the mounted router prefix.
            builder (PageBuilder): Builder instance used to generate response
                payloads.
            methods (list[str] | None): Allowed HTTP methods. Defaults to
                `["POST"]` when omitted.

        Returns:
            None
        """
        self.router = APIRouter()
        self.builder = builder

        route_methods = methods or ["POST"]

        self.router.add_api_route(path, self.handle_request, methods = route_methods)

    async def handle_request(self, request: PageRequest | None = None) -> dict[str, Any]:
        """
        Handle an incoming page request and return a standard response envelope.

        Args:
            request (PageRequest | None): Parsed request body. If omitted, a
                default empty `PageRequest` is used.

        Returns:
            dict[str, Any]: Response object containing status and built page
            data.

        Example:
        ```
            {
                "status": "success",
                "data": {...}
            }
        ```
        """
        actual_request = request or PageRequest()

        data = self.builder.build(actual_request)
        
        return {"status": "success", "data": data}
