import logging
from typing import Any, Callable



logger = logging.getLogger(__name__)


class PageBuilder:
    """
    Build a page response by executing a dictionary of section resolvers.

    Notes:
        Each entry in `page_config` maps a response key to a callable that
        accepts the same request payload.
    """

    def __init__(self, page_config: dict[str, Callable[[Any], Any]]):
        """
        Initialize the builder with page resolver functions.

        Args:
            page_config (dict[str, Callable[[Any], Any]]): Mapping of response
                keys to callables that generate each page section.

        Returns:
            None
        """
        self.page_config = page_config

    def build(self, request_data: Any) -> dict[str, Any]:
        """
        Execute all configured resolvers and return the combined page payload.

        Notes:
            Resolver failures are isolated per section so one failing widget does
            not break the whole page response.

        Args:
            request_data (Any): Input payload passed to each section resolver.

        Returns:
            dict[str, Any]: Aggregated page result keyed by section name.

        Example:
        ```
            builder = PageBuilder({"kpi": lambda req: {"value": 10}})
            payload = builder.build(request_data)
        ```
        """
        result: dict[str, Any] = {}

        for key, func in self.page_config.items():
            if not callable(func):
                result[key] = {"error": "Configured value is not callable."}
               
                continue

            try:
                result[key] = func(request_data)
            
            except Exception as e:
                logger.error(f"Widget Resolver failed | Key: {key!r} | Func: {func!r} | Error: {e}")

                result[key] = {"error": "Failed to load this widget.", "key": key}

        return result
