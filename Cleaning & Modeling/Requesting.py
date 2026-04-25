import requests
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Iterable, Collection, Union


class BaseAPIClient(requests.Session):
    """
    A robust API client inheriting directly from requests.Session.
    Provides built-in connection pooling, auto-retries, and base URL resolution.
    """
    def __init__(
            self,
            base_url: str = "",
            total_retries: bool | int | None = 4,
            backoff_factor: float = 2,
            status_forcelist: Collection[int] | None = (429, 500, 502, 503, 504)
        ) -> None:
        """
        Initialize the API session with retry-aware HTTP adapters.

        Args:
            base_url (str): Optional base URL prepended to relative endpoints.
            total_retries (bool | int | None): Retry budget passed to
                `urllib3.Retry`. Defaults to 4.
            backoff_factor (float): Exponential backoff factor between retries.
                Defaults to 2.
            status_forcelist (Collection[int] | None): HTTP status codes that
                should trigger retries. Defaults to `(429, 500, 502, 503, 504)`.

        Returns:
            None
        """
        super().__init__()
        self.base_url = base_url.rstrip('/')

        retry_strategy = Retry(
            total = total_retries,
            backoff_factor = backoff_factor,
            status_forcelist = status_forcelist,
        )
        adapter = HTTPAdapter(max_retries = retry_strategy)
        
        self.mount("http://", adapter)
        self.mount("https://", adapter)


    def __calculate_endpoints_weights(
        self, 
        endpoint_template: str, 
        targets: Iterable[str], 
        extraction_key: str = "count",
        delay_between_requests: float = 0.5,
        **kwargs
    ) -> dict[str, float]:
        """
        Hit multiple endpoints to extract a numeric value and calculate their
        percentage weights.

        Args:
            endpoint_template (str): Endpoint template containing `{target}`.
            targets (Iterable[str]): Target identifiers such as country codes.
            extraction_key (str): Key used to extract the numeric value from the
                response body. Defaults to `"count"`.
            delay_between_requests (float): Delay between weight requests in
                seconds. Defaults to 0.5.
            **kwargs: Extra request parameters forwarded to `self.get`.

        Returns:
            dict[str, float]: Mapping of target to percentage weight.

        Example:
        ```
            client._BaseAPIClient__calculate_endpoints_weights(
                endpoint_template="{target}/search/1",
                targets=["gb", "us"]
            )
        ```
        """
        sizes = {}
        total_count = 0
        
        for target in targets:
            dynamic_endpoint = endpoint_template.format(target = target)
            
            data = self.get(dynamic_endpoint, **kwargs)

            count = data.get(extraction_key, 0) if isinstance(data, dict) else 0

            sizes[target] = count
            total_count += count
            
                
            time.sleep(delay_between_requests)
            
        weights = {}
        if total_count > 0:
            for target, count in sizes.items():
                weights[target] = round((count / total_count) * 100, 2)
                
        return dict(sorted(weights.items(), key = lambda item: item[1], reverse = True))


    def __fetch_paginated(
        self, 
        endpoint_template: str, 
        page_sequence: Iterable | None = None, 
        extraction_key: str | None = None,
        delay_between_requests: float = 0.5,
        targets: Iterable[str] | None = None,
        weight_endpoint_template: str | None = None,
        total_pages_budget: int | None = None,
        count_extraction_key: str = "count",
        **kwargs
    ) -> list:
        """
        Fetch paginated resources using either an explicit page sequence or a
        distributed page budget across targets.

        Args:
            endpoint_template (str): Endpoint template containing `{page}` and
                optionally `{target}`.
            page_sequence (Iterable | None): Explicit page numbers to fetch.
            extraction_key (str | None): Response key used to extract page data.
            delay_between_requests (float): Delay between requests in seconds.
            targets (Iterable[str] | None): Target identifiers for distributed
                pagination.
            weight_endpoint_template (str | None): Endpoint template used to
                estimate target weights.
            total_pages_budget (int | None): Total page budget to distribute
                across targets.
            count_extraction_key (str): Response key used when reading target
                weights. Defaults to `"count"`.
            **kwargs: Extra request parameters forwarded to `self.get`.

        Returns:
            list: Aggregated results collected from all fetched pages.
        """
        all_results = []
        
        fetch_plan = []

        if targets is not None and weight_endpoint_template is not None and total_pages_budget is not None:
            weights = self.__calculate_endpoints_weights(
                endpoint_template = weight_endpoint_template,
                targets = targets,
                extraction_key = count_extraction_key,
                **kwargs
            )

            for target, weight in weights.items():
                allocated_pages = max(1, int((weight / 100) * total_pages_budget))
                target_endpoint = endpoint_template.replace("{target}", str(target))
                
                fetch_plan.append((target_endpoint, range(1, allocated_pages + 1)))
                
                print(f"Plan added: Target '{target}' -> {allocated_pages} pages.")
        
        elif page_sequence is not None:
            fetch_plan.append((endpoint_template, page_sequence))
            
        else:
            raise ValueError("You must provide either 'page_sequence' or 'targets + total_pages_budget'.")
        
        for current_endpoint, pages in fetch_plan:
            for page in pages:
                dynamic_endpoint = current_endpoint.format(page = page)
                
                data = self.get(dynamic_endpoint, **kwargs)
                
                if not data:
                    print(f"Stopping pagination on {dynamic_endpoint} due to empty data/error.")
                    break
                    
                page_data = data.get(extraction_key, []) if extraction_key else data
                
                if not page_data:
                    break
                    
                all_results += page_data

                if delay_between_requests:
                    time.sleep(delay_between_requests)
                
        return all_results
    

    def request(
        self, 
        method: str, 
        url: str, 
        is_paginated: bool = False, 
        timeout: float | None = 30.0,
        page_sequence: Iterable | None = None, 
        extraction_key: str | None = None,
        delay_between_requests: float = 0.5,
        targets: Iterable[str] | None = None,
        weight_endpoint_template: str | None = None,
        total_pages_budget: int | None = None,
        count_extraction_key: str = "count",
        *args, 
        **kwargs
    ) -> Union[dict, list]:
        """
        Override the default session request method to support relative URLs,
        custom timeouts, and seamless routing to paginated fetching.

        Args:
            method (str): HTTP method, such as `"GET"` or `"POST"`.
            url (str): Relative or absolute endpoint URL.
            is_paginated (bool): Whether the request should be routed through
                the pagination helper. Defaults to False.
            timeout (float | None): Per-request timeout in seconds. Defaults to
                30.0.
            page_sequence (Iterable | None): Explicit page numbers for standard
                pagination.
            extraction_key (str | None): Response key used to extract page data.
            delay_between_requests (float): Delay between paginated requests.
            targets (Iterable[str] | None): Target identifiers for distributed
                pagination.
            weight_endpoint_template (str | None): Endpoint template used to
                compute target weights.
            total_pages_budget (int | None): Total page budget for distributed
                pagination.
            count_extraction_key (str): Key used when reading count values for
                weights. Defaults to `"count"`.
            *args: Additional positional arguments forwarded to the parent
                request implementation.
            **kwargs: Additional keyword arguments forwarded to the parent
                request implementation.

        Returns:
            dict | list: Parsed JSON response for standard requests, or a merged
            list for paginated requests.
        """
        has_distributed_pagination = any(
            value is not None
            for value in [targets, weight_endpoint_template, total_pages_budget]
        )

        if is_paginated:
            if page_sequence is not None and has_distributed_pagination:
                raise ValueError(
                    "Conflict Error: You cannot mix standard pagination ('page_sequence') "
                    "with distributed pagination ('targets', 'budget'). Please pick one track."
                )

            if has_distributed_pagination:
                if targets is None or weight_endpoint_template is None or total_pages_budget is None:
                    raise ValueError(
                        "Missing Parameters: Distributed Pagination requires 'targets', "
                        "'weight_endpoint_template', AND 'total_pages_budget' together."
                    )
            
            elif page_sequence is None:
                raise ValueError(
                    "Missing Parameters: For paginated requests, you must provide either "
                    "'page_sequence' OR distributed parameters ('targets', 'budget')."
                )

            return self.__fetch_paginated(
                endpoint_template = url,
                page_sequence = page_sequence,
                extraction_key = extraction_key,
                delay_between_requests = delay_between_requests,
                targets = targets,
                weight_endpoint_template = weight_endpoint_template,
                total_pages_budget = total_pages_budget,
                count_extraction_key = count_extraction_key,
                **kwargs
            )
            
        else:
            if page_sequence is not None or has_distributed_pagination:
                raise ValueError(
                    "Configuration Error: You provided pagination parameters (like 'page_sequence' or 'targets'), "
                    "but 'is_paginated' is set to False. Did you forget to pass 'is_paginated=True'?"
                )

            full_url = f"{self.base_url}/{url.lstrip('/')}" if self.base_url else url


            try:
                response = super().request(method, full_url, timeout = timeout, *args, **kwargs)
                response.raise_for_status()
                
                return response.json()

            except ValueError as e:
                print(f"Error decoding JSON from {full_url}: {e}")
                
                return {}
                
            except requests.exceptions.RequestException as e:
                print(f"Error fetching {full_url}: {e}")
               
                return {}
