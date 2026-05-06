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
        params: dict | None = None,
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
            params (dict | None): Query parameters sent with each weight
                request. Defaults to None.
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
        params = params.copy() if params else {}
        if isinstance(targets, str):
            targets = [targets]

        sizes = {}
        total_count = 0

        for target in targets:
            dynamic_endpoint = endpoint_template.format(target = target)

            data = self.get(dynamic_endpoint, params = params, **kwargs)

            count = data.get(extraction_key, 0) if isinstance(data, dict) else 0

            sizes[target] = count

            total_count += count


            time.sleep(delay_between_requests)

        weights = {}
        if total_count > 0:
            for target, count in sizes.items():
                weights[target] = round((count / total_count) * 100, 2)

        return dict(sorted(weights.items(), key = lambda item: item[1], reverse = True))


    def __prepare_chunks(
            self,
            keywords: list,
            seperator: str = "",
            value_container: str | None = None,
            max_chars: int = 1500
        ) -> list[str]:
        """
        Divide a list of keywords into string chunks, each within a character limit.

        Args:
            keywords (list): Raw keyword values to serialize into chunks.
            seperator (str): Separator inserted between chunked keyword values.
                Defaults to an empty string.
            value_container (str | None): Optional even-length wrapper split
                around each value, such as `"()"`, `"[]"`, or `"\"\""`.
            max_chars (int): Maximum character length for each chunk. Defaults
                to 1500.

        Returns:
            list[str]: Chunked keyword strings ready to be sent as request
            parameter values.

        Raises:
            ValueError: If `value_container` has an odd length or `max_chars`
            is not positive.

        Notes:
            If a single keyword exceeds `max_chars`, it is kept as one chunk
            because splitting inside a keyword would change the request value.
        """
        if value_container and len(value_container) % 2 != 0:
            raise ValueError("value_container must have an even length (e.g., '()', '[]', '\"\"'). Single characters are not allowed.")

        if max_chars <= 0:
            raise ValueError("max_chars must be a positive integer.")

        chunks = []
        current_chunk = []
        current_len = 0

        seperator_len = len(seperator)
        container_len = len(value_container) if value_container else 0


        left_wrap = value_container[0: container_len // 2] if value_container else ""
        right_wrap = value_container[container_len // 2: container_len] if value_container else ""

        for kw in keywords:
            formatted_kw = f"{left_wrap}{kw}{right_wrap}" if value_container else str(kw)
            addition_len = len(formatted_kw) if not current_chunk else len(formatted_kw) + seperator_len

            if current_chunk and current_len + addition_len > max_chars:
                chunks.append(seperator.join(current_chunk))

                current_chunk = [formatted_kw]
                current_len = len(formatted_kw)

            else:
                current_chunk.append(formatted_kw)

                current_len += addition_len

        if current_chunk:
            chunks.append(seperator.join(current_chunk))

        return chunks


    def __fetch_paginated(
        self,
        endpoint_template: str,
        params: dict | None = None,
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
        Fetch one or more paginated API endpoints and aggregate their results.

        Args:
            endpoint_template (str): Endpoint template. It may contain `{page}`
                for page numbers and `{target}` for distributed target values.
            params (dict | None): Query parameters copied into each request.
                Defaults to None.
            page_sequence (Iterable | None): Explicit page numbers to request.
            extraction_key (str | None): Response key containing page records.
                When omitted, the full response is aggregated.
            delay_between_requests (float): Delay between page requests in
                seconds. Defaults to 0.5.
            targets (Iterable[str] | None): Optional target values such as
                country codes.
            weight_endpoint_template (str | None): Optional target endpoint
                used to estimate how many pages each target should receive.
            total_pages_budget (int | None): Total pages to distribute across
                weighted targets.
            count_extraction_key (str): Key used when reading weighting counts.
                Defaults to `"count"`.
            **kwargs: Extra keyword arguments forwarded to `self.get`.

        Returns:
            list: Aggregated records extracted from all requested pages.

        Notes:
            Pagination stops early for a target when an empty response or empty
            extracted page is returned.
        """
        params = params.copy() if params else {}
        if isinstance(targets, str):
            targets = [targets]
        elif targets is not None:
            targets = list(targets)

        if page_sequence is not None and not isinstance(page_sequence, range):
            page_sequence = list(page_sequence)

        all_results = []
        fetch_plan = []

        if targets is not None:
            if weight_endpoint_template is not None and total_pages_budget is not None:
                weights = self.__calculate_endpoints_weights(
                    endpoint_template = weight_endpoint_template,
                    targets = targets,
                    extraction_key = count_extraction_key,
                    params = params,
                    **kwargs
                )

                for target, weight in weights.items():
                    allocated_pages = max(1, int((weight / 100) * total_pages_budget))
                    target_endpoint = endpoint_template.replace("{target}", str(target))
                    fetch_plan.append((target_endpoint, range(1, allocated_pages + 1)))

                    print(f"Plan added: Target '{target}' -> {allocated_pages} pages.")

            elif page_sequence is not None:
                for target in targets:
                    target_endpoint = endpoint_template.replace("{target}", str(target))

                    fetch_plan.append((target_endpoint, page_sequence))

                try:
                    num_pages = len(page_sequence)
                except TypeError:
                    num_pages = "an unknown number of"

                print(f"[FETCH PLAN] Added {len(targets)} targets. Fetching {num_pages} pages per target. Expected max requests: {len(targets) * (num_pages if isinstance(num_pages, int) else 0) or 'Unknown'}.")

            else:
                raise ValueError("Missing Logic: When using 'targets', you must provide either 'page_sequence' OR ('weight_endpoint_template' & 'total_pages_budget').")

        else:
            if page_sequence is not None:
                fetch_plan.append((endpoint_template, page_sequence))
            else:
                raise ValueError("Missing Logic: When not using 'targets', you must provide a 'page_sequence'.")

        for current_endpoint, pages in fetch_plan:
            for page in pages:
                dynamic_endpoint = current_endpoint.format(page = page)

                data = self.get(dynamic_endpoint, params = params, **kwargs)

                if not data:
                    print(f"Stopping pagination on {dynamic_endpoint} due to empty data/error.")
                    break

                page_data = data.get(extraction_key, []) if extraction_key else data

                if not page_data:
                    print(f"No data found at {dynamic_endpoint} for page {page}. Ending pagination.")
                    break

                if isinstance(page_data, list):
                    all_results.extend(page_data)

                else:
                    all_results.append(page_data)

                if delay_between_requests:
                    time.sleep(delay_between_requests)

        return all_results


    def request(
        self,
        method: str,
        url: str,
        params: dict | None = None,
        is_paginated: bool = False,
        timeout: float | None = 30.0,
        page_sequence: Iterable | None = None,
        extraction_key: str | None = None,
        delay_between_requests: float = 0.5,
        targets: Iterable[str] | None = None,
        weight_endpoint_template: str | None = None,
        total_pages_budget: int | None = None,
        count_extraction_key: str = "count",
        chunk_keywords: list | None = None,
        chunk_param_key: str | None = None,
        chunk_seperator: str = ", ",
        chunk_value_container: str | None = None,
        chunk_max_chars: int = 1500,
        *args,
        **kwargs
    ) -> Union[dict, list]:
        """
        Execute a single API request, a paginated request flow, or a chunked
        request sequence.

        Args:
            method (str): HTTP method, such as `"GET"` or `"POST"`.
            url (str): Relative or absolute endpoint URL.
            params (dict | None): Query parameters sent with the request.
            is_paginated (bool): Whether the request should use the pagination
                helper. Defaults to False.
            timeout (float | None): Per-request timeout in seconds. Defaults to
                30.0.
            page_sequence (Iterable | None): Explicit page numbers for standard
                pagination.
            extraction_key (str | None): Response key used when extracting page
                data.
            delay_between_requests (float): Delay between paginated requests.
            targets (Iterable[str] | None): Target identifiers for distributed
                pagination.
            weight_endpoint_template (str | None): Endpoint template used to
                compute target weights.
            total_pages_budget (int | None): Total page budget for distributed
                pagination.
            count_extraction_key (str): Key used when reading count values for
                distributed weights. Defaults to `"count"`.
            chunk_keywords (list | None): Optional keywords to split into
                multiple chunked requests.
            chunk_param_key (str | None): Query parameter name that receives
                each keyword chunk.
            chunk_seperator (str): Separator inserted between chunked keywords.
                Defaults to `", "`.
            chunk_value_container (str | None): Optional even-length wrapper
                applied around every keyword before chunking, such as `"()"`.
            chunk_max_chars (int): Maximum length for each keyword chunk.
                Defaults to 1500.
            *args: Additional positional arguments forwarded to the underlying
                request call.
            **kwargs: Additional keyword arguments forwarded to the underlying
                request call.

        Returns:
            dict | list: Parsed JSON response for standard requests, or an
            aggregated list for paginated/chunked requests.

        Raises:
            ValueError: If pagination or chunked-request parameters are
            incomplete, conflicting, or incorrectly configured.
        """
        params = params.copy() if params else {}
        kwargs.pop('params', None)

        has_weights = weight_endpoint_template is not None or total_pages_budget is not None

        if (chunk_keywords is None) != (chunk_param_key is None):
            raise ValueError("Chunked requests require both 'chunk_keywords' and 'chunk_param_key'.")

        if chunk_param_key is not None:
            chunk_param_key = str(chunk_param_key).strip()
            if not chunk_param_key:
                raise ValueError("'chunk_param_key' must be a non-empty string.")

        if is_paginated:
            if has_weights:
                if targets is None or weight_endpoint_template is None or total_pages_budget is None:
                    raise ValueError("Missing Parameters: Distributed pagination requires 'targets', 'weight_endpoint_template', AND 'total_pages_budget'.")

                if page_sequence is not None:
                    raise ValueError("Conflict Error: Cannot mix 'page_sequence' with distributed weight parameters.")
            else:
                if page_sequence is None:
                    raise ValueError("Missing Parameters: You must provide a 'page_sequence' for standard pagination.")
        else:
            if any(v is not None for v in [page_sequence, targets, weight_endpoint_template, total_pages_budget]):
                raise ValueError("Configuration Error: Pagination parameters provided, but 'is_paginated' is False.")

        if chunk_keywords is not None and chunk_param_key is not None:
            chunks = self.__prepare_chunks(keywords = chunk_keywords, seperator = chunk_seperator, max_chars = chunk_max_chars, value_container = chunk_value_container)
            if not chunks:
                return []

            all_chunked_results = []

            for chunk in chunks:
                chunk_params = params.copy()
                chunk_params[chunk_param_key] = chunk

                chunk_result = self.request(
                    method = method,
                    url = url,
                    params = chunk_params,
                    is_paginated = is_paginated,
                    timeout = timeout,
                    page_sequence = page_sequence,
                    extraction_key = extraction_key,
                    delay_between_requests = delay_between_requests,
                    targets = targets,
                    weight_endpoint_template = weight_endpoint_template,
                    total_pages_budget = total_pages_budget,
                    count_extraction_key = count_extraction_key,
                    chunk_keywords = None,
                    chunk_param_key = None,
                    *args, **kwargs
                )

                if isinstance(chunk_result, list):
                    all_chunked_results.extend(chunk_result)

                elif isinstance(chunk_result, dict) and extraction_key and extraction_key in chunk_result:
                    extracted_data = chunk_result[extraction_key]

                    if isinstance(extracted_data, list):
                        all_chunked_results.extend(extracted_data)
                    else:
                        all_chunked_results.append(extracted_data)

            return all_chunked_results

        if is_paginated:
            return self.__fetch_paginated(
                endpoint_template = url,
                page_sequence = page_sequence,
                extraction_key = extraction_key,
                delay_between_requests = delay_between_requests,
                targets = targets,
                weight_endpoint_template = weight_endpoint_template,
                total_pages_budget = total_pages_budget,
                count_extraction_key = count_extraction_key,
                params = params,
                **kwargs
            )

        full_url = f"{self.base_url}/{url.lstrip('/')}" if self.base_url else url

        try:
            response = super().request(method, full_url, params = params, timeout = timeout, *args, **kwargs)
            response.raise_for_status()

            return response.json()

        except ValueError as e:
            print(f"Error decoding JSON from {full_url}: {e}")

            return {}

        except requests.exceptions.RequestException as e:
            print(f"Error fetching {full_url}: {e}")

            return {}
