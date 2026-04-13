import os
import time
import json
import requests
import pandas as pd
from dotenv import load_dotenv

from Caching import CacheManager



load_dotenv(
    dotenv_path = os.path.join(
        os.path.dirname(
            os.path.dirname(
                os.path.abspath(
                    __file__
                )
            )
        ),
        '.env'
    )
)

class DataFetcher:
    """
    Fetch raw datasets from the Adzuna API and maintain helper caches used by
    the ingestion pipeline.

    Notes:
        The class centralizes API credentials, supported country markets,
        endpoint routing, and automated reference-cache generation.
    """

    def __init__(self):
        """
        Initialize API credentials, endpoint metadata, and automated caches.

        Raises:
            ValueError: If either `ADZUNA_API_ID` or `ADZUNA_APP_KEY` is missing
            from the environment.
        """
        api_id = os.getenv("ADZUNA_API_ID")
        api_key = os.getenv("ADZUNA_APP_KEY")

        if not api_id or not api_key:
            raise ValueError("\"ADZUNA_API_ID\" and \"ADZUNA_APP_KEY\" environment variables must be set")

        self.params = {
            "app_id": api_id,
            "app_key": api_key
        }

        self.countries = [
            "gb", "us", "at", "au", "be", "br", "ca", "ch", "de", 
            "es", "fr", "in", "it", "mx", "nl", "nz", "pl", "sg", "za"
        ]

        self.endpoints_without_pages = ["categories", "top_companies", "geodata", "history"] # Without "Histogram"
        self.endpoints_with_pages = ["search"]

        self.keys = {
            "search": "results", "categories": "results", "top_companies": "leaderboard",
            "geodata": "locations", "history": "month"
        }

        self.cache_manager = CacheManager(cache_dir = "Cache Data/Automated")

    
    def __calculate_market_weights(self, endpoint_with_pages: str) -> dict:
        """
        Estimate each country's share of the global market for a paginated
        endpoint.

        Notes:
            The method queries one lightweight page per country, reads the
            endpoint's total `count`, and converts those counts into percentage
            weights. The result is used to distribute page budgets across
            countries when fetching search jobs.

        Args:
            endpoint_with_pages (str): A paginated endpoint name such as
                `"search"`.

        Returns:
            dict: A dictionary mapping country code to percentage weight,
            ordered from largest market to smallest.
        """
        market_sizes = {}
        total_global_jobs = 0
        
        for country in self.countries:
            url = f"https://api.adzuna.com/v1/api/jobs/{country}/{endpoint_with_pages}/1"
            
            params = self.params.copy()
            params.update({"results_per_page": 1})

            while True:
                try:
                    response = requests.get(url, params = params, timeout = 30)
                    
                    if response.status_code == 200:
                        data = response.json()
                        country_total_jobs = data.get("count", 0)
                        
                        market_sizes[country] = country_total_jobs
                        total_global_jobs += country_total_jobs
                        
                        time.sleep(0.5)
                        break

                    elif response.status_code in [503, 504]:
                        print(
                            f"Server overloaded/Gateaway Timeout ({response.status_code}) for {country} in {endpoint_with_pages}.",
                            "Sleeping for 10 seconds before retrying..."
                        )

                        time.sleep(10)
                        continue

                    else:
                        print(f"Failed to get size for {country} (Error: {response.status_code})")
                        market_sizes[country] = 0
                        break

                except requests.exceptions.RequestException as e:
                    print(f"Network Error while scanning {country}: {e}. Retrying in 15s...")
                    time.sleep(15)
                    continue
                
        country_weights = {}
        
        if total_global_jobs > 0:
            for country, count in market_sizes.items():
                percentage = (count / total_global_jobs) * 100
                country_weights[country] = round(percentage, 2)
                
        country_weights = dict(sorted(country_weights.items(), key=lambda item: item[1], reverse=True))
            
        return country_weights


    def fetch_and_save_data(self, total_target_jobs: int) -> None:
        """
        Fetch data from all endpoints and save each to a corresponding JSON file.

        Note:
            The method expects a directory named 'Raw Data' to exist.
        
        Example:
        ```
            If endpoint is 'search'
            Saves to: 'Raw Data/search.json'
        ``` 
        """

        raw_data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Raw Data")
        os.makedirs(raw_data_dir, exist_ok = True)

        for endpoint in self.endpoints_without_pages + self.endpoints_with_pages:
            result = None

            if endpoint in self.endpoints_without_pages:
                result = self.__fetch_data_without_pages(endpoint)

            elif endpoint in self.endpoints_with_pages:
                result = self.__fetch_data_with_pages(endpoint, total_target_jobs)

            json_string = json.dumps(result, ensure_ascii = False, indent = 4) 
            with open(os.path.join(raw_data_dir, f"{endpoint}.json"), "w", encoding = 'utf-8') as dt:
                dt.write(json_string)


    def __fetch_data_without_pages(self, endpoint: str) -> list:
        """
        Fetch all data for endpoints that do not require pagination.

        Notes:
            The method iterates through every configured country, retries
            transient server and network failures, and concatenates the endpoint
            payloads into one list.

        Args:
            endpoint (str): Endpoint name such as `"categories"` or
                `"geodata"`.

        Returns:
            list: Combined records collected from all configured countries.
        """
        result = []

        for country in self.countries:
            url = f"https://api.adzuna.com/v1/api/jobs/{country}/{endpoint}"

            while True:
                try:
                    response = requests.get(url, params = self.params, timeout = 120)

                    if response.status_code == 200:
                        data = response.json()

                        print(f"Data is Fetched Successfully for {country} in {endpoint}")

                        result += data.get(self.keys[endpoint], [])
                        break

                    elif response.status_code in [503, 504]:
                        print(
                            f"Server overloaded/Gateaway Timeout ({response.status_code}) for {country} in {endpoint}.",
                            f"Sleeping for 10 seconds before retrying..."
                        )

                        time.sleep(10)
                        continue
                    
                    else:
                        print(f"Error in {endpoint} for {country}: {response.status_code}")
                        break
                
                except requests.exceptions.RequestException as e:
                        print(f"Network Error while scanning {country}: {e}. Retrying in 15s...")

                        time.sleep(15)
                        continue

        return result


    def __fetch_data_with_pages(self, endpoint: str, target_total_jobs: int) -> list:
        """
        Fetch paginated endpoint data while distributing the page budget across
        markets.

        Notes:
            Page allocation is based on market weights calculated from total job
            counts. Each country is then fetched page by page with retry logic
            for transient failures.

        Args:
            endpoint (str): Paginated endpoint name, currently intended for
                `"search"`.
            target_total_jobs (int): Approximate global number of jobs to target
                before converting that target into page counts.

        Returns:
            list: Combined paginated records collected from all configured
            countries.
        """
        result = []

        market_weights = self.__calculate_market_weights(endpoint)

        total_target_pages = target_total_jobs // 50
        optimal_pages_per_country = {}

        for country, weight in market_weights.items(): # "Search" Loop
            calculated_pages = int((weight / 100) * total_target_pages)

            optimal_pages_per_country[country] = max(1, calculated_pages)
            
        for country in self.countries:
            page = 1
            max_pages = optimal_pages_per_country.get(country, 1)

            with_pages_params = self.params.copy()

            with_pages_params.update({
                "results_per_page": 50,
                "sort_by": "date"
            })

            while page < max_pages + 1:
                url = f"https://api.adzuna.com/v1/api/jobs/{country}/{endpoint}/{page}"


                try:
                    response = requests.get(url, params = with_pages_params, timeout = 30)

                    if response.status_code == 200:
                        data = response.json()
                        page_result = data.get(self.keys[endpoint], [])

                        if not page_result:
                            break
                        
                        result += page_result

                        if page % 10 == 0:
                            print(f"   - Fetched {page * 50} jobs so far for {country} in {endpoint}...")

                        page += 1
                        time.sleep(0.5)

                    elif response.status_code in [503, 504]:
                        print(
                            f"Server overloaded/Gateaway Timeout ({response.status_code}) for {country} in {endpoint}.",
                            f"Sleeping for 10 seconds before retrying page {page}..."
                        )

                        time.sleep(10)
                        continue

                    else:
                        print(f"Error in {endpoint} for {country} at page {page}: {response.status_code}")
                        break
                
                except requests.exceptions.RequestException as e:
                    print(f"Network Error while scanning {country}: {e}. Retrying in 15s...")

                    time.sleep(15)
                    continue
                
        return result
    

    def build_reference_lexicon(self, *source_cols, file_path: str, target_cache_key: str):
        """
        Extract normalized values from one or more file columns and sync them
        into a cache-backed lexicon.

        Notes:
            Source values are lower-cased, stripped, deduplicated, and stored as
            boolean lookup keys. Existing cached content is replaced only when
            the source data changes.

        Args:
            *source_cols: Column names to read from the external file.
            file_path (str): Path to a tabular file such as CSV or Excel.
            target_cache_key (str): Top-level cache key under
                `build_reference_lexicon_cache.json`.

        Returns:
            None

        Example:
        ```
            fetcher.build_reference_lexicon(
                "skill_name",
                "normalized_skill",
                file_path="skills.csv",
                target_cache_key="skills"
            )
        ```
        """
    
        print(f"Extracting data from '{file_path}' (Columns: {source_cols})...")
        
        try:
            ext = os.path.splitext(file_path)[1].lower().replace('.', '')
            
            if ext == 'json':
                print(f"Notice: '{file_path}' is already a JSON file. Exiting extraction...")
                return None
                
            if ext == 'csv':
                try:
                    df = pd.read_csv(file_path, encoding = 'utf-8')
                
                except UnicodeDecodeError:
                    print(f"Encoding fallback: Trying 'cp1252' for '{file_path}'...")
                    
                    df = pd.read_csv(file_path, encoding = 'cp1252')
            
            elif ext in ['xlsx', 'xls']:
                df = pd.read_excel(file_path)
                
            else:
                read_method = getattr(pd, f"read_{ext}", None)
                
                if read_method:
                    df = read_method(file_path)
                
                else:
                    print(f"Error: Pandas does not support reading '.{ext}' directly.")
                    return None
            
            valid_cols = [col for col in source_cols if col in df.columns]
            if not valid_cols:
                print(f"Error: None of the columns {source_cols} exist in the CSV.")
                
                return None

            func_name = "build_reference_lexicon"
            source_cache_dict = self.cache_manager.get_column_cache(func_name, target_cache_key)
            
            cache_updated = False

            for col in valid_cols:
                new_data = df[col].dropna().astype(str).str.strip().str.lower().unique().tolist()

                new_data_set = set(new_data)

                if col not in source_cache_dict:
                    source_cache_dict[col] = {}
                
                col_cache_dict = source_cache_dict[col]
                existing_data_set = set(col_cache_dict.keys())

                if new_data_set != existing_data_set:
                    print(f"Changes detected in column '{col}'. Updating cache object...")

                    col_cache_dict.clear()
                    col_cache_dict.update({item: True for item in new_data_set})

                    cache_updated = True
                
                else:
                    print(f"Column '{col}' is already up-to-date.")

            if cache_updated:
                self.cache_manager.save_function_cache(func_name)
                
                print(f"Success: Data for '{target_cache_key}' synced and saved to {func_name}_cache.json.")
            
            else:
                print(f"All columns in '{target_cache_key}' are up-to-date. No write needed.")

        except FileNotFoundError:
            print(f"Error: Could not find the file '{file_path}'.")
        
        except Exception as e:
            print(f"Error during ingestion: {e}")

        return None