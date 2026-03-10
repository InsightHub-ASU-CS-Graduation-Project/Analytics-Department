import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

class DataFetcher:
    def __init__(self):
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


    def fetch_and_save_data(self):
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

        for endpoint in self.endpoints_without_pages + self.endpoints_with_pages:
            result = None

            if endpoint in self.endpoints_without_pages:
                result = self.__fetch_data_without_pages(endpoint)

            elif endpoint in self.endpoints_with_pages:
                result = self.__fetch_data_with_pages(endpoint)

            json_string = json.dumps(result) 
            with open(f"Raw Data/{endpoint}.json", "w") as dt:
                dt.write(json_string)


    def __fetch_data_without_pages(self, endpoint: str) -> list:
        result = []

        for country in self.countries:
            url = f"https://api.adzuna.com/v1/api/jobs/{country}/{endpoint}"

            response = requests.get(url, params = self.params, timeout = 30)

            if response.status_code == 200:
                data = response.json()

                result += data.get(self.keys[endpoint], [])
            
            else:
                print(f"Error: {response.status_code}")

        return result


    def __fetch_data_with_pages(self, endpoint: str) -> list:
        result = []

        for country in self.countries: # "Search" Loop
            page = 1

            while True:
                
                url = f"https://api.adzuna.com/v1/api/jobs/{country}/{endpoint}/{page}"

                response = requests.get(url, params = self.params, timeout = 30)

                if response.status_code == 200:
                    data = response.json()
                    page_result = data.get(self.keys[endpoint], [])

                    if not page_result:
                        break

                    result += page_result
                    page += 1

                else:
                    print(f"Error: {response.status_code}")
                    break
            
        return result