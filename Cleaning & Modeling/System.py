from Libraries import *



class CacheManager:
    """
    Manage lightweight JSON cache files for function-level and column-level
    operations.

    Notes:
        Each function gets its own JSON file inside the configured cache
        directory. Individual methods can then store nested objects keyed by
        column name or another logical cache bucket.
    """

    def __init__(self, cache_dir: str = "Cache Data"):
        """
        Initialize the cache manager and resolve its storage directory.

        Args:
            cache_dir (str): Relative cache directory located beside this module.
                Defaults to `"Cache Data"`.
        """
        self.cache_dir = cache_dir
        self.cache_dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), self.cache_dir)
                                           
        self.caches = {}


    def _get_file_path(self, func_name: str) -> str:
        """
        Build the cache-file path for a specific function name.

        Args:
            func_name (str): The logical function identifier used as the cache
                filename prefix.

        Returns:
            str: Absolute path to the JSON cache file for the requested
            function.
        """
        return os.path.join(self.cache_dir_path, f"{func_name}_cache.json")


    def load_function_cache(self, func_name: str):
        """
        Load a function cache from disk into memory on first access.

        Args:
            func_name (str): Function identifier whose cache file should be
                loaded.

        Returns:
            dict: The in-memory cache object for the requested function. If no
            cache file exists yet, an empty dictionary is created and returned.
        """
        if func_name not in self.caches:
            file_path = self._get_file_path(func_name)

            if os.path.exists(file_path):
                with open(file_path, 'r', encoding = 'utf-8') as cache:
                    self.caches[func_name] = json.load(cache)
            
            else:
                self.caches[func_name] = {}
        
        return self.caches[func_name]


    def get_column_cache(self, func_name: str, col_name: str):
        """
        Return a nested cache bucket for a specific function and column.

        Args:
            func_name (str): Parent function cache name.
            col_name (str): Nested cache key, commonly a column name.

        Returns:
            dict: The cache dictionary stored under the requested nested key.
            The dictionary is created automatically if it does not yet exist.
        """
        func_cache = self.load_function_cache(func_name)
        
        if col_name not in func_cache:
            func_cache[col_name] = {}
        
        return func_cache[col_name]


    def save_function_cache(self, func_name: str):
        """
        Persist the in-memory cache for a function to disk as JSON.

        Args:
            func_name (str): Function identifier whose cache should be written.

        Returns:
            None
        """
        os.makedirs(self.cache_dir_path, exist_ok = True)
        
        file_path = self._get_file_path(func_name)
        with open(file_path, 'w', encoding = 'utf-8') as f:
            json.dump(self.caches[func_name], f, ensure_ascii = False, indent = 4)


    def delete_column_cache(self, func_name: str, col_name: str):
        """
        Remove a nested cache bucket from a function cache and save the result.

        Args:
            func_name (str): Parent function cache name.
            col_name (str): Nested cache key to remove.

        Returns:
            None
        """
        func_cache = self.load_function_cache(func_name)
        
        if col_name in func_cache:
            del func_cache[col_name]
            self.save_function_cache(func_name)
            
            print(f"Cache for column '{col_name}' deleted from '{func_name}_cache.json'")
        
        else:
            print(f"No cache found for column '{col_name}' in '{func_name}_cache.json'")
