from Libraries import *



class CacheManager:
    def __init__(self, cache_dir: str = "Cache Data"):
        self.cache_dir = cache_dir
        self.cache_dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), self.cache_dir)
                                           
        self.caches = {}


    def _get_file_path(self, func_name: str) -> str:
        return os.path.join(self.cache_dir_path, f"{func_name}_cache.json")


    def load_function_cache(self, func_name: str):
        if func_name not in self.caches:
            file_path = self._get_file_path(func_name)

            if os.path.exists(file_path):
                with open(file_path, 'r', encoding = 'utf-8') as cache:
                    self.caches[func_name] = json.load(cache)
            
            else:
                self.caches[func_name] = {}
        
        return self.caches[func_name]


    def get_column_cache(self, func_name: str, col_name: str):
        func_cache = self.load_function_cache(func_name)
        
        if col_name not in func_cache:
            func_cache[col_name] = {}
        
        return func_cache[col_name]


    def save_function_cache(self, func_name: str):
        os.makedirs(self.cache_dir_path, exist_ok = True)
        
        file_path = self._get_file_path(func_name)
        with open(file_path, 'w', encoding = 'utf-8') as f:
            json.dump(self.caches[func_name], f, ensure_ascii = False, indent = 4)


    def delete_column_cache(self, func_name: str, col_name: str):
        func_cache = self.load_function_cache(func_name)
        
        if col_name in func_cache:
            del func_cache[col_name]
            self.save_function_cache(func_name)
            
            print(f"Cache for column '{col_name}' deleted from '{func_name}_cache.json'")
        
        else:
            print(f"No cache found for column '{col_name}' in '{func_name}_cache.json'")