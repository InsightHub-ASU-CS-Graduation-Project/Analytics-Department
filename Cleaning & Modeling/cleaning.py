import pandas as pd
import json

class JsonFile(pd.DataFrame):

    @property
    def _constructor(self):
        return type(self)


    def __init__(self, data = None, json_file_path: str = None, *args, **kwargs):
        if json_file_path is not None:
            with open(json_file_path, "r") as file:
                raw_data = json.load(file)

            data = pd.json_normalize(raw_data)

        super().__init__(data = data, *args, **kwargs)


    def drop_by_substring(self, substring: str, inplace: bool = False):
        """
        Drop all columns that contain a specific substring within their names.

        Args:
            substring (str): The text to search for; any column containing this 
                string will be removed.
            inplace (bool): If True, modifies the object directly and returns None. 
                Defaults to False.

        Returns:
            The modified object (self) if `inplace` is False, otherwise None.

        Example:
        ```
            Column Removal:
                'user_id'         ->  (dropped)
                'user_metadata'   ->  (dropped)
                'created_at'      ->  'created_at' (kept)

            Usage:
                df.drop_by_substring("user")
        ```
        """

        columns_to_drop = [column for column in self.columns if substring in column]

        if not columns_to_drop:
            return self if not inplace else None
        
        return self.drop(columns = columns_to_drop, inplace = inplace)
    

    def drop_CLASS_columns(self, inplace: bool = False):
        """
        Drop all columns that contain the substring "__CLASS__" from the data.

        Args:
            inplace (bool): If True, modifies the object directly and returns None. 
                Defaults to False.

        Returns:
            The modified object (self) if `inplace` is False, otherwise None.

        Example:
        ```
            Column Removal:
                'metadata.__CLASS__'  ->  (dropped)
                'object.__CLASS__id'  ->  (dropped)
                'user_name'           ->  'user_name' (kept)

            Usage:
                df.drop_CLASS_columns()
        ```
        """

        return self.drop_by_substring("__CLASS__", inplace = inplace)


    def rename_by_substring(self, old_substring: str, new_substring: str, inplace: bool = False):
        """
        Rename columns by replacing a specific substring with a new one.
        
        Args:
            old_substring (str): The text to search for within the column names.
            new_substring (str): The text to replace the old substring with.
            inplace (bool): If True, modifies the object directly and returns None. 
                Defaults to False.

        Returns:
            The modified object (self) if `inplace` is False, otherwise None.

        Example:
        ```
            Column Transformation:
                'user_display_name'  ->  'user_name'
                'post_display_name'  ->  'post_name'
                'created_at'         ->  'created_at' (unchanged)

            Usage:
                df.rename_by_substring("display_name", "name")
        ```     
        """

        columns_to_rename = [column for column in self.columns if old_substring in column]

        if not columns_to_rename:
            return self if not inplace else None
        
        renamer = {}
        for column in columns_to_rename:
            renamer[column] = column.replace(old_substring, new_substring)
        
        return self.rename(columns = renamer, inplace = inplace)
    

    def rename_all_DOTS(self, unified_seperator: str = "_", inplace: bool = False):
        """
        Rename columns by replacing all occurrences of dots ('.') with a unified separator.

        Args:
            unified_seperator (str): The new string to replace dots with. 
                Defaults to "_".
            inplace (bool): If True, modifies the object directly and returns None. 
                Defaults to False.

        Returns:
            The modified object (self) if `inplace` is False, otherwise None.

        Example:
        ```
            Column Transformation:
                'location.display_name'  ->  'location_display_name'
                'user.id'                ->  'user_id'
                'status'                 ->  'status' (unchanged)

            Usage:
                df.rename_all_DOTS(unified_seperator = "_")
        ```
        """

        return self.rename_by_substring(".", unified_seperator, inplace = inplace)


class SearchFile(JsonFile):
    def __init__(self, data = None, *args, **kwargs):
        if data is None and 'json_file_path' not in kwargs:
            kwargs['json_file_path'] = r"Raw Data/search.json"

        super().__init__(data = data, *args, **kwargs)


class CategoriesFile(JsonFile):
    def __init__(self, data = None, *args, **kwargs):
        if data is None and 'json_file_path' not in kwargs:
            kwargs['json_file_path'] = r"Raw Data/categories.json"

        super().__init__(data = data, *args, **kwargs)


class GeodataFile(JsonFile):
    def __init__(self, data = None, *args, **kwargs):
        if data is None and 'json_file_path' not in kwargs:
            kwargs['json_file_path'] = r"Raw Data/geodata.json"

        super().__init__(data = data, *args, **kwargs)


class HistoryFile(JsonFile):
    def __init__(self, data = None, *args, **kwargs):
        if data is None and 'json_file_path' not in kwargs:
            kwargs['json_file_path'] = r"Raw Data/history.json"

        super().__init__(data = data, *args, **kwargs)


class TopCompaniesFile(JsonFile):
    def __init__(self, data = None, *args, **kwargs):
        if data is None and 'json_file_path' not in kwargs:
            kwargs['json_file_path'] = r"Raw Data/top_companies.json"

        super().__init__(data = data, *args, **kwargs)