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

    
    def view(self):
        """
        Return the object as a standard pandas DataFrame for inspection and 
        rich display.

        Returns:
            pandas.DataFrame: A DataFrame representation of the current object.

        Example:
        ```
            Usage:
                # Viewing a slice of the custom object as a dynamic table
                search_dataframe.iloc[0:100].view()

                # Using pandas-specific plotting on the view
                search_dataframe.view().plot(kind='bar')
        ```
        """
        
        return pd.DataFrame(self)


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

        columns_to_drop = [col for col in self.columns if substring in col]

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

        columns_to_rename = {
            col: col.replace(old_substring, new_substring)
            for col in self.columns
            if old_substring in col
        }

        if not columns_to_rename:
            return self if not inplace else None
        
        return self.rename(columns = columns_to_rename, inplace = inplace)
    

    def rename_all_DOTS(self, unified_separator: str = "_", inplace: bool = False):
        """
        Rename columns by replacing all occurrences of dots ('.') with a unified separator.

        Args:
            unified_separator (str): The new string to replace dots with. 
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
                df.rename_all_DOTS(unified_separator = "_")
        ```
        """

        return self.rename_by_substring(".", unified_separator, inplace = inplace)


    def reorder_columns(self, first_cols: list = [], inplace: bool = False):
        """
        Reorder the dataframe by moving a specified list of columns to the front.
        
        Any columns not mentioned in `first_cols` will be appended to the end 
        preserving their original relative order.

        Args:
            first_cols (list): A list of column names that should appear first.
            inplace (bool): If True, modifies the object directly and returns None. 
                Defaults to False.

        Returns:
            The modified object (self) if `inplace` is False, otherwise None.

        Example:
        ```
            Initial Columns: ['C', 'A', 'D', 'B']
            
            Operation: reorder_columns(first_cols=['A', 'B'])
            Result:    ['A', 'B', 'C', 'D']

            Usage:
                df.reorder_columns(["id", "timestamp"], inplace=True)
        ```
        """
        
        ordered = [col for col in first_cols if col in self.columns]
        
        if not ordered:
            return self if not inplace else None
        
        final_order = ordered + [col for col in self.columns if col not in ordered]

        if inplace:
            for i, col in enumerate(final_order):
                col_data = self.pop(col)
                self.insert(i, col, col_data)

            return None
            
        else:
            return self[final_order]


    def move_column(self, col_to_move: str, col_target: str, position: str = "after", inplace: bool = False):
        """
        Move a specific column to a new position relative to a target column.

        Args:
            col_to_move (str): The name of the column you want to relocate.
            col_target (str): The reference column where the move will happen.
            position (str): Whether to place the column "after" or "before" the 
                target column. Defaults to "after".
            inplace (bool): If True, modifies the object directly and returns None. 
                Defaults to False.

        Returns:
            The modified object (self) if `inplace` is False, otherwise None.

        Raises:
            ValueError: If the `position` argument is not 'after' or 'before'.

        Example:
        ```
            Initial Columns: ['A', 'B', 'C', 'D']
            
            Operation: move_column(col_to_move='A', col_target='C', position='after')
            Result:    ['B', 'C', 'A', 'D']

            Usage:
                df.move_column("user_id", "user_name", position="before")
        ```
        """
        
        if col_to_move not in self.columns or col_target not in self.columns:
            return self if not inplace else None
            
        if position not in ["after", "before"]:
            raise ValueError("position parameter must be 'after' or 'before'")

        if inplace:
            col_data = self.pop(col_to_move)
            target_index = self.columns.get_loc(col_target)

            insert_index = target_index + 1 if position == "after" else target_index
            
            self.insert(insert_index, col_to_move, col_data)
            return None
            
        else:
            cols = self.columns.to_list()
            cols.remove(col_to_move)
            target_index = cols.index(col_target)
            
            insert_index = target_index + 1 if position == "after" else target_index
            
            cols.insert(insert_index, col_to_move)
            return self[cols]
        
        
    def align_lat_lng(self, inplace: bool = False):
        """
        Standardize the coordinate order by moving the 'longitude' column 
        immediately after the 'latitude' column.

        .. note::
        This method strictly expects the column names to be exactly **'latitude'** 
        and **'longitude'** (case-sensitive). If the columns are named differently 
        (e.g., 'lat' or 'LONG'), the alignment will not occur.

        Args:
            inplace (bool): If True, modifies the object directly and returns None. 
                Defaults to False.

        Returns:
            The modified object (self) if `inplace` is False, otherwise None.

        Example:
        ```
            Column Reordering:
                ['longitude', 'id', 'latitude']  ->  ['id', 'latitude', 'longitude']

            Usage:
                df.align_lat_lng(inplace=True)
        ```
        """
        
        return self.move_column(col_to_move = "longitude", col_target = "latitude", position = "after", inplace = inplace)


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