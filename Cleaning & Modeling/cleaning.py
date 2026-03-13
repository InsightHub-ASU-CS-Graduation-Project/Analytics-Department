import pandas as pd
from langdetect import detect, DetectorFactory
import json
import os
from geopy.geocoders import Nominatim
from typing import Self

class JsonFile(pd.DataFrame):

    @property
    def _constructor(self):
        return type(self)


    def __init__(self, data = None, json_file_path: str = None, *args, **kwargs) -> None:
        if json_file_path is not None:
            with open(json_file_path, "r") as file:
                raw_data = json.load(file)

            data = pd.json_normalize(raw_data)

        super().__init__(data = data, *args, **kwargs)

    
    def view(self) -> pd.DataFrame:
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


    def drop_by_substring(self, substring: str, inplace: bool = False) -> (Self | None):
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
    

    def drop_CLASS_columns(self, inplace: bool = False) -> (Self | None):
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


    def rename_by_substring(self, old_substring: str, new_substring: str, inplace: bool = False) -> (Self | None):
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
    

    def rename_all_DOTS(self, unified_separator: str = "_", inplace: bool = False) -> (Self | None):
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


    def reorder_columns(self, *first_cols, inplace: bool = False) -> (Self | None):
        """
        Reorder the dataframe by moving a specified list of columns to the front.
        
        Any columns not mentioned in `first_cols` will be appended to the end 
        preserving their original relative order.

        Args:
            first_cols (tuple): A list of column names that should appear first.
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


    def move_column(self, col_to_move: str, col_target: str, position: str = "after", inplace: bool = False) -> (Self | None):
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
        
        
    def align_lat_lng(self, inplace: bool = False) -> (Self | None):
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
    

    def extract_keywords(
            self,
            *source_cols,
            inplace: bool = False,
            remove_extracted: bool = False,
            **target_cols_and_keywords
    ) -> (Self | None):
        """
        Search for keywords across source columns and extract them into new target columns.
        Matches are case-insensitive and are formatted to Title Case in the target column. 

        Notes:
            - **Keyword Priority**: Keywords are extracted based on list order. The first item in the list has the highest precedence.
            - **Non-Destructive Updates**: If this method is called multiple times targeting the exact same output column.
                Subsequent calls will only populate empty cells (NaNs) and will NEVER overwrite previously extracted values.
        
        Args:
            *source_cols: Variable number of column names to search within.
            inplace (bool): If True, modifies the object directly and returns None. 
                Defaults to False.
            remove_extracted (bool): If True, removes the matched keywords from the 
                original source columns to clean the text. Defaults to False.
            **target_cols_and_keywords: Keyword arguments where the key is the new 
                column name and the value is a list of strings to search for.

        Returns:
            The modified object (self) if `inplace` is False, otherwise None.

        Example:
        ```
            Initial Column: 
                'description': ["Blue Suede Shoes", "Red Cotton Shirt"]

            Operation: 
                extract_keywords('description', colors=['blue', 'red'], remove_extracted=True)

            Result:
                'description': ["Suede Shoes", "Cotton Shirt"]
                'colors':      ["Blue", "Red"]

            Usage:
                df.extract_keywords("comments", "tags", categories=["urgent", "review"], inplace=True)
        ```
        """
    
        sources = list(source_cols)
        valid_sources = [col for col in sources if col in self.columns]
        
        if not valid_sources:
            return self if not inplace else None

        combined_text = self[valid_sources].fillna('').agg(' '.join, axis = 1)
        
        result = self.copy() if not inplace else self
        
        for new_col_name, keywords_list in target_cols_and_keywords.items():
            extracted = pd.Series(index = combined_text.index, dtype = str)

            for word in reversed(keywords_list):
                pattern = rf"(?i)\b{word}\b"

                mask = combined_text.str.contains(pattern, na = False, regex = True)

                extracted.loc[mask] = word.title()

            if new_col_name in result.columns:
                result[new_col_name] = result[new_col_name].fillna(extracted).str.title()
            else:
                result[new_col_name] = extracted.str.title()

            if remove_extracted:
                for col in valid_sources:
                    mask = result[col].notna()
                    
                    result.loc[mask, col] = result.loc[mask, col].str.replace(rf"(?i)\b({'|'.join(keywords_list)})\b", '', regex = True)
                    
                    result.loc[mask, col] = result.loc[mask, col].str.replace(r'\s+', ' ', regex = True).str.strip()
        
        return None if inplace else result
            

    def split_list_objects(
            self,
            source_col: str,
            remove_source_col: bool = False,
            explode_rows: bool = False,
            inplace: bool = False,
            **target_cols_and_indices
    ) -> (Self | None):
        """
        Extract specific elements from lists stored within a column into new columns.

        This method allows you to pull items out of list-like objects by their index. 
        It can either join multiple indices into a string or explode them into separate rows.

        Notes:
            - **Inplace Restriction**: 
              Using `inplace=True` alongside `explode_rows=True` will fail to modify the original 
              dataframe's shape due to pandas memory reallocation. 
              Always use `inplace=False` and reassign when exploding.
            - **String Joining**: When providing a list of indices with 
              `explode_rows=False`, valid extracted elements are joined 
              into a single comma-separated string (NaNs are ignored).

        Args:
            source_col (str): The name of the column containing list-like objects.
            remove_source_col (bool): If True, deletes the original source column after 
                extraction. Defaults to False.
            explode_rows (bool): If True, and an index list is provided, expands the 
                dataframe so each extracted element gets its own row. Defaults to False.
            inplace (bool): If True, modifies the object directly and returns None. 
                Defaults to False.
            **target_cols_and_indices: Keyword arguments where the key is the new 
                column name and the value is the index (int) or list of indices (list) 
                to extract from the source list.

        Returns:
            The modified object (self) if `inplace` is False, otherwise None.

        Example:
        ```
            Initial Data: 
                'data_list': [['A', 'B', 'C'], ['X', 'Y']]

            Operation: 
                split_list_objects('data_list', first_val=0, second_val=1)

            Result:
                'data_list': [['A', 'B', 'C'], ['X', 'Y']]
                'first_val': ['A', 'X']
                'second_val': ['B', 'Y']

            Usage:
                # Extract index 0 and 2, then explode into rows
                df.split_list_objects("coordinates", lat_lng=[0, 1], explode_rows=True)
        ```
        """

        if source_col not in self.columns:
            return self if not inplace else None

        result = self.copy() if not inplace else self
        
        for col, index in target_cols_and_indices.items():
            if isinstance(index, list):
                extracted_parts = [result[source_col].str[i] for i in index]

                if explode_rows:
                    result[col] = [[item for item in row if pd.notna(item)] for row in zip(*extracted_parts)]

                    result = result.explode(col, ignore_index = True)

                else:
                    result[col] = [
                        ', '.join([str(item) for item in row if pd.notna(item) and str(item).strip() != ''])
                        for row in zip(*extracted_parts)
                    ]
            
            else:
                result[col] = result[source_col].str[index]

        if remove_source_col:
            result.drop(columns = [source_col], inplace = True)
        
        return None if inplace else result
        
    
    def impute_by_language(
            self,
            reference_col: str,
            *target_cols,
            target_lang: str = 'en',
            inplace: bool = False
    ) -> (Self | None):
        """
        Impute missing or inconsistent values in target columns based on a reference column 
        and a preferred language.

        Notes:
            - **Dependencies**: This method requires the `langdetect` library.
                It will raise an ImportError if not installed.
            - **Resolution Hierarchy**: The method determines the correct translation using a 3-pass fallback strategy:
                1. Exact Match: Checks if the target value matches the reference column (ignoring hyphens and case).
                2. AI Detection: Uses `langdetect` to verify if the text matches the `target_lang`.
                3. Auto-Generation: If no valid translation is found, it generates a fallback name by title-casing the reference column.
            - **Deterministic Output**:
                The `langdetect` seed is fixed to 0 to ensure consistent results across multiple runs.

        Args:
            reference_col (str): The column used as a key (e.g., 'category_slug').
            *target_cols: One or more columns to be updated/imputed (e.g., 'category_name').
            target_lang (str): The ISO 639-1 language code to prefer. Defaults to 'en'.
            inplace (bool): If True, modifies the object directly and returns None. 
                Defaults to False.

        Returns:
            The modified object (self) if `inplace` is False, otherwise None.

        Example:
        ```
            Initial Data:
                'slug': ['red-apple', 'red-apple', 'green-pear']
                'name': [NaN,         'Red Apple', 'Pear (FR)']

            Operation:
                df.impute_by_language('slug', 'name', target_lang='en')

            Result:
                'name': ['Red Apple', 'Red Apple', 'Green Pear']

            Usage:
                df.impute_by_language("city_id", "city_name", target_lang="ar")
        ```
        """
        DetectorFactory.seed = 0 

        if reference_col not in self.columns:
            return self if not inplace else None

        result = self.copy() if not inplace else self
        
        target_cols = list(target_cols)

        for col in target_cols:
            if col not in result.columns:
                continue

            unique_pairs = result[[reference_col, col]].dropna().drop_duplicates()
            
            mapping_dict = {}
            for ref, val in zip(unique_pairs[reference_col], unique_pairs[col]):
                if ref in mapping_dict:
                    continue

                text = str(val).strip()
                ref_clean = str(ref).replace('-', ' ').lower()
                
                if text.lower() == ref_clean:
                    mapping_dict[ref] = val
                    continue
            
            for ref, val in zip(unique_pairs[reference_col], unique_pairs[col]):
                if ref in mapping_dict:
                    continue

                text = str(val).strip()

                if not text or text.isnumeric():
                    continue

                try:
                    if detect(text) == target_lang:
                        mapping_dict[ref] = val
                except:
                    continue

            all_unique_refs = result[reference_col].dropna().unique()
            for ref in all_unique_refs:
                if ref not in mapping_dict:
                    mapping_dict[ref] = str(ref).replace('-', ' ').title()
            
            result[col] = result[reference_col].map(mapping_dict).fillna(result[col])

        return None if inplace else result




class SearchFile(JsonFile):
    def __init__(self, data = None, *args, **kwargs) -> None:
        if data is None and 'json_file_path' not in kwargs:
            raw_data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Raw Data")
            
            kwargs['json_file_path'] = os.path.join(raw_data_dir, f"search.json")

        super().__init__(data = data, *args, **kwargs)


class CategoriesFile(JsonFile):
    def __init__(self, data = None, *args, **kwargs) -> None:
        if data is None and 'json_file_path' not in kwargs:
            raw_data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Raw Data")
            
            kwargs['json_file_path'] = os.path.join(raw_data_dir, f"categories.json")

        super().__init__(data = data, *args, **kwargs)


class GeodataFile(JsonFile):
    def __init__(self, data = None, *args, **kwargs) -> None:
        if data is None and 'json_file_path' not in kwargs:
            raw_data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Raw Data")
            
            kwargs['json_file_path'] = os.path.join(raw_data_dir, f"geodata.json")

        super().__init__(data = data, *args, **kwargs)


class HistoryFile(JsonFile):
    def __init__(self, data = None, *args, **kwargs) -> None:
        if data is None and 'json_file_path' not in kwargs:
            raw_data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Raw Data")
            
            kwargs['json_file_path'] = os.path.join(raw_data_dir, f"history.json")

        super().__init__(data = data, *args, **kwargs)


class TopCompaniesFile(JsonFile):
    def __init__(self, data = None, *args, **kwargs) -> None:
        if data is None and 'json_file_path' not in kwargs:
            raw_data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Raw Data")
            
            kwargs['json_file_path'] = os.path.join(raw_data_dir, f"top_companies.json")

        super().__init__(data = data, *args, **kwargs)