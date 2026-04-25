import os
import json
import pandas as pd

class LocalDataHandler:
    """
    Handles local file system operations: saving payloads and building reference caches.
    """
    def __init__(self, base_dir: str | None = None):
        """
        Initialize the local data handler with a base output directory.

        Args:
            base_dir (str | None): Base directory used for all saved outputs.
                Defaults to the current module directory when omitted.

        Returns:
            None
        """
        self.base_dir = base_dir or os.path.dirname(os.path.abspath(__file__))


    def save_payload_to_json(
            self,
            data: list | dict,
            file_name: str,
            folder_name: str = "Raw Data",
            encoding: str = 'utf-8',
            ensure_ascii: bool = False,
            indent: int | str | None = 4
    ) -> None:
        """
        Save a list or dictionary payload to a local JSON file.

        Args:
            data (list | dict): Payload to serialize.
            file_name (str): Target file name without extension.
            folder_name (str): Output folder relative to `base_dir`. Defaults
                to `"Raw Data"`.
            encoding (str): File encoding. Defaults to `'utf-8'`.
            ensure_ascii (bool): Whether to escape non-ASCII characters.
                Defaults to False.
            indent (int | str | None): JSON indentation passed to `json.dump`.
                Defaults to 4.

        Returns:
            None
        """
        target_dir = os.path.join(self.base_dir, folder_name)
        
        os.makedirs(target_dir, exist_ok = True)
        
        file_path = os.path.join(target_dir, f"{file_name}.json")
        
        with open(file_path, "w", encoding = encoding) as dt:
            json.dump(data, dt, ensure_ascii = ensure_ascii, indent = indent)
            
        print(f"Data saved to {file_path}")


    def build_reference_lexicon(
        self, 
        *source_cols, 
        file_path: str, 
        save_to_json: bool = False,
        file_name: str = "reference_lexicon",
        folder_name: str = "Lexicons",
        encoding: str = 'utf-8',
        ensure_ascii: bool = False,
        indent: int | str | None = 4
    ) -> dict | None:
        """
        Extract normalized values from one or more file columns and return them as a dictionary.

        Args:
            *source_cols: Column names to read from the external file.
            file_path (str): Path to a tabular file such as CSV or Excel.
            save_to_json (bool): If True, saves the generated dict using `save_payload_to_json`.
            file_name (str): The target file name if saving to JSON.
            folder_name (str): The target folder name if saving to JSON.

        Returns:
            dict | None: A dictionary where keys are column names and values are 
            boolean lookup dicts, e.g., {'skill_name': {'python': True, 'sql': True}}.
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
                print(f"Error: None of the columns {source_cols} exist in the file.")
                
                return None

            lexicon_dict = {}

            for col in valid_cols:
                new_data = df[col].dropna().astype(str).str.strip().str.lower().unique().tolist()
                
                lexicon_dict[col] = {item: True for item in new_data}
                
                print(f"Extracted {len(new_data)} unique items for column '{col}'.")

            if save_to_json:
                self.save_payload_to_json(
                    data = lexicon_dict, 
                    file_name = file_name, 
                    folder_name = folder_name,
                    encoding = encoding,
                    ensure_ascii = ensure_ascii,
                    indent = indent
                )

                print(f"Success: Lexicon saved to {folder_name}/{file_name}.json")
            
            return lexicon_dict

        except FileNotFoundError:
            print(f"Error: Could not find the file '{file_path}'.")
        
        except Exception as e:
            print(f"Error during extraction: {e}")

        return None
