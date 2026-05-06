import os
import json
import pandas as pd
from collections.abc import Callable

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
            append: bool = False,
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
            append (bool): If True and the target JSON file exists, merges the
                new payload into the existing list or dictionary before writing.
                Defaults to False.
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

        final_data = data
        is_appended = False

        if append and os.path.exists(file_path):
            try:
                with open(file_path, "r", encoding = encoding) as f:
                    existing_data = json.load(f)

                if isinstance(existing_data, list) and isinstance(data, list):
                    existing_data.extend(data)
                    final_data = existing_data
                    is_appended = True

                elif isinstance(existing_data, dict) and isinstance(data, dict):
                    if "results" in existing_data and "results" in data:
                        existing_data["results"].extend(data["results"])

                    else:
                        existing_data.update(data)

                    final_data = existing_data
                    is_appended = True

                else:
                    print("[WARNING] Data types mismatch (e.g., list vs dict). Overwriting instead.")

            except Exception as e:
                print(f"[WARNING] Could not read existing file for append ({e}). Overwriting instead.")

        with open(file_path, "w", encoding = encoding) as dt:
            json.dump(final_data, dt, ensure_ascii = ensure_ascii, indent = indent)

        mode_str = "APPENDED TO" if is_appended else "SAVED NEW"

        print(f" [{mode_str}] Data successfully written to {file_path}")

        return None


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


    def update_json_payload(
        self,
        file_name: str,
        folder_name: str,
        target_key: str | None = None,
        unique_key: str | None = None,
        remove_duplicates: bool = True,
        new_records: list | None = None,
        condition_func: Callable | None = None,
        merge_existing: bool = True,
        output_file_name: str | None = None,
        output_folder_name: str | None = None,
        save_changes: bool = False,
        encoding: str = 'utf-8',
        ensure_ascii: bool = False,
        indent: int | str | None = 4
    ) -> dict | list | None:
        """
        Update a JSON payload by loading an existing file, optionally merging new
        records, removing duplicates, pruning records, and saving the result.

        Overview:
            This method acts as a flexible JSON update engine for list-based
            payloads. It can read an existing file, merge incoming records,
            remove duplicates while keeping the latest version of each record,
            filter records with a condition function, and optionally persist the
            final payload to either the original file or a custom output path.

        Args:
            file_name (str): Source JSON file name without extension.
            folder_name (str): Source folder relative to `base_dir`.
            target_key (str | None): Root key that stores the record list when
                the JSON payload is a dictionary. Leave as None for flat JSON
                lists.
            unique_key (str | None): Record field used to identify duplicates
                when `remove_duplicates=True`.
            remove_duplicates (bool): If True, removes duplicate records by
                `unique_key` while keeping the latest occurrence. Defaults to
                True.
            new_records (list | None): New records to merge into the payload.
                Defaults to None.
            condition_func (Callable | None): Optional predicate that receives
                one record and returns True when the record should be kept.
            merge_existing (bool): If True, loads and merges the current source
                payload before processing. Defaults to True.
            output_file_name (str | None): Optional output file name used only
                when `save_changes=True`.
            output_folder_name (str | None): Optional output folder used only
                when `save_changes=True`.
            save_changes (bool): If True, writes the final payload to disk.
                Defaults to False.
            encoding (str): File encoding used for read/write operations.
                Defaults to `'utf-8'`.
            ensure_ascii (bool): Whether JSON output should escape non-ASCII
                characters. Defaults to False.
            indent (int | str | None): Indentation passed to `json.dump`.
                Defaults to 4.

        Returns:
            dict | list | None: The final payload in its output structure. This
            is a dictionary when `target_key` is used, otherwise a flat list.

        Raises:
            ValueError: If the configuration matches a documented anti-pattern,
                such as custom output paths without saving or wiping a file by
                saving an empty non-merged payload.
            TypeError: If the loaded source payload or provided records do not
                match the expected list-based structure.
            KeyError: If `target_key` is provided for a dictionary payload but
                the key does not exist in the source file.

        Example:
        ```
            handler.update_json_payload(
                file_name="search",
                folder_name="Raw Data",
                target_key="results",
                unique_key="id",
                new_records=fetched_jobs,
                merge_existing=True,
                remove_duplicates=True,
                save_changes=True
            )
        ```

        Notes:
            - `file_name` and `folder_name` are always required.
            - When `remove_duplicates=True`, a non-empty `unique_key` should be
              supplied; otherwise deduplication is skipped with a warning.
            - `output_file_name` and `output_folder_name` are only meaningful
              when `save_changes=True`.
            - If `merge_existing=False`, `new_records` is empty, and
              `save_changes=True`, the method raises an error instead of wiping
              the target file.
            - `target_key` is structural only: use it for dictionary roots that
              store the record list under one key.
        """
        file_name = str(file_name).strip()
        folder_name = str(folder_name).strip()
        target_key = str(target_key).strip() if target_key is not None else None
        unique_key = str(unique_key).strip() if unique_key is not None else None
        output_file_name = str(output_file_name).strip() if output_file_name is not None else None
        output_folder_name = str(output_folder_name).strip() if output_folder_name is not None else None

        if not file_name or not folder_name:
            raise ValueError("'file_name' and 'folder_name' must be non-empty strings.")

        if not save_changes and (output_file_name or output_folder_name):
            raise ValueError("Custom output paths require 'save_changes=True'.")

        source_file_path = os.path.join(self.base_dir, folder_name, f"{file_name}.json")

        if new_records is None:
            new_records = []
        elif not isinstance(new_records, list):
            raise TypeError("'new_records' must be a list of records or None.")

        if save_changes and not merge_existing and not new_records:
            raise ValueError(
                "Unsafe configuration: saving with 'merge_existing=False' and no "
                "'new_records' would wipe the target payload."
            )

        if remove_duplicates and not unique_key:
            print("[WARNING] 'remove_duplicates=True' was requested without 'unique_key'. Deduplication will be skipped.")
            remove_duplicates = False

        old_records = []
        output_payload = {target_key: []} if target_key else []

        if merge_existing and os.path.exists(source_file_path):
            try:
                with open(source_file_path, "r", encoding = encoding) as f:
                    data = json.load(f)

                if isinstance(data, dict):
                    if not target_key:
                        raise ValueError(
                            "Loaded JSON root is a dictionary. Provide 'target_key' "
                            "to identify the record list."
                        )

                    if target_key not in data:
                        raise KeyError(f"Key '{target_key}' was not found in '{source_file_path}'.")

                    if not isinstance(data[target_key], list):
                        raise TypeError(f"Key '{target_key}' must contain a list of records.")

                    old_records = data[target_key]
                    output_payload = data.copy()

                elif isinstance(data, list):
                    old_records = data

                else:
                    raise TypeError("Source JSON payload must be either a list or a dictionary containing a record list.")

            except (OSError, json.JSONDecodeError) as e:
                print(f"[ERROR] Could not read old file: {e}. Will treat as empty.")

        initial_old_count = len(old_records)
        combined_records = old_records.copy() if merge_existing else []
        combined_records.extend(new_records)

        if remove_duplicates and unique_key:
            seen_keys = set()
            processed_records = []

            for row in reversed(combined_records):
                if not isinstance(row, dict):
                    raise TypeError("Deduplication requires every record to be a dictionary.")

                val = row.get(unique_key)

                if val not in seen_keys:
                    if val is not None:
                        seen_keys.add(val)

                    processed_records.append(row)

            processed_records.reverse()

        else:
            processed_records = combined_records

        if condition_func:
            final_records = [row for row in processed_records if condition_func(row)]

        else:
            final_records = processed_records
        
        duplicates_removed = len(combined_records) - len(processed_records)
        old_jobs_pruned = len(processed_records) - len(final_records)

        print(f"[UPDATE STATS] Loaded: {initial_old_count} | Fetched: {len(new_records)}")
        print(f" ┣━ Removed Duplicates: {duplicates_removed}")
        print(f" ┣━ Pruned by Condition: {old_jobs_pruned}")
        print(f" ┗━ Final Saved: {len(final_records)}")

        if target_key:
            output_payload[target_key] = final_records

        else:
            output_payload = final_records

        if save_changes:
            final_file_name = output_file_name if output_file_name else file_name
            final_folder_name = output_folder_name if output_folder_name else folder_name

            self.save_payload_to_json(
                data = output_payload,
                file_name = final_file_name,
                folder_name = final_folder_name,
                append = False,
                encoding = encoding,
                ensure_ascii = ensure_ascii,
                indent = indent
            )

        return output_payload
