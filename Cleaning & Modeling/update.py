import os
import time
import json
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
from sqlalchemy.types import NVARCHAR, Float, Integer, DateTime

from Cleaning import JsonFile
from Requesting import BaseAPIClient
from Handling import LocalDataHandler



current_dir = os.path.dirname(os.path.abspath(__file__))

load_dotenv(
    dotenv_path = os.path.join(
        os.path.dirname(
            current_dir
        ),
        '.env'
    )
)

lexicon_path = os.path.join(current_dir, "Lexicon References")



def get_data(days_window: int = 90, safety_pages_budget: int = 1000, start_page: int = 1) -> tuple[BaseAPIClient, LocalDataHandler]:
    """
    Fetch raw datasets from the configured API source and persist them to the
    project's raw-data directory using the decoupled architecture.

    Args:
        days_window (int): Maximum age, in days, for jobs returned by the API.
            Defaults to 90.
        safety_pages_budget (int): Number of pages to request per target
            country, starting at `start_page`. Defaults to 1000.
        start_page (int): First API page to request. Defaults to 1.

    Returns:
        tuple[BaseAPIClient, LocalDataHandler]: The initialized API client and
        storage handler instances after the fetch operation completes,
        making chaining or later inspection convenient.

    Raises:
        ValueError: If API credentials are missing, or if any numeric request
        controls are not positive.

    Example:
    ```
        api_client, storage = get_data(days_window=60, safety_pages_budget=35, start_page=1)
    ```
    """
    for name, value in {
        "days_window": days_window,
        "safety_pages_budget": safety_pages_budget,
        "start_page": start_page
    }.items():
        if not isinstance(value, int) or value <= 0:
            raise ValueError(f"'{name}' must be a positive integer.")

    app_id = os.getenv("ADZUNA_API_ID")
    app_key = os.getenv("ADZUNA_APP_KEY")

    if not app_id or not app_key:
        raise ValueError("\"ADZUNA_API_ID\" and \"ADZUNA_APP_KEY\" environment variables must be set.")


    # ---------- Requester Data
    api_client = BaseAPIClient(base_url = "https://api.adzuna.com/v1/api/jobs")

    # ---------- Handler Data
    storage = LocalDataHandler()


    # --- Prepare the keys and parameters
    api_client.params.update({
        "app_id": app_id,
        "app_key": app_key
    })

    countries = [
        "us", "gb", "ca", "au", "nz", "in",
        "de", "fr", "nl", "it", "es", "pl",
        "at", "be", "ch", "br", "mx", "sg", "za"
    ]


    # --- Load Lexicon
    if not os.path.exists(lexicon_path):
        os.makedirs(lexicon_path, exist_ok = True)

        print(f"\n[INFO] Lexicon directory created at: {lexicon_path}.")

    domains_path = os.path.join(lexicon_path, "domains.json")
    careers_lexicon = {}

    try:
        with open(domains_path, 'r', encoding = 'utf-8') as f:
            careers_lexicon = json.load(f)

            print("\nStarting Fetching...")

    except FileNotFoundError:
        print(f"\n[WARNING] Lexicon file not found at: {domains_path}. Skipping Domain-Specific Fetching.")

    except json.JSONDecodeError:
        print(f"\n[WARNING] Lexicon file at {domains_path} is corrupted (Invalid JSON). Skipping...")

    except Exception as e:
        print(f"\n[WARNING] Unexpected error loading Lexicon: {e}. Skipping...")


    # ---------- Fetching
    all_results = []

    print(f"\n--- Fetching Broad Market Data ---")
    try:
        broad_keywords = "data analyst analytics intelligence engineer developer software business marketing sales financial"

        search_data = api_client.request(
            method = "GET",
            url = "{target}/search/{page}",
            is_paginated = True,
            targets = countries,
            page_sequence = range(start_page, start_page + safety_pages_budget),
            extraction_key = "results",
            params = {
                "what_or": broad_keywords,
                "results_per_page": 50,
                "sort_by": "date",
                "max_days_old": days_window
            }
        )

        if isinstance(search_data, list):
            all_results.extend(search_data)

        elif isinstance(search_data, dict) and "results" in search_data:
             all_results.extend(search_data["results"])

    except Exception as e:
        print(f"Error fetching data: {e}")

    print(f"\nTotal jobs fetched across all categories: {len(all_results)}")

    return api_client, storage, all_results



def clean_data(dataframe: JsonFile) -> JsonFile:
    """
    Run the end-to-end cleaning and enrichment pipeline for the search dataset.

    Notes:
        The pipeline standardizes metadata, normalizes categorical fields,
        translates non-English content, imputes missing values, enriches
        coordinates, converts salaries to USD, handles salary outliers, and
        reorders the final columns for downstream consumption.

    Args:
        dataframe (JsonFile): The raw or partially processed search dataset to
            clean.

    Returns:
        JsonFile: A cleaned search dataset ready to be exported or used in
        later analysis steps.

    Raises:
        ValueError: If required raw columns are missing after metadata
        normalization.

    Example:
    ```
        search_dataframe = JsonFile(json_file_path="Raw Data/search.json", key="results")
        cleaned = clean_data(search_dataframe)
    ```
    """
    # ---------- cleaning
    search_dataframe = dataframe


    # --- Load Lexicon
    if not os.path.exists(lexicon_path):
        os.makedirs(lexicon_path, exist_ok = True)

        print(f"\n[INFO] Lexicon directory created at: {lexicon_path}.")


    # --- Metadata Cleaning
    search_dataframe.drop_CLASS_columns(inplace = True)

    search_dataframe.drop(
        columns = ['adref', 'salary_is_predicted', 'redirect_url'],
        errors = 'ignore',
        inplace = True
    )

    search_dataframe.rename_all_DOTS(inplace = True)
    search_dataframe.rename_by_substring("display_name", "name", inplace = True)

    required_columns = [
        'company_name',
        'location_area',
        'title',
        'description',
        'category_tag',
        'category_label',
        'created',
        'contract_type',
        'contract_time',
        'latitude',
        'longitude',
        'salary_min',
        'salary_max',
        'location_name'
    ]

    available_columns = set(search_dataframe.columns)
    missing_columns = [col for col in required_columns if col not in available_columns]

    if missing_columns:
        raise ValueError(f"Missing required columns for cleaning: {missing_columns}")


    # --- Company Name
    search_dataframe = search_dataframe.dropna(subset = ['company_name']).reset_index(drop = True)


    # --- location area (split into 'country' and 'state_or_gov')
    search_dataframe.split_list_objects(
        source_col = 'location_area',
        remove_source_col = True,
        inplace = True,
        country = 0,
        state_or_gov = 1
    )

    search_dataframe['country'] = search_dataframe['country'].astype(str).str.strip()
    search_dataframe['state_or_gov'] = search_dataframe['state_or_gov'].astype(str).str.strip()


    # --- Title (Normalization & Cleaning)
    unique_titles = search_dataframe['title'].dropna().unique()

    temp_df = JsonFile(data = pd.DataFrame({'raw_title': unique_titles, 'title': unique_titles}))

    try:
        with open(os.path.join(lexicon_path, 'tech_replacements.json'), 'r', encoding = 'utf-8') as f:
            tech_replacements = json.load(f)

        temp_df['title'] = temp_df['title'].replace(tech_replacements, regex = True)

    except Exception as e:
        pass

    temp_df.truncate_after_substring(
        regex = False,
        inplace = True,
        title = [' - ', ' -', '- ', ' (', ' , ', ' | ', ': ', '[', '! ', ' _', '_ ']
    )

    temp_df['title'] = temp_df['title'].str.replace(r'[^\w\s\-]|_', '', regex = True)
    temp_df['title'] = temp_df['title'].str.replace(r'(?i)\b(?:In|[UO])\b', '', regex = True)
    temp_df['title'] = temp_df['title'].str.replace(r'\s+', ' ', regex = True).str.strip(' -').str.title()

    try:
        with open(os.path.join(lexicon_path, 'reverse_tech_replacements.json'), 'r', encoding = 'utf-8') as f:
            reverse_tech_replacements = json.load(f)

        temp_df['title'] = temp_df['title'].replace(reverse_tech_replacements, regex = True)

    except Exception as e:
        pass

    title_mapping = dict(zip(temp_df['raw_title'], temp_df['title']))
    search_dataframe['title'] = search_dataframe['title'].map(title_mapping)

    search_dataframe.loc[
        search_dataframe['country'] == 'US',
        'title'
    ] = search_dataframe.loc[
            search_dataframe['country'] == 'US'
        ].truncate_after_substring(regex = False, title = ' In ')['title']

    search_dataframe = search_dataframe[search_dataframe['title'].str.len() <= 35].reset_index(drop = True)

    search_dataframe['title'] = search_dataframe['title'].replace(r'^\s*$', np.nan, regex = True)
    search_dataframe = search_dataframe.dropna(subset = ['title']).reset_index(drop = True)



    # --- Field
    try:
        with open(os.path.join(lexicon_path, "domains.json"), 'r', encoding = 'utf-8') as f:
            careers_lexicon = json.load(f)

        search_dataframe.categorize_by_reference(
            'title', "description",
            target_col = 'field_label',
            reference = careers_lexicon,
            default_category = 'Other',
            inplace = True
        )

        initial_count = len(search_dataframe)

        search_dataframe = search_dataframe[search_dataframe['field_label'] != 'Other'].reset_index(drop = True)

        print(f"\n[INFO] Strict Filter applied: Dropped {initial_count - len(search_dataframe)} unrelated jobs.")

    except Exception as e:
        print(f"Error in categorization: {e}")


    # --- Title (Translation)
    search_dataframe.translate_conditional_column(
        partition_col = 'country',
        detect_col = 'title',
        target_col = 'title',
        target_lang = 'en',
        sample_size = 15,
        threshold = 0.6,
        inplace = True
    )

    search_dataframe['title'] = search_dataframe['title'].str.title()


    # --- Country
    search_dataframe.translate_categorical_column('country', inplace = True)
    search_dataframe['country'] = search_dataframe['country'].replace(
        ['Deutschland', 'The Netherlands'],
        ['Germany', 'Netherlands']
    )


    # --- State or Government
    search_dataframe.translate_categorical_column('state_or_gov', inplace = True)

    search_dataframe.loc[search_dataframe['state_or_gov'] == 'in', 'state_or_gov'] = np.nan

    search_dataframe.fill_missing(
        'state_or_gov',
        strategy = 'mode',
        hierarchy_cols = [
            'country',
            'category_label',
            'company_name',
            'location_name'
        ],
        inplace = True
    )

    search_dataframe['state_or_gov'] = search_dataframe['state_or_gov'].fillna('Not Specified')


    # --- Description
    search_dataframe.translate_conditional_column(
        partition_col = 'country',
        detect_col = 'description',
        target_col = 'description',
        target_lang = 'en',
        sample_size = 15,
        threshold = 0.6,
        inplace = True
    )


    # --- Created date
    search_dataframe['created'] = pd.to_datetime(search_dataframe['created'], errors = 'coerce')
    search_dataframe['created_year'] = search_dataframe['created'].dt.year


    # -- Contract Type
    search_dataframe['contract_type'] = search_dataframe['contract_type'].fillna('Not Specified')
    search_dataframe['contract_type'] = search_dataframe['contract_type'].replace({
        'permanent': 'Permanent',
        'contract': 'Contract',
        'temporary': 'Temporary',
        'volunteer': 'Volunteer',
        'other': 'Other'
    })


    # -- Contract Time
    search_dataframe['contract_time'] = search_dataframe['contract_time'].fillna('Not Specified')

    search_dataframe['contract_time'] = search_dataframe['contract_time'].replace({
        'full_time': 'Full Time',
        'part_time': 'Part Time',
    })


    # --- Category Tag
    search_dataframe['category_tag'] = search_dataframe['category_tag'].astype(str).str.strip()
    search_dataframe['category_tag'] = search_dataframe['category_tag'].replace({'unknown': 'other'})


    # --- Category Label
    search_dataframe['category_label'] = search_dataframe['category_label'].replace({'Unknown': 'Other'})

    search_dataframe.impute_by_language('category_tag', 'category_label', target_lang = 'en', inplace = True)

    search_dataframe.loc[search_dataframe['category_tag'] == 'hr-jobs', 'category_label'] = 'HR Jobs'

    search_dataframe.loc[
        search_dataframe['category_tag'] == 'pr-advertising-marketing-jobs',
        'category_label'
    ] = 'PR, Advertising & Marketing Jobs'

    search_dataframe.loc[
        search_dataframe['category_tag'] == 'Accounting en Financiële vacatures',
        'category_label'
    ] = 'Accounting & Finance Jobs'


    # --- Drop duplicates
    search_dataframe.drop_duplicates(inplace = True)


    # --- Seniority Level
    search_dataframe.extract_keywords(
        'title',
        inplace = True,
        remove_extracted = True,
        seniority_level = ['intern', 'trainee', 'principal', 'senior', 'mid-level', 'junior']
    )
    search_dataframe.extract_keywords(
        'title',
        inplace = True,
        remove_extracted = False,
        seniority_level = ['chief', 'director', 'head', 'lead', 'manager']
    )

    search_dataframe['seniority_level'] = search_dataframe['seniority_level'].fillna('Not Specified')


    # --- Coordinates (Longitude and Latitude)
    search_dataframe.fill_missing_coordinates(
        'state_or_gov', 'country',
        lat_col = 'latitude',
        long_col = 'longitude',
        inplace = True
    )


    # --- Salary (min and max)
    search_dataframe['salary_min'] = pd.to_numeric(search_dataframe['salary_min'], errors = 'coerce')
    search_dataframe['salary_max'] = pd.to_numeric(search_dataframe['salary_max'], errors = 'coerce')

    search_dataframe.convert_to_usd(
        'salary_min', 'salary_max',
        country_col = 'country',
        year_col = 'created_year',
        new_cols = False,
        round_decimals = 0,
        inplace = True
    )

    search_dataframe.handle_outliers(
        'salary_min', 'salary_max',
        hierarchy_cols = [
            'contract_time',
            'contract_type',
            'title',
            'seniority_level',
            'category_label',
            'country',
            'state_or_gov',
            'company_name'
        ],
        min_samples = 5,
        inplace = True
    )

    search_dataframe.fill_missing(
        'salary_min', 'salary_max',
        strategy = 'median',
        hierarchy_cols = [
            'contract_time',
            'contract_type',
            'category_label',
            'country',
            'state_or_gov',
            'seniority_level',
            'title',
            'company_name'
        ],
        inplace = True
    )

    search_dataframe.loc[
        search_dataframe['salary_min'] > search_dataframe['salary_max'],
        'salary_max'
    ] = search_dataframe.loc[
        search_dataframe['salary_min'] > search_dataframe['salary_max'],
        'salary_min'
    ]

    search_dataframe['salary_avg'] = (search_dataframe['salary_min'] + search_dataframe['salary_max']) / 2


    # --- Drop the final unnecessary columns
    search_dataframe.drop(columns = ['description'], inplace = True)
    search_dataframe.drop(columns = ['location_name'], inplace = True)


    search_dataframe.reorder_columns(
        'id',
        'title',
        'field_label',
        'category_label',
        'category_tag',
        'seniority_level',
        'company_name',
        'created',
        'created_year',
        'contract_type',
        'contract_time',
        'salary_min',
        'salary_max',
        'salary_avg',
        'latitude',
        'longitude',
        'state_or_gov',
        'country',
        inplace = True
    )

    return search_dataframe



def run_pipeline(days_window: int = 90, safety_pages_budget: int = 1000, start_page: int = 1) -> None:
    """
    Execute one full fetch-clean-save cycle for the search-data pipeline.

    Notes:
        This helper orchestrates upstream data collection, dataframe cleaning,
        JSON export, and SQL persistence in a single call.

    Args:
        days_window (int): Maximum job age in days requested from the API.
        safety_pages_budget (int): Maximum pages fetched per country target.
        start_page (int): First page index used by paginated fetches.

    Returns:
        None

    Raises:
        Exception: Propagates any failure from fetching, cleaning, JSON export,
            or SQL write so the scheduler can retry.
    """
    print("\n--- Starting Data Pipeline ---")
    
    print("\n--- Starting Data Pipeline ---")
    
    client, handler, new_fetched_jobs = get_data(
        days_window = days_window, 
        safety_pages_budget = safety_pages_budget, 
        start_page = start_page
    )

    cutoff_date = (datetime.now() - timedelta(days = 90)).isoformat()

    print("\n[INFO] Updating and Pruning Raw Data JSON...")
    handler.update_json_payload(
        file_name = "search",
        folder_name = "Raw Data",
        target_key = "results",
        unique_key = "id",
        remove_duplicates = True,
        new_records = new_fetched_jobs,
        condition_func = lambda row: str(row.get('created', '')) >= cutoff_date,
        merge_existing = True,
        save_changes = True
    )

    search_dataframe = JsonFile(
        json_file_path = os.path.join(current_dir, "Raw Data/search.json"),
        key = "results"
    )

    search_dataframe = clean_data(dataframe = search_dataframe)

    search_dataframe.save_to_json(
        os.path.join(os.path.dirname(current_dir), 'Shared Data/search_data.json'),
        orient = 'records',
        force_ascii = False,
        indent = 4
    )

    sql_data_types = {
        'id': NVARCHAR(255), 'title': NVARCHAR(500), 'category_label': NVARCHAR(255),
        'category_tag': NVARCHAR(255), 'seniority_level': NVARCHAR(100),
        'company_name': NVARCHAR(500), 'created': DateTime(), 'created_year': Integer(),
        'contract_type': NVARCHAR(100), 'contract_time': NVARCHAR(100),
        'salary_min': Float(), 'salary_max': Float(), 'salary_avg': Float(),
        'latitude': Float(), 'longitude': Float(), 'state_or_gov': NVARCHAR(255),
        'country': NVARCHAR(255),
    }

    search_dataframe.save_to_sql(
        table_name = 'cleaned_search_data',
        if_exists = 'replace',
        index = False,
        database_type = os.getenv('DB_TYPE'),
        database_driver = os.getenv('DB_DRIVER'),
        database_user = os.getenv('DB_USER'),
        database_password = os.getenv('DB_PASSWORD'),
        database_host = os.getenv('BACKEND_HOST'),
        database_port = os.getenv('DB_PORT'),
        database_name = os.getenv('DB_NAME'),
        columns_types = sql_data_types,
        fast_executemany = True,
        chunk_size = 1000
    )



# --- Main Execution
if __name__ == "__main__":

    STATE_FILE = os.path.join(current_dir, "Settings/Timers/run_state.json")
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok = True)

    print("[SYSTEM] Starting InsightHub Cron-Service...")

    while True:
        now = datetime.now()
        
        if now.hour >= 16:
            last_scheduled = now.replace(hour = 16, minute = 0, second = 0, microsecond = 0)
            next_scheduled = (now + timedelta(days = 1)).replace(hour = 0, minute = 0, second = 0, microsecond = 0)
            
        elif now.hour >= 8:
            last_scheduled = now.replace(hour = 8, minute = 0, second = 0, microsecond = 0)
            next_scheduled = now.replace(hour = 16, minute = 0, second = 0, microsecond = 0)
            
        else:
            last_scheduled = now.replace(hour = 0, minute = 0, second = 0, microsecond = 0)
            next_scheduled = now.replace(hour = 8, minute = 0, second = 0, microsecond = 0)

        should_run = False

        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding = "utf-8") as f:
                try:
                    data = json.load(f)
                    last_run = datetime.fromisoformat(data.get("last_run", ""))
                    
                    if last_run < last_scheduled:
                        should_run = True
                    else:
                        remaining = next_scheduled - now
                        total_seconds = max(0, int(remaining.total_seconds()))
                       
                        hours, remainder = divmod(total_seconds, 3600)
                        minutes, _ = divmod(remainder, 60)
                        
                        print(f"[{now.strftime('%H:%M')}] Standby. Next run exactly at {next_scheduled.strftime('%I:%M %p')}. (In {hours}h {minutes}m)")
                        sleep_duration = min(60 * 30, remaining.total_seconds())
                        
                        time.sleep(max(1, sleep_duration))
                       
                        continue
                        
                except (ValueError, KeyError, json.JSONDecodeError):
                    should_run = True
        else:
            should_run = True

        if should_run:
            try:
                print(f"\n[ACTION] Triggering scheduled pipeline at {now.strftime('%Y-%m-%d %H:%M:%S')}...")
                
                # THE PIPELINE
                run_pipeline(days_window = 1, safety_pages_budget = 15, start_page = 1)
                
                with open(STATE_FILE, "w", encoding = "utf-8") as f:
                    json.dump({"last_run": datetime.now().isoformat()}, f, indent = 4)
                    
                print(f"[SUCCESS] State saved. See you at {next_scheduled.strftime('%I:%M %p')}!")
                
            except Exception as e:
                print(f"[CRITICAL ERROR] Pipeline failed: {e}")
                print("[SYSTEM] Will retry in 5 minutes to catch up on this shift...")
                
                time.sleep(60 * 5)
