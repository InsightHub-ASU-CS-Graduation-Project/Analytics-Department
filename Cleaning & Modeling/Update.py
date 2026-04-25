import os
import json
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy.types import NVARCHAR, Float, Integer, DateTime

from Cleaning import SearchFile
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



def get_data(total_target_jobs: int = 55555) -> tuple[BaseAPIClient, LocalDataHandler]:
    """
    Fetch raw datasets from the configured API source and persist them to the
    project's raw-data directory using the decoupled architecture.

    Args:
        total_target_jobs (int): Approximate number of search jobs to target when
            collecting paginated search results. Defaults to 55555.

    Returns:
        tuple[BaseAPIClient, LocalDataHandler]: The initialized API client and 
        storage handler instances after the fetch operation completes, 
        making chaining or later inspection convenient.

    Raises:
        ValueError: If API credentials are missing or `total_target_jobs` is
        not positive.

    Example:
    ```
        api_client, storage = get_data(total_target_jobs=25000)
    ```
    """
    if total_target_jobs <= 0:
        raise ValueError("'total_target_jobs' must be a positive integer.")

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
        "gb", "us", "at", "au", "be", "br", "ca", "ch", "de", 
        "es", "fr", "in", "it", "mx", "nl", "nz", "pl", "sg", "za"
    ]
    

    print("\nStarting Fetching for Search...")
   
    # ---------- Fetching
    total_pages_budget = max(1, total_target_jobs // 50)
    
    search_data = api_client.request(
        method = "GET",
        url = "{target}/search/{page}",
        is_paginated = True,
        targets = countries,
        weight_endpoint_template = "{target}/search/1",
        total_pages_budget = total_pages_budget,
        extraction_key = "results",
        count_extraction_key = "count",
        params = {
            "results_per_page": 50,
            "sort_by": "date"
        }
    )
    
    storage.save_payload_to_json(search_data, file_name = "search")
    
    return api_client, storage


def clean_data(dataframe: SearchFile):
    """
    Run the end-to-end cleaning and enrichment pipeline for the search dataset.

    Notes:
        The pipeline standardizes metadata, normalizes categorical fields,
        translates non-English content, imputes missing values, enriches
        coordinates, converts salaries to USD, handles salary outliers, and
        reorders the final columns for downstream consumption.

    Args:
        dataframe (SearchFile): The raw or partially processed search dataset to
            clean.

    Returns:
        SearchFile: A cleaned search dataset ready to be exported or used in
        later analysis steps.

    Example:
    ```
        search_dataframe = SearchFile()
        cleaned = clean_data(search_dataframe)
    ```
    """
    # ---------- cleaning
    search_dataframe = dataframe


    # --- Metadata Cleaning
    search_dataframe.drop_CLASS_columns(inplace = True)
    search_dataframe.drop(columns = ['adref'], inplace = True)
    search_dataframe.drop(columns = ['salary_is_predicted'], inplace = True)
    search_dataframe.drop(columns = ['redirect_url'], inplace = True)

    search_dataframe.rename_all_DOTS(inplace = True)
    search_dataframe.rename_by_substring("display_name", "name", inplace = True)


    # --- Company Name
    search_dataframe = search_dataframe.dropna(subset = ['company_name']).reset_index(drop = True)


    # --- Created date
    search_dataframe['created'] = pd.to_datetime(search_dataframe['created'])
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


    # --- Drop duplicates
    search_dataframe.drop_duplicates(inplace = True)


    # --- Title
    with open(os.path.join(current_dir, 'Cache Data/Manual/tech_replacements.json'), 'r', encoding = 'utf-8') as f:
        tech_replacements = json.load(f)

    search_dataframe['title'] = search_dataframe['title'].replace(tech_replacements, regex = True)

    search_dataframe.truncate_after_substring(
        regex = False,
        inplace = True,
        title = [' - ', ' -', '- ', ' (', ' , ', ' | ', ': ', '[', '! ', ' _', '_ ']
    )

    search_dataframe['title'] = search_dataframe['title'].str.replace(r'[^\w\s\-]', '', regex = True)
    search_dataframe['title'] = search_dataframe['title'].str.replace(r'_', '', regex = True)

    search_dataframe['title'] = search_dataframe['title'].str.replace(r'(?i)\bIn\b', '', regex = True)
    search_dataframe['title'] = search_dataframe['title'].str.replace(r'(?i)\b[UO]\b', '', regex = True)

    search_dataframe['title'] = search_dataframe['title'].str.replace(r'\s+', ' ', regex = True).str.strip(' -').str.title()

    search_dataframe = search_dataframe[search_dataframe['title'].str.len() <= 35].reset_index(drop = True)

    with open(os.path.join(current_dir, 'Cache Data/Manual/reverse_tech_replacements.json'), 'r', encoding = 'utf-8') as f:
        reverse_tech_replacements = json.load(f)

    search_dataframe['title'] = search_dataframe['title'].replace(reverse_tech_replacements, regex = True)

    search_dataframe.loc[search_dataframe['country'] == 'US'] = search_dataframe.loc[
        search_dataframe['country'] == 'US'
    ].truncate_after_substring(regex = False, title = ' In ')

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
        'basic_skills',
        inplace = True
    )

    return search_dataframe



# --- Main Execution

if __name__ == "__main__":
    search_dataframe = SearchFile()
    
    client, handler = get_data(total_target_jobs = 55555)

    search_dataframe = clean_data(dataframe = search_dataframe)

    
    search_dataframe.save_to_json(
        os.path.join(os.path.dirname(current_dir), 'Shared Data/search_data.json'),
        orient = 'records',
        force_ascii = False,
        indent = 4
    )

    sql_data_types = {
        'id': NVARCHAR(255),
        'title': NVARCHAR(500),
        'category_label': NVARCHAR(255),
        'category_tag': NVARCHAR(255),
        'seniority_level': NVARCHAR(100),
        'company_name': NVARCHAR(500),
        'created': DateTime(),             
        'created_year': Integer(),
        'contract_type': NVARCHAR(100),
        'contract_time': NVARCHAR(100),
        'salary_min': Float(),
        'salary_max': Float(),
        'salary_avg': Float(),
        'latitude': Float(),
        'longitude': Float(),
        'state_or_gov': NVARCHAR(255),
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