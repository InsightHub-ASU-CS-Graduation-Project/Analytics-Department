from fetching import *
from cleaning import *
from Libraries import *



current_dir = os.path.dirname(os.path.abspath(__file__))

load_dotenv(dotenv_path = os.path.join(current_dir,'.env'))



def get_data( fetcher: DataFetcher, total_target_jobs: int = 55555):
    # ---------- fetch data (Temporarily Disabled)
    fetcher = fetcher
    fetcher.fetch_and_save_data(total_target_jobs = total_target_jobs)

    return fetcher


def clean_data(dataframe: SearchFile):
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
        'latitude',
        'longitude',
        'state_or_gov',
        'country',
        'basic_skills',
        inplace = True
    )

    return search_dataframe



# --- Main Execution

fetcher = DataFetcher()
# fetcher = get_data(fetcher = fetcher, total_target_jobs = 55555)




search_dataframe = SearchFile()
search_dataframe = clean_data(dataframe = search_dataframe)