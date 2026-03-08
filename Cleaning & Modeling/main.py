import pandas as pd
from get_data import *
from cleaning import *

def run_pipeline():
    #------------- fetch data
    fetcher = DataFetcher()

    fetcher.fetch_and_save_data()


    #------------- cleaning
    search_dataframe = SearchFile()

    search_dataframe.drop_CLASS_columns(inplace = True)
    search_dataframe.drop(columns = 'adref', inplace = True)
    