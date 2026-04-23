import os
import logging
import uvicorn
import pandas as pd
from fastapi import FastAPI
from dotenv import load_dotenv

from Analytics import Analyzer
from Services import PageBuilder
from Routes import DynamicPageRouter
from Configs import get_home_page_config, get_explore_page_config



logging.basicConfig(
    level=logging.INFO,
    format = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
)


current_dir = os.path.dirname(os.path.abspath(__file__))

load_dotenv(
    dotenv_path = os.path.join(
        os.path.dirname(
            current_dir
        ),
        '.env'
    )
)


app = FastAPI(title = "Data Analytics Dashboard API")


try:
    df = pd.read_json(os.path.join(os.path.dirname(current_dir), "Shared Data/search_data.json")) 
    analyzer = Analyzer(df)
    
    print("Analyzer Engine Loaded Successfully!")

except Exception as e:
    print(f"Error loading data: {e}")
    
    analyzer = Analyzer(pd.DataFrame())



home_page_config = get_home_page_config(analyzer)
explore_page_config = get_explore_page_config(analyzer)



app.include_router(
    DynamicPageRouter(path = "/home", builder = PageBuilder(home_page_config)).router, 
    prefix = "/api",
    tags = ["Dashboard Pages"]
)

app.include_router(
    DynamicPageRouter(path = "/explore", builder = PageBuilder(explore_page_config)).router, 
    prefix = "/api",
    tags = ["Dashboard Pages"]
)


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host = os.getenv("ANALYST_HOST"),
        port = int(os.getenv("CHARTS_PORT")),
        reload = True
    )