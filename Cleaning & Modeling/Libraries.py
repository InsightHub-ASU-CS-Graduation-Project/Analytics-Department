import os
import time
import json
import spacy
import urllib
import requests
import pycountry
import numpy as np
import pandas as pd
import yfinance as yf
from typing import Self
from dotenv import load_dotenv
from spacy.matcher import Matcher
from sqlalchemy import create_engine
from geopy.geocoders import Nominatim
from deep_translator import GoogleTranslator
from langdetect import detect, DetectorFactory
from geopy.extra.rate_limiter import RateLimiter
from babel.numbers import get_territory_currencies
from sqlalchemy.types import NVARCHAR, Float, Integer, DateTime