import os
from datetime import datetime
import requests, json

## Class for extracting data from API
class E:
    def __init__(self, api_url: str, params: dict = None):
        self.api_url = api_url
        self.params = params if params else {}

    def fetch_data(self):
        try:
            response = requests.get(self.api_url, params=self.params, timeout=20)
            response.raise_for_status()
            raw_data = response.json()
            return raw_data.get('features', [])
        except Exception as e:
            print(f"Error fetching data: {e}")
            return []
