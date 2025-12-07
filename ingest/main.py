from extract import E
from load_raw import L
from datetime import datetime

api_url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
params = {
    "format": "geojson",
    "starttime": "1975-01-01",
    "endtime": datetime.now().strftime("%Y-%m-%d"),
    "minmagnitude": "5",
    "limit": 20000
}

db_config = {
    "host": "ep-autumn-hall-a1sskzm8.ap-southeast-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_Z2ih1qpKHusN",
    "port": 5432
}

def main():


    extractor = E(api_url, params)
    data = extractor.fetch_data()
    try:
        print(type(data))
        print(len(data))
    except Exception as e:
        print(f"Error printing data: {e}")

    loader = L(db_config, schema='raw', table='usgs_raw')
    loader.load_raw(data)
    print("Data ingestion completed.")

if __name__ == "__main__":
    main()  