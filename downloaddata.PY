import urllib.request

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
urllib.request.urlretrieve(url, "yellow_tripdata_2024-01.parquet")

zones_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
urllib.request.urlretrieve(zones_url, "taxi_zone_lookup.csv")

print("Download complete.")