import urllib.request

url = "https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.csv?accessType=DOWNLOAD"
print("Downloading... this may take a few minutes (large file).")
urllib.request.urlretrieve(url, "nyc_collisions.csv")
print("Download complete.")