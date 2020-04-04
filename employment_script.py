import requests
from datetime import datetime    


url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

payload = """{"seriesid": ["SMU36000000000000001"], "startyear": "2019","endyear": "2020"}"""

headers = {
  'Content-type': 'application/json'
}

r = requests.request("POST", url, headers=headers, data = payload)

print(r)
