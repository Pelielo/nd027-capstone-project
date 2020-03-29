import requests
import json 

def concat_coordinates(latitude, longitude):
    return str(latitude) + ',' + str(longitude)

def reverse_geocode(latitude, longitude, api_key):
    url = "https://api.opencagedata.com/geocode/v1/json"
    lat_long = concat_coordinates(latitude, longitude)
    params = {'q': lat_long, 'key': api_key}
    return requests.request("GET", url, params=params)

def state_parser(json_str):
    return json.loads(json_str.text)['results'][0]['components']['state']

api_key = 'bd6d0a633f26459a87412f18e120616c'
latitude = '32.85'
longitude = '-84.63'


# print(response.text)
print(state_parser(reverse_geocode(latitude, longitude, api_key)))
