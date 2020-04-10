# nd027-capstone-project

US city recomendation system

* City (location, description, state)
  * https://developers.google.com/adwords/api/docs/appendix/geotargeting
* Population 
  * https://simplemaps.com/data/us-cities
* Employment numbers
  * https://api.bls.gov/publicAPI/v2/timeseries/data/CEU0500000003
* Weather
  * https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data#GlobalLandTemperaturesByCity.csv
  * https://kilthub.cmu.edu/articles/Compiled_daily_temperature_and_precipitation_data_for_the_U_S_cities/7890488
* Safety
* cost of living, housing indicators, health care, traffic, crime and pollution.
  
## Getting Started

* add connections to airflow (s3_conn,aws_credentials)
* add variables to airflow (kaggle_username, kaggle_api_key, bls_api_key)