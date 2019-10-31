"""
WeatherDataManager (WDM) retrieves data from url and imports newest values into a SQL table.
The data consists of weather data stations and weather data measures. The WDM finds nearest
zip code next to each weather data station. The WDM enriches weather data with information
from weather data station, and appends the newest data to an existing SQL table.
"""

# TODO: docker-version
# TODO: check relevant ports
# TODO: roll-out strategy: docker, full setup or git pull?


import app.DataRequester
import app.DataManager
import app.Helper
import time


print(f"\n=======================WeatherDataManager========================="
#     f"\nimport app"
#      f"\nDR = app.DataRequester.DataRequester()"
#      f"\nDR.run()"
#      f"\n"
#      f"\n and afterwards call app.DataManager()"
      )


dr = app.DataRequester.DataRequester()
dr.run()
dm = app.DataManager.DataManager()
dm.run()
