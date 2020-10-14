import app.DataRequester
import app.DataManager
import app.Helper


print(f"\n=======================WeatherDataManager=========================")


dr = app.DataRequester.DataRequester()
dr.run()
dm = app.DataManager.DataManager()
dm.run()