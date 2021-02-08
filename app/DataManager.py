"""
WeatherDataManager (WDM) retrieves data from url and imports newest values into a SQL table.
The weather data consists of location of stations and data observations. The WDM finds nearest
zip code next to each weather data station. The WDM enriches weather data with information
from weather data station, and appends the newest data to an existing SQL table.
"""

import numpy as np
import pandas as pd
from pathlib import Path
from sqlalchemy.sql import text
import zipfile

import app.Helper


class DataManager(object):
    """
    DataManager imports data of weather stations and weather measures,
    finds the nearest zip code for a given data station,
    and imports new weather measure to SQL table.
    """

    def __init__(self):

        #self.config = configparser.ConfigParser()
        #self.config.read(pathlib.Path.cwd().joinpath("config").joinpath("config.ini"))

        self.logger = app.Helper.MyLogger()
        self.logger.setup_handlers()
        self.database = app.Helper.MyConnector()

        self.lat_values = None
        self.long_values = None
        self.mapping_zipcode_coordinates = None
        self.saved_zipfile_path = None
        self.data_stations = None
        self.data_weather = None
        return

    def run(self):
        try:
            self.logger.info(f"DataManager running...")
            self.find_last_zipfile()

            self.import_weather_stations()
            self.import_weather_measures()
            self.import_locational_data()

            self.enrich_data_stations()
            self.get_nearest_zipcode()

            self.database.reset_temporal_table()
            self.sql_from_weather_measures()
            self.delete_old_weather_data()
            self.insert_new_weather_data()  # show changes in last date of weather date

            self.logger.info("DataManager closed.")
            print(f"\n========================= GOOD BYE ==============================")
        except Exception:
            self.logger.exception(f"Error while running DataManager")

    def find_last_zipfile(self):
        """
        Find last zip file containing weather resources that is available in subdirectory 'resources'.
        """

        # OLD: data_dir = self.config.get()
        data_dir = app.Helper.get_setting(["general", "data_dir"])

        self.saved_zipfile_path = Path.cwd().joinpath(data_dir)
        assert self.saved_zipfile_path, IsADirectoryError(f"Error generating path /{data_dir}/ to last zipfile")
        self.logger.debug(f"Searching for last zipfile in: {self.saved_zipfile_path}")

        try:
            # The filename of all saved zipfiles includes a date part.
            # So sort these filenames and take last available zipfile.
            self.saved_zipfile_path = sorted(self.saved_zipfile_path.glob(
                                            '????-??-??_wetterdaten_CSV.zip'),
                                            reverse=True)[0]
            if not self.zipfile_exists():
                raise FileNotFoundError("Can't find a file with the pattern: '????-??-??_wetterdaten_CSV.zip'")
            else:
                self.logger.info(f"Using last zipfile: '{self.saved_zipfile_path}'")

        except FileNotFoundError:
            self.logger.exception()

    def zipfile_exists(self):
        """
        Ensure a given zipfile name is a valid file,
        contains some information,
        and has around 800 kB.
        """

        path_to_file = Path(self.saved_zipfile_path)
        if path_to_file.is_file() and path_to_file.stat().st_size > 800000:
            return True
        else:
            self.logger.warning(f"Can't determine a valid file: '{self.saved_zipfile_path}'")
            return False

    def import_weather_stations(self):
        """
        Import weather stations and corresponding data
        from locally saved zip file.
        """

        #OLD file_name = 'wetterdaten_Wetterstation.csv'  # given filename in zip
        file_name = app.Helper.get_setting(["general", "CSV_Name_Wetterstation"])

        # create DataFrame
        try:
            with zipfile.ZipFile(self.saved_zipfile_path) as local_zip:
                with local_zip.open(file_name) as csv_file:
                    self.data_stations = pd.read_csv(
                        csv_file,
                        delimiter=";",  # ensure utf-encoding, ensure delimiter ";"
                        index_col=False,
                        header=0,
                        decimal=",",
                        encoding='cp1250')  # convert typical german encoding
            del self.data_stations["Unnamed: 6"]  # delete last column because of trailing ";" in csv file
            self.logger.info("Imported into DataFrame")
        except ImportError:
            self.logger.exception(f"Can't import {file_name} into DataFrame.")

    def import_weather_measures(self):
        """
        Import weather measures at a given station. The corresponding data is from saved zip file.
        """

        # OLD file_name = 'wetterdaten_Wettermessung.csv'  # given filename in zip
        file_name = app.Helper.get_setting(["general", "CSV_Name_Wettermessung"])

        # create DataFrame
        try:
            with zipfile.ZipFile(self.saved_zipfile_path) as local_zip:
                with local_zip.open(file_name) as csv_file:
                    self.data_weather = pd.read_csv(
                        csv_file,
                        parse_dates=["Datum"],
                        delimiter=";",  # ensure utf-encoding, ensure delimiter ";"
                        index_col=False,
                        header=0,
                        decimal=",",
                        encoding='cp1250')  # convert typical german encoding
            del self.data_weather["Unnamed: 14"]  # delete last column because of trailing ";" in csv file
            self.logger.info("Imported into DataFrame")
        except ImportError:
            self.logger.exception(f"Can't import {file_name} into a DataFrame.")

    def import_locational_data(self):
        """
        Import locational data from a local csv file that contains a mapping of zip codes
        and their corresponding coordinates.
        """

        #data_dir = self.config.get("general", "data_dir")
        data_dir = app.Helper.get_setting(["general", "data_dir"])
        file_name = data_dir + '/geodaten_de.csv'  # filename of static mapping resources,
        # that relates zip codes to coordinates in Germany
        new_columns = ['Plz', 'Ort', 'Latitude', 'Longitude']

        # parse zip code with a leading zero:
        # def cast_to_str(x):
        #    if len(x) > 4:
        #        return str(x)
        #    else:
        #        return "0" + str(x)

        # create DataFrame
        try:
            self.mapping_zipcode_coordinates = pd.read_csv(
                file_name,
                delimiter=";",  # ensure utf-encoding, ensure delimiter ";"
                index_col=False,
                header=0,
                names=new_columns,
                dtype={
                    'Plz': np.str,
                    'Ort': np.str,
                    'Latitude': np.float,
                    'Longitude': np.float},
                encoding='cp1250'  # converts german encoding
                # converters = {"Plz": cast_to_str}
            )
            self.logger.info("Imported into DataFrame")
        except ImportError:
            self.logger.exception(f"Can't import {file_name} to DataFrame 'mapping_zipcode_coordinates'.")

    def enrich_data_stations(self):
        """Appends four columns to existing DataFrame 'data_stations'."""

        try:
            appended_columns = self.mapping_zipcode_coordinates.columns + "_matched"

            self.data_stations.loc[:, str(appended_columns[0])] = np.nan
            self.data_stations.loc[:, str(appended_columns[1])] = np.nan
            self.data_stations.loc[:, str(appended_columns[2])] = np.nan
            self.data_stations.loc[:, str(appended_columns[3])] = np.nan
            self.logger.debug("Appended columns to DataFrame 'data_stations'")
        except Exception:
            self.logger.exception("Could not append columns to DataFrame 'data_stations'.")

    def find_nearest_zipcode(self, row):
        """ Find the nearest zip code for a given data station by minimal distance of coordinates."""

        # a row describes a data_station
        station_para_lat = row["Geo_Breite"]
        station_para_long = row["Geo_Laenge"]

        # use already extracted numpy-arrays of coordinates for all zip codes,
        # and subtract the coordinate of a data station
        lat = self.lat_values - station_para_lat
        long = self.long_values - station_para_long

        # Calculate all distances for a given data station,
        # find the minimal distance, and return the corresponding zip code
        # as the nearest zip code for the data station.
        distance = abs(long) + abs(lat)
        nearest_location = self.mapping_zipcode_coordinates.iloc[np.argmin(distance)]

        return nearest_location

    def get_nearest_zipcode(self):
        """Calculate the nearest zip code
         that has the minimal distance to a data station."""

        try:
            self.lat_values = self.mapping_zipcode_coordinates["Latitude"].values
            self.long_values = self.mapping_zipcode_coordinates["Longitude"].values

            self.data_stations[["Plz_matched", "Ort_matched", "Longitude_matched", "Latitude_matched"]] \
                = self.data_stations.apply(self.find_nearest_zipcode, axis=1)
            self.logger.info("Mapped data_stations to zipcodes")
        except Exception:
            self.logger.exception("Can't map a data_station to a zipcode")

    def sql_from_weather_measures(self):
        try:
            table_name = "temporal_measures"
            self.data_weather.to_sql(table_name, con=self.database.connection, if_exists='replace', index=False)
            self.logger.info(f"Transferred DataFrame containing weather data to temporary table '{table_name}'")
        except Exception:
            self.logger.exception("Error transferring DataFrame to Database")

    def sql_from_weather_stations(self):
        """
        Helper-method to import weather station data into sql table.
        """
        try:
            table_name = 'measures'
            self.data_stations.to_sql(table_name, con=self.database.connection, if_exists='replace', index=False)
            self.logger.info(f"Transferred DataFrame 'stations' to Database: '{self.database.connection}'")
        except Exception:
            self.logger.exception("Error transferring DataFrame to Database")

    def delete_old_weather_data(self):
        """Deletes all existing data rows in temporary table that have an
        identical combination of Station_ID and Datum within the complete database table"""
        temporal_table_name = "temporal_measures"
        try:
            self.database.connection.execute(
                text(f"DELETE FROM {temporal_table_name} WHERE ('{temporal_table_name}.Stations_ID', '{temporal_table_name}.Datum') "
                     "IN (SELECT DISTINCT 'Stations_ID', 'Datum' FROM measures);"
                     ))
            self.logger.debug("Cleared duplicate data from temporary table")
        except Exception:
            self.logger.exception("Error clearing duplicate data from temporary table")

    def insert_new_weather_data(self):

        temporal_table_name = "temporal_measures"
        try:
            # GET last Date before Insert
            latest_dates = {
                "measure_before": self.database.connection.execute(text("SELECT Max(Datum) from measures;")).fetchall(),
                "temporal": self.database.connection.execute(text(f"SELECT Max(Datum) from {temporal_table_name};")).fetchall()
            }

            # Insert transaction
            self.database.connection.execute(
                text(f"INSERT INTO Wettermessung SELECT * FROM {temporal_table_name};")
            )

            # GET last Date after Insert
            latest_dates["measure_after"] = self.database.connection.execute(text("SELECT Max(Datum) from measures;")).fetchall()

            # SHOW newer dates
            self.logger.info(f"Updated weather data: to new LATEST date {latest_dates['measure_after']} " 
                             f"from old lasted date {latest_dates['measure_before']}")
            # print("Updated weather data measure. \n"
            # "├ Last date before update: {before}\n"
            # "└ Last date after update: {after}\n".format(before=latest_dates["measure_before"],
            #        after=latest_dates["measure_after"]))
        except Exception:
            self.logger.exception("Error updating/integrating new weather_data into permanent database table {}")

