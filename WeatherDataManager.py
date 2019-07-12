"""
WeatherDataManager (WDM) retrieves data from url and imports newest values into a SQL table.
The data consists of weather data stations and weather data measures. The WDM finds nearest
zip code next to each weather data station. The WDM enriches weather data with information
from weather data station, and appends the newest data to an existing SQL table.
"""


class DataRequester(object):
    """Requests and save distant weather data into a local zip file."""

    def __init__(self):
        self.last_zip_file = ""
        self.saved_zipfile_path = ""
        self.url = ""

    def get_saved_zipfile_path(self):
        return self.saved_zipfile_path

    def get_distant_filename(self):
        """Scrap HTML site to extract newest link."""
        from bs4 import BeautifulSoup
        import requests
        from urllib.parse import urljoin

        # extract current link in html file
        self.url = "https://dbup2date.uni-bayreuth.de/blocklysql/"  # static link
        html_file = "wetterdaten.html"  # static filename
        selfhtml_path = urljoin(self.url, html_file)

        resp = requests.get(selfhtml_path, verify=False)
        soup = BeautifulSoup(resp.content, features="html.parser")
        for link in soup.find_all('a', href=True):
            if "CSV" in link['href']:
                zip_file_url = link['href']  # example: "downloads\wetterdaten\2019-07-07_wetterdaten_CSV.zip"
                break

        # get distant zip file path
        if zip_file_url:
            self.last_zip_file = zip_file_url
        else:
            raise FileNotFoundError("Can't find a zipfile '*wetterdaten_CSV.zip' in HTML")

    def save_zip_locally(self):
        """Load last weather data saved in zip."""
        # example: url = 'https://dbup2date.uni-bayreuth.de/blocklysql/downloads/wetterdaten/2019-06-21_wetterdaten.zip'
        import requests
        import urllib.parse
        from pathlib import Path

        # load last distant zip file
        full_url_zip = urllib.parse.urljoin(self.url, self.last_zip_file)
        r = requests.get(full_url_zip, allow_redirects=True, verify=False)

        # save file locally: "/data/*.zip"
        zip_file_name = self.last_zip_file.rsplit(sep="/", maxsplit=1)[-1]  # split url by "/"
        # and take most right element
        self.saved_zipfile_path = Path.cwd().joinpath("data").joinpath(zip_file_name)

        try:
            open(self.saved_zipfile_path.name, 'wb').write(r.content)
            print("Saved {} from {}".format(self.saved_zipfile_path, full_url_zip))
            dir(self.saved_zipfile_path)
        except IOError as ioerr:
            print("Error writing {}".format(self.saved_zipfile_path))
            print(ioerr)


class DataManager(object):
    """DataManager imports data of weather stations and weather measures,
    finds the nearest zip code for a given data station,
    and imports new weather measure to SQL table."""

    def __init__(self):
        from pathlib import Path

        self.connection = None
        self.data_stations = None
        self.data_weather = None
        self.engine = None
        self.lat_values = None
        self.long_values = None
        self.mapping_zipcode_coordinates = None
        self.measures = None
        self.metadata = None
        self.saved_zipfile_path = None
        self.stations = None

        self.connect_database()
        self.find_last_zipfile()

        try:
            print("Found last zipfile {}".format(self.saved_zipfile_path.parts[-1]))
        except ValueError as value_error:
            print("Error using path to last zip file {}".format(self.saved_zipfile_path))
            print(value_error)

    def import_weather_stations(self):
        """Import weather stations and corresponding data
        from current locally saved zip file."""
        import pandas as pd
        import zipfile

        file_name = 'wetterdaten_Wetterstation.csv'  # given filename in zip

        try:
            # ensure utf-encoding, ensure delimiter ";"
            with zipfile.ZipFile(self.saved_zipfile_path) as local_zip:
                with local_zip.open(file_name) as csv_file:
                    self.data_stations = pd.read_csv(
                        csv_file,
                        delimiter=";",
                        index_col=False,
                        header=0,
                        decimal=",",
                        encoding='cp1250')  # convert typical german encoding

            del self.data_stations["Unnamed: 6"]  # delete last column because of trailing ";" in csv file
        except ImportError:
            print("Can't import {} to DataFrame 'data_stations'.".format(file_name))

    def import_weather_measures(self):
        """Import weather measures at a given station. The corresponding data is from saved zip file."""
        import pandas as pd
        import zipfile

        file_name = 'wetterdaten_Wettermessung.csv'  # given filename in zip

        try:
            # ensure utf-encoding, ensure delimiter ";"
            with zipfile.ZipFile(self.saved_zipfile_path) as local_zip:
                with local_zip.open(file_name) as csv_file:
                    self.data_weather = pd.read_csv(
                        csv_file,
                        parse_dates=["Datum"],
                        delimiter=";",
                        index_col=False,
                        header=0,
                        decimal=",",
                        encoding='cp1250')  # convert typical german encoding

            del self.data_weather["Unnamed: 14"]  # delete last column because of trailing ";" in csv file
            print(self.data_weather)
        except ImportError:
            print("Can't import {} to DataFrame 'data_stations'.".format(file_name))

    def import_locational_data(self):
        """Import locational data from a local csv file
         that contains a mapping of zip codes
          and their corresponding coordinates."""
        import pandas as pd
        import numpy as np

        file_name = 'data/geodaten_de.csv'  # filename of static mapping data,
        # that relates zip codes to coordinates in Germany
        new_columns = ['Plz', 'Ort', 'Latitude', 'Longitude']

        # parse zip code with a leading zero:
        # def cast_to_str(x):
        #    if len(x) > 4:
        #        return str(x)
        #    else:
        #        return "0" + str(x)

        try:
            # ensure utf-encoding, ensure delimiter ";"
            self.mapping_zipcode_coordinates = pd.read_csv(
                file_name,
                delimiter=";",
                index_col=False,
                header=0,
                names=new_columns,
                dtype={
                    'Plz': np.str,
                    'Ort': np.str,
                    'Latitude': np.float,
                    'Longitude': np.float},
                encoding='cp1250'  # converts typical german encoding
                # converters = {"Plz": cast_to_str}
            )
        except ImportError:
            print("Can't import {} to DataFrame 'mapping_zipcode_coordinates'.".format(file_name))

    def enrich_data_stations(self):
        """Appends four columns to existing DataFrame 'data_stations'."""
        import numpy as np

        try:
            appended_columns = self.mapping_zipcode_coordinates.columns + "_matched"

            self.data_stations.loc[:, str(appended_columns[0])] = np.nan
            self.data_stations.loc[:, str(appended_columns[1])] = np.nan
            self.data_stations.loc[:, str(appended_columns[2])] = np.nan
            self.data_stations.loc[:, str(appended_columns[3])] = np.nan
        except ValueError:
            print("Could not append columns to DataFrame 'data_stations'.")
        except Exception as err:
            print("Unexpected error:", err)

    def find_nearest_zipcode(self, row):
        """ Find the nearest zip code for a given data station by minimal distance of coordinates."""
        import numpy as np

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

        self.lat_values = self.mapping_zipcode_coordinates["Latitude"].values
        self.long_values = self.mapping_zipcode_coordinates["Longitude"].values

        self.data_stations[["Plz_matched", "Ort_matched", "Longitude_matched", "Latitude_matched"]] \
            = self.data_stations.apply(self.find_nearest_zipcode, axis=1)

        print(self.data_stations)

    def sql_from_weatherstations(self):
        print("Connected to Database '{}':".format(self.engine))
        self.data_stations.to_sql('stations', con=self.engine, if_exists='append', index=False)

    def sql_from_weathermeasures(self):
        print("Connected to Database '{}'".format(self.engine))
        table_name = self.measures.fullname  # for example: table_name = "temp"
        self.data_weather.to_sql(table_name, con=self.engine, if_exists='replace', index=False)
        print("Transferred DataFrame to table '{}'".format(table_name))

    def connect_database(self):
        """Connect to local SQLite database."""
        import sqlalchemy as db

        sqlite_db = {'drivername': 'sqlite', 'database': 'data/weather_repo.sqlite'} # if file in subfolder, don't start string for subfolder with "/"
        db_conn = db.engine.url.URL(**sqlite_db)

        self.engine = db.create_engine(db_conn)  # create connection to database
        self.connection = self.engine.connect()  # connect for interaction with database
        self.metadata = db.MetaData()
        print("Connected to Database '{}'".format(self.engine))

    def create_table_stations(self):
        """Create schema for table of weather data stations."""
        import sqlalchemy as db

        self.stations = db.Table(
            'stations',
            self.metadata,
            db.Column('S_ID', db.Integer()),
            db.Column('Standort', db.String(255), nullable=False),
            db.Column('Geo_Breite', db.Float(), nullable=False),
            db.Column('Geo_Laenge', db.Float(), nullable=False),
            db.Column('Hoehe', db.Integer(), nullable=False),
            db.Column('Betreiber', db.String(255), nullable=False),
            db.Column('PLZ_matched', db.String(5), nullable=False),
            db.Column('Ort_matched', db.String(255), nullable=False),
            db.Column('Latitude_matched', db.Float(), nullable=False),
            db.Column('Longitude_matched', db.Float(), nullable=False),
            extend_existing=True
        )

        self.stations.create(self.engine, checkfirst=True)  # Create the table

    def create_table_measures(self, table_name=None):
        """Create schema for temporary table of weather data measures that extend existing data measures."""
        import sqlalchemy as db

        if table_name is None:
            table_name = input("Name of new table for weather data measures, e.g. 'temp'? ")

        self.measures = db.Table(
            table_name,
            self.metadata,
            db.Column('Stations_ID', db.Integer(), nullable=False, primary_key=True),
            db.Column('Datum', db.DateTime(), nullable=False, primary_key=True),
            db.Column('Qualitaet', db.Integer(), nullable=True),
            db.Column('Min_5cm', db.Float(), nullable=True),
            db.Column('Min_2m', db.Float(), nullable=True),
            db.Column('Mittel_2m', db.Float(), nullable=True),
            db.Column('Max_2m', db.Float(), nullable=True),
            db.Column('Relative_Feuchte', db.Float(), nullable=True),
            db.Column('Mittel_Windstaerke', db.Float(), nullable=True),
            db.Column('Max_Windgeschwindigkeit', db.Float(), nullable=True),
            db.Column('Sonnenscheindauer', db.Float(), nullable=True),
            db.Column('Mittel_Bedeckungsgrad', db.Float(), nullable=True),
            db.Column('Niederschlagshoehe', db.Float(), nullable=True),
            db.Column('Mittel_Luftdruck', db.Float(), nullable=True),
            extend_existing=True
        )

        self.measures.create(self.engine, checkfirst=True)  # Creates the table

    def reset_temporal_table(self):
        """Recreate temporal weather data table with identical schema in database."""
        from sqlalchemy.orm import sessionmaker

        session = sessionmaker()
        session.configure(bind=self.engine)
        temporary_session = session()

        # create schema for table measure
        self.create_table_measures('temp')

        # reset the temporary weather data table in database pertaining the schema
        self.measures.drop(self.engine, checkfirst=True)
        temporary_session.commit()

        self.measures.create(self.engine, checkfirst=True)
        temporary_session.commit()
        print("Reset table '{}'".format(self.measures.name))

    def delete_old_weather_data(self):
        from sqlalchemy.sql import text
        self.engine.execute(
            text("DELETE FROM {} WHERE (Stations_ID, Datum) IN (SELECT DISTINCT Stations_ID, Datum FROM measures);"
                 .format(self.measures.name)))

    def insert_new_weather_data(self):
        from sqlalchemy.sql import text
        self.engine.execute(
            text('INSERT INTO measures SELECT * FROM temp;'))

    def find_last_zipfile(self, saved_filepath=""):
        """Finds the last zip file containings weather data that is available in subdirectory data."""
        from pathlib import Path

        self.path_to_data = Path.cwd().joinpath("data")
        print("Searching for last zipfile in: {}".format(self.path_to_data))

        # The filename of all saved zipfiles include the date.
        # Thus, sort those filenames and take last available zip-file-name.
        self.saved_zipfile_path = sorted(self.path_to_data.glob('????-??-??_wetterdaten_CSV.zip'), reverse=True)[0]


if __name__ == "__main__":

    # TODO: separate classes into files
    # TODO: ensure files contain some information and have around 800 kB
    # TODO:

    # from WeatherDataManager import (DataRequester, DataManager)

    dr = DataRequester
    dr.get_distant_filename(dr)
    dr.save_zip_locally(dr)

    dm = DataManager()
    dm.import_weather_stations()
    dm.import_weather_measures()
    dm.import_locational_data()

    dm.enrich_data_stations()
    dm.get_nearest_zipcode()

    dm.reset_temporal_table()
    dm.sql_from_weathermeasures()
    dm.delete_old_weather_data()
    #dm.insert_new_weather_data()



"""
    def find_last_zipfile(self, saved_filepath=""):
        from pathlib import Path

        #if not Path(saved_filepath).exists():
            # path = Path(path_zip_file_name)
            # zip_file_name = path.name
            # if not saved_filepath:
        saved_filepath = Path(saved_filepath)
        self.saved_zipfile_path = Path.cwd().joinpath("data")  # .joinpath(zip_file_name)
        print(self.saved_zipfile_path)
        self.saved_zipfile_path = sorted(saved_pathfile.glob('????-??-??_wetterdaten_CSV.zip'), reverse=True)[0]
        print("Found last file in directory: {}".format(self.saved_zipfile_path.parts()))
"""