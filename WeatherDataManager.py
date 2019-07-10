"""
WeatherDataManager (WDM) retrieves data from url and imports newest values into a SQL table.
The data consists of weather data stations and weather data measures. The WDM finds nearest zip code next to each weather data station.
The WDM enriches weather data with information from weather data station, and appends the newest data to an existing SQL table
"""

class DataRequester(object):
    """Requests and save distant weather data into a local zip file."""

    def __init__(self):
        pass


    def get_saved_zipfile_path(self):
        return self.saved_zipfile_path


    def get_distant_filename(self):
        """Scrap HTML site to extract newest link."""
        from bs4 import BeautifulSoup
        import requests
        from urllib.parse import urljoin

        # extract current links in html file
        self.url = "https://dbup2date.uni-bayreuth.de/blocklysql/" # static link
        html_file = "wetterdaten.html" # static filename
        selfhtml_path = urljoin(self.url, html_file)

        resp = requests.get(selfhtml_path)
        soup = BeautifulSoup(resp.content, features="html.parser")
        for link in soup.find_all('a', href=True):
            if "CSV" in link['href']:
                zip_file_path = link['href'] # example: "downloads\wetterdaten\2019-07-07_wetterdaten_CSV.zip"
                break

        # get distant zip file path
        if zip_file_path:
            self.last_zip_file = zip_file_path
        else:
            raise FileNotFoundError("Can't find zipfile '*wetterdaten_CSV.zip' in HTML")


    def save_zip_locally(self):
        """Load last weather data saved in zip."""
        # example: url = 'https://dbup2date.uni-bayreuth.de/blocklysql/downloads/wetterdaten/2019-06-21_wetterdaten.zip'
        import requests
        import urllib.parse
        from pathlib import Path


        # load last distant zip file
        #full_url_zip = PurePath(self.url).joinpath(self.last_zip_file) # build full url to file
        full_url_zip = urllib.parse.urljoin(self.url, self.last_zip_file)
        print(full_url_zip)
        r = requests.get(full_url_zip, allow_redirects=True)
        #file_name = self.get_last_filename(r.headers.get('content-disposition'))

        # save file locally: "/data/*.zip"
        zip_file_name = self.last_zip_file.rsplit(sep="/", maxsplit=1)[-1] # split url by "/" and take most right element
        self.saved_zipfile_path = Path.cwd().joinpath("data").joinpath(zip_file_name)

        try:
            open(self.saved_zipfile_path, 'wb').write(r.content)
            print("Saved {}".format(self.saved_zipfile_path))
            dir(self.saved_zipfile_path)
        except IOError as ioerr:
            print("Error writing {}".format(self.saved_zipfile_path))
            print(ioerr)


class DataBaseManager(object):
    """Connect to Databases and inserts new weather data."""

    def __init__(self):
        self.connect_database()


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

        self.stations = db.Table('stations', self.metadata,
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


    def create_table_measures(self, table_name = None):
        """Create schema for temporary table of weather data measures that extend existing data measures."""
        import sqlalchemy as db

        if table_name is None:
            table_name = input("Name of new table for weather data measures, e.g. 'temp'? ")

        self.measures = db.Table(table_name, self.metadata,
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
        """ Recreate schema for temporal weather data table in database."""
        from sqlalchemy.orm import sessionmaker
        Session = sessionmaker()
        Session.configure(bind=self.engine)
        session = Session()

        #create schema for table measure
        self.create_table_measures('temp')

        # reset the temporary weather data table in database pertaining the schema
        self.measures.drop(self.engine, checkfirst=True)
        session.commit()

        self.measures.create(self.engine, checkfirst=True)
        session.commit()
        print("Reset table '{}'".format(self.measures.name))


    def delete_old_weather_data(self):
        from sqlalchemy.sql import text
        self.engine.execute(text('DELETE from "{}" where Datum in (SELECT DISTINCT Datum from measures);'.format(self.measures.name)))



class DataManager(object):
    """DataManager imports data of weather stations and weather measure, finds the nearest zip ode for a given data station, and import new wether measure to SQL table."""

    def __init__(self, saved_zipfile_path=""):
        from pathlib import Path

        if not saved_zipfile_path:
            self.saved_zipfile_path = Path.cwd().joinpath("data").joinpath("2019-07-02_wetterdaten_CSV.zip")
        else:
            self.saved_zipfile_path = Path(saved_zipfile_path)


    def import_weather_stations(self):
        """Import weather stations and corresponding data from current locally saved zip file."""
        import pandas as pd
        import zipfile

        file_name = 'wetterdaten_Wetterstation.csv' # given filename in zip

        try:
            # ensure utf-encoding, ensure delimiter ";"
            with zipfile.ZipFile(self.saved_zipfile_path) as local_zip:
                with local_zip.open(file_name) as csv_file:
                    self.data_stations = pd.read_csv(csv_file,
                        delimiter=";",
                        index_col=False,
                        header=0,
                        decimal=",",
                        encoding='cp1250')  # convert typical german encoding

            del self.data_stations["Unnamed: 6"] # delete last column because of trailing ";" in csv file
        except ImportError:
            print("Can't import {} to DataFrame 'data_stations'.".format(file_name))


    def import_weather_measures(self):
        """Import weather measures at a given station. The corresponding data is from saved zip file."""
        import pandas as pd
        import zipfile

        file_name = 'wetterdaten_Wettermessung.csv' # given filename in zip

        try:
            # ensure utf-encoding, ensure delimiter ";"
            with zipfile.ZipFile(self.saved_zipfile_path) as local_zip:
                with local_zip.open(file_name) as csv_file:
                    self.data_weather = pd.read_csv(csv_file,
                        parse_dates=["Datum"],
                        delimiter=";",
                        index_col=False,
                        header=0,
                        decimal=",",
                        encoding='cp1250')  # convert typical German encoding

            del self.data_weather["Unnamed: 14"] # delete last column because of trailing ";" in csv file
            print(self.data_weather)
        except ImportError:
            print("Can't import {} to DataFrame 'data_stations'.".format(file_name))


    def import_locational_data(self):
        """Import locational data from a local csv file that contains a mapping of zip codes and their corresponding coordinates."""
        import pandas as pd
        import numpy as np

        file_name = 'data/geodaten_de.csv' # local filename of static mapping data: relates zip codes to coordinates in Germany
        new_columns = ['Plz', 'Ort', 'Latitude', 'Longitude']

        # parse zip code with a leading zero:
        #def cast_to_str(x):
        #    if len(x) > 4:
        #        return str(x)
        #    else:
        #        return "0" + str(x)

        try:
            # ensure utf-encoding, ensure delimiter ";"
            self.mapping_zipcode_coordinates = pd.read_csv(file_name,
                delimiter=";",
                index_col=False,
                header=0,
                names=new_columns,
                dtype={'Plz': np.str, 'Ort': np.str, 'Latitude': np.float,
                    'Longitude': np.float},
                encoding='cp1250'  # converts typical German encoding
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

        # use already extracted numpy-arrays of coordinates for all zip codes and subtract the coordinate of a data station
        lat = self.lat_values - station_para_lat
        long = self.long_values - station_para_long

        # Calculate all distances for a given data station, find the minimal distance,
        # and return the corresponding zip code as the nearest zip code for the data station.
        distance = abs(long) + abs(lat)
        nearest_location = self.mapping_zipcode_coordinates.iloc[np.argmin(distance)]

        return nearest_location


    def get_nearest_zipcode(self):
        """Calculate the nearest zip code that has the minimal distance to a data station."""

        self.lat_values = self.mapping_zipcode_coordinates["Latitude"].values
        self.long_values = self.mapping_zipcode_coordinates["Longitude"].values

        self.data_stations[["Plz_matched", "Ort_matched", "Longitude_matched", "Latitude_matched"]] \
            = self.data_stations.apply(self.find_nearest_zipcode, axis=1)

        print(self.data_stations)


    def SQL_from_weatherstations(self, db: DataBaseManager):
        print("Connected to Database '{}':".format(db.engine))
        self.data_stations.to_sql('stations', con=db.engine, if_exists='append', index=False)


    def sql_from_weathermeasures(self, db: DataBaseManager):
        print("Connected to Database '{}'".format(db.engine))
        table_name = db.measures.fullname # for example: table_name = "temp"
        self.data_weather.to_sql(table_name, con=db.engine, if_exists='replace', index=False)
        print("Transferred DataFrame to table '{}'".format(table_name))


def find_last_zipfilename():
    from pathlib import Path

    saved_pathfile = None
    # path = Path(path_zip_file_name)
    # zip_file_name = path.name
    if not saved_pathfile:
        saved_pathfile = Path.cwd().joinpath("data")  # .joinpath(zip_file_name)
        saved_pathfile = sorted(saved_pathfile.glob('????-??-??_wetterdaten_CSV.zip'), reverse=True)[0]

    return saved_pathfile





if __name__ == "__main__":

    # TODO: separate classes into files
    # TODO: use last zip-file
    dr = DataRequester
    dr.get_distant_filename(dr)
    #dr.save_zip_locally(dr)
    #print( dr.get_saved_zipfile_path(dr) )

    dm = DataManager()
    dm.import_weather_stations()
    dm.import_weather_measures()
    dm.import_locational_data()

    dm.enrich_data_stations()
    dm.get_nearest_zipcode()

    #WeatherDataManager.find_last_zipfilename()

    dbm = DataBaseManager()
    dbm.reset_temporal_table()
    dm.sql_from_weathermeasures(dbm)
    dbm.delete_old_weather_data()
