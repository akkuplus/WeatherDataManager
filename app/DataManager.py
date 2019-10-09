"""
WeatherDataManager (WDM) retrieves data from url and imports newest values into a SQL table.
The data consists of weather data stations and weather data measures. The WDM finds nearest
zip code next to each weather data station. The WDM enriches weather data with information
from weather data station, and appends the newest data to an existing SQL table.
"""

class DataManager(object):
    """DataManager imports data of weather stations and weather measures,
    finds the nearest zip code for a given data station,
    and imports new weather measure to SQL table."""

    def __init__(self):
        import pathlib
        import configparser

        self.config = configparser.ConfigParser()
        self.config.read(pathlib.Path.cwd().joinpath("config").joinpath("config.ini"))

        self.logger = None
        self.get_logger()

        self.connection = None
        self.data_stations = None
        self.data_weather = None
        self.engine = None
        self.lat_values = None
        self.long_values = None
        self.mapping_zipcode_coordinates = None
        self.metadata = None
        self.saved_zipfile_path = None

        # TODO: request name-parameters from configuration_file
        self.stations = None
        self.measures = None

    def run(self):
        try:
            self.logger.info(f"DataManager running...")
            self.find_last_zipfile()
            self.connect_database()

            self.import_weather_stations()
            self.import_weather_measures()
            self.import_locational_data()

            self.enrich_data_stations()
            self.get_nearest_zipcode()

            self.reset_temporal_table()
            self.sql_from_weathermeasures()
            self.delete_old_weather_data()
            self.insert_new_weather_data()  # show changes in last date of weather date

            self.logger.info("DataManager closed.")
        except Exception:
            self.logger.exception(f"Error while running DataManager")

    def get_logger(self):
        import logging
        import logging.handlers

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        # TODO: save handlers and options in config-file

        # create console handler with a higher log level
        consolhandler = logging.StreamHandler()
        consolhandler.setLevel(logging.INFO)
        consol_formatter = logging.Formatter(
            '%(levelname)s function %(funcName)s: %(message)s'
            )
        consolhandler.setFormatter(consol_formatter)
        self.logger.addHandler(consolhandler)

        # create file handler which logs on level warnings and above
        filehandler = logging.FileHandler('config/error_log.txt')
        filehandler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - function:%(funcName)s - %(message)s')
        filehandler.setFormatter(file_formatter)
        self.logger.addHandler(filehandler)

        # TODO: implement email logging handler
        if self.config.getboolean("error", "send_email"):
            self.logger.info(f"Using E-Mail Logging handler")
            # create email handler which notifies on level errors and above

            error_handler = logging.handlers.SMTPHandler(
                mailhost=("localhost", 25),
                fromaddr="me@me.de",
                toaddrs=[self.config.get("error", "recipients")],
                subject="Error WeatherDataManager",
            )
            error_handler.setLevel(logging.WARNING)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - function:%(funcName)s - %(message)s')
            # error_handler.setFormatter(logging.Formatter(formatter))
            error_handler.setFormatter(formatter)
            self.logger.addHandler(error_handler)
            self.logger.error("TEST")

    def find_last_zipfile(self):
        """Finds the last zip file containing weather resources that is available in subdirectory 'resources'."""
        from pathlib import Path

        data_dir = self.config.get("general", "data_dir")
        self.saved_zipfile_path = Path.cwd().joinpath(data_dir)
        assert self.saved_zipfile_path, IsADirectoryError(f"Error generating path /{data_dir}/ to last zipfile")
        self.logger.debug(f"Searching for last zipfile in: {self.saved_zipfile_path}")

        try:
            # The filename of all saved zipfiles includes a date part.
            # Thus, sort those filenames and take last available zipfile.
            self.saved_zipfile_path = sorted(self.saved_zipfile_path.glob(
                '????-??-??_wetterdaten_CSV.zip'),
                reverse=True)[0]
            if not self.last_zipfile_exists():
                raise FileNotFoundError("Can't find a file with the pattern: '????-??-??_wetterdaten_CSV.zip'")
            else:
                self.logger.info(f"Using last zipfile: '{self.saved_zipfile_path}'")

        except FileNotFoundError:
            self.logger.exception()

    def last_zipfile_exists(self):
        """Ensure a given zipfile name is a valid file and contains
         some information and having around 800 kB in subfolder '/resources/'."""
        from pathlib import Path

        try:
            path_to_file = Path(self.saved_zipfile_path)
            if path_to_file.is_file() and path_to_file.stat().st_size > 800000:
                return True
            else:
                self.logger.warning(f"Can't determine a valid file: '{self.saved_zipfile_path}'")
                return False
        except Exception:
            self.logger.exception()

    def connect_database(self):
        """Connect to database."""
        import sqlalchemy as db

        try:
            database_path = self.config.get("general", "data_dir") + "/" +self.config.get("sqlite", "database")
            sqlite_db = {'drivername': 'sqlite',
                'database': database_path}
            self.db_conn = db.engine.url.URL(**sqlite_db)
            self.engine = db.create_engine(self.db_conn)  # create connection to database
            self.connection = self.engine.connect()  # connect for interaction with database
            self.metadata = db.MetaData()
            self.logger.info(f"Connected to database: {self.engine}")
        except Exception:
            self.logger.exception(f"Error connecting to database")

    def create_table_stations(self):
        """Create schema for table of weather data stations."""
        import sqlalchemy as db

        # TODO: request name-parameter from configuration_file
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
        self.logger.info(f"Created a new table 'stations'")

    def create_table_measures(self, table_name=None):
        """Create schema for temporary table of weather data measures that extend existing data measures."""
        import sqlalchemy as db

        if table_name is None:
            table_name = input("Name of new table for weather data measures, e.g. 'temp'? ")

        # TODO: request name-parameter from configuration_file
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
        self.logger.info(f"Created a new measures table '{table_name}'")



    def import_weather_stations(self):
        """Import weather stations and corresponding data
        from current locally saved zip file."""
        import pandas as pd
        import zipfile

        file_name = 'wetterdaten_Wetterstation.csv'  # given filename in zip

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
        """Import weather measures at a given station. The corresponding data is from saved zip file."""
        import pandas as pd
        import zipfile

        file_name = 'wetterdaten_Wettermessung.csv'  # given filename in zip

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
        """Import locational data from a local csv file
         that contains a mapping of zip codes
          and their corresponding coordinates."""
        import pandas as pd
        import numpy as np

        file_name = 'resources/geodaten_de.csv'  # filename of static mapping resources,
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
                encoding='cp1250'  # converts typical german encoding
                # converters = {"Plz": cast_to_str}
            )
            self.logger.info("Imported into DataFrame")
        except ImportError:
            self.logger.exception(f"Can't import {file_name} to DataFrame 'mapping_zipcode_coordinates'.")

    def enrich_data_stations(self):
        """Appends four columns to existing DataFrame 'data_stations'."""
        import numpy as np

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

        try:
            self.lat_values = self.mapping_zipcode_coordinates["Latitude"].values
            self.long_values = self.mapping_zipcode_coordinates["Longitude"].values

            self.data_stations[["Plz_matched", "Ort_matched", "Longitude_matched", "Latitude_matched"]] \
                = self.data_stations.apply(self.find_nearest_zipcode, axis=1)
            self.logger.info("Mapped data_stations to zipcodes")
        except Exception:
            self.logger.exception("Can't map a data_station to a zipcode")

    def sql_from_weatherstations(self):
        try:
            self.data_stations.to_sql('stations', con=self.engine, if_exists='append', index=False)
            self.logger.info(f"Transferred DataFrame 'stations' to Database: '{self.engine}'")
        except Exception:
            self.logger.exception("Error transferring DataFrame to Database")

    def sql_from_weathermeasures(self):
        try:
            table_name = self.measures.fullname  # for example: table_name = "temp"
            self.data_weather.to_sql(table_name, con=self.engine, if_exists='replace', index=False)
            self.logger.info(f"Transferred DataFrame to Table '{table_name}' in Database '{self.engine}'")
        except Exception:
            self.logger.exception("Error transferring DataFrame to Database")

    def reset_temporal_table(self):
        """Recreate temporal weather resources table with identical schema in database."""
        from sqlalchemy.orm import sessionmaker

        try:
            session = sessionmaker()
            session.configure(bind=self.engine)
            temporary_session = session()

            # create schema for table measure
            # TODO: request name-parameter from configuration_file
            self.create_table_measures('temp')

            # reset the temporary weather data table in database pertaining the schema
            self.measures.drop(self.engine, checkfirst=True)
            temporary_session.commit()

            self.measures.create(self.engine, checkfirst=True)
            temporary_session.commit()
            self.logger.info(f"Reset table '{self.measures.name}'")
        except Exception:
            self.logger.exception("Error transferring DataFrame to Database")

    def delete_old_weather_data(self):
        """Deletes all existing data rows in temporary table that have an
        identical combination of Station_ID and Datum in the full database table"""
        from sqlalchemy.sql import text

        try:
            self.engine.execute(
                text("DELETE FROM temp WHERE (Stations_ID, Datum) "
                     "IN (SELECT DISTINCT Stations_ID, Datum FROM measures);"
                     ))
            self.logger.debug("Cleared duplicate data from temporary table")
        except Exception:
            self.logger.exception("Error clearing duplicate data from temporary table")

    def insert_new_weather_data(self):
        from sqlalchemy.sql import text

        # TODO: get information from config_file
        try:
            latest_dates = {"measure_before": self.engine.execute(text("SELECT Max(Datum) from measures;")).fetchall(),
                        "temporal": self.engine.execute(text("SELECT Max(Datum) from temp;")).fetchall()}

            self.engine.execute(
                text('INSERT INTO measures SELECT * FROM temp;'))
            latest_dates["measure_after"] = self.engine.execute(text("SELECT Max(Datum) from measures;")).fetchall()
            self.logger.info(f"Updated weather data: to last date {latest_dates['measure_after']} " \
                             f"from last date {latest_dates['measure_before']}")
            # print("Updated weather data measure. \n"
            # "├ Last date before update: {before}\n"
            # "└ Last date after update: {after}\n".format(before=latest_dates["measure_before"],
            #        after=latest_dates["measure_after"]))
        except Exception:
            self.logger.exception("Error updating/integrating new weather_data into permanent database table {}")