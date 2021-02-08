import logging
import logging.handlers
import pathlib
import configparser
from sqlalchemy import create_engine, Column, DateTime, Float, Integer, MetaData, Table, String
from sqlalchemy.orm import sessionmaker, scoped_session
#from sqlalchemy import Table, Column, Integer, String,


def get_setting(value):
    configs = configparser.ConfigParser()
    path = pathlib.Path(".").absolute().joinpath("config").joinpath("config.ini")
    successfully_read = configs.read(str(path))
    if len(successfully_read) > 0:
        return configs.get( value[0], value[1] )
    else:
        return None


def get_project_path(project_path=None):
    if project_path is None:
        project_path = pathlib.Path.cwd()
    if "app" in project_path.parts:
        project_path = project_path.parent
    return project_path


class MyConnector(object):

    def __init__(self, db_system_name: str = "postgresql"):

        self.logger = None
        self.logger = MyLogger()
        self.logger.setup_handlers()

        self.db_system_name = db_system_name
        self.connection = None
        self.engine = None
        self.stations = None
        self.measures = None
        self.metadata = None

        try:
            connect_info = get_setting([db_system_name,"drivername"])
            assert connect_info
            self.logger.debug(f"Imported settings for {connect_info} from configfile")
        except Exception as ex:
            self.logger.exception(f"Error reading settings. {ex}")

        try:
            if is_connected := self.connect_database(db_system_name):
                tables = self.test_query()
                self.logger.info(f"Successfully connected to database. Database contains tables: {tables}")
            else:
                self.logger.error(f"Error connecting to database.")
        except Exception:
            self.logger.exception(f"Error connecting to database with settings")

    def connect_database(self, db_system_name: str = "postgresql"):
        """Connect to database."""

        assert (db_system_name in ["postgresql", "mysql"]), "Name of dbms is unknown"

        DATABASE_URI = get_setting([db_system_name, "drivername"]) + "://"\
                         + get_setting([db_system_name, "user"]) + ":"\
                         + get_setting([db_system_name, "passwd"]) + "@"\
                         + get_setting([db_system_name, "host"]) + ":"\
                         + get_setting([db_system_name, "port"]) + "/"\
                         + get_setting([db_system_name, "database_name"])

        self.logger.info(f"Connecting to: {get_setting([db_system_name, 'host'])}")

        try:
            self.engine = create_engine(DATABASE_URI, echo=True)
            self.connection = self.engine.connect()
            self.metadata = MetaData()
        except Exception as ex:
            self.logger.warning(f"Error connecting to Database. {ex}")

        return True if self.connection else False


    def test_query(self):
        self.create_table_measures('temporal_measures')
        self.create_table_measures('measures')
        self.create_table_stations('stations')
        #self.metadata.create_all(self.engine)
        return self.engine.table_names()

    def create_table_stations(self, table_name="stations"):
        """Create schema for table of weather data stations."""

        self.stations = Table(
            table_name, self.metadata,
            Column('S_ID', Integer()),
            Column('Standort', String(255), nullable=False),
            Column('Geo_Breite', Float(), nullable=False),
            Column('Geo_Laenge', Float(), nullable=False),
            Column('Hoehe', Integer(), nullable=False),
            Column('Betreiber', String(255), nullable=False),
            Column('PLZ_matched', String(5), nullable=False),
            Column('Ort_matched', String(255), nullable=False),
            Column('Latitude_matched', Float(), nullable=False),
            Column('Longitude_matched', Float(), nullable=False),
            extend_existing=True
        )
        try:
            self.stations.create(self.connection, checkfirst=True)  # If NOT exists, create the table.
            self.logger.info(f"Created table 'stations'")
        except Exception:
            self.logger.exception("Error creating Table in Database")

    def create_table_measures(self, table_name="temp"):
        """Create schema for temporary table of weather data measures that extend existing data measures."""

        if table_name is None:
            table_name = input("Name of new table for weather data measures, e.g. 'temp'? ")

        self.measures = Table(
            table_name,
            self.metadata,
            Column('Stations_ID', Integer(), nullable=False, primary_key=True),
            Column('Datum', DateTime(), nullable=False, primary_key=True),
            Column('Qualitaet', Integer(), nullable=True),
            Column('Min_5cm', Float(), nullable=True),
            Column('Min_2m', Float(), nullable=True),
            Column('Mittel_2m', Float(), nullable=True),
            Column('Max_2m', Float(), nullable=True),
            Column('Relative_Feuchte', Float(), nullable=True),
            Column('Mittel_Windstaerke', Float(), nullable=True),
            Column('Max_Windgeschwindigkeit', Float(), nullable=True),
            Column('Sonnenscheindauer', Float(), nullable=True),
            Column('Mittel_Bedeckungsgrad', Float(), nullable=True),
            Column('Niederschlagshoehe', Float(), nullable=True),
            Column('Mittel_Luftdruck', Float(), nullable=True),
            extend_existing=True
        )
        try:
            self.measures.create(self.connection, checkfirst=True)  # If NOT exists, create the table.
            self.logger.info(f"Created table '{table_name}'")
        except Exception:
            self.logger.exception("Error creating Table in Database")

    def reset_temporal_table(self):
        """Recreate temporal weather resources table with identical schema in database."""

        try:
            session = sessionmaker()
            session.configure(bind=self.connection)
            temporary_session = session()

            # create schema for table measure
            self.create_table_measures('temporal_measures')

            # reset the temporary weather data table in database pertaining the schema
            self.measures.drop(self.connection, checkfirst=True)
            temporary_session.commit()

            self.measures.create(self.connection, checkfirst=True)
            temporary_session.commit()
            self.logger.info(f"Reset table '{self.measures.name}'")
        except Exception:
            self.logger.exception("Error transferring recreating Database")


class MyLogger(logging.Logger):

    def __init__(self):
        super().__init__(__name__)
        #self = logging.getLogger()
        self.setLevel(logging.DEBUG)

    def setup_handlers(self, path=None):
        # TODO: save handlers and options in config-file

        # create console handler with a higher log level ""info""
        consolhandler = logging.StreamHandler()
        consolhandler.setLevel(logging.INFO)
        consol_formatter = logging.Formatter(
            '%(levelname)s function %(funcName)s: %(message)s'
            )
        consolhandler.setFormatter(consol_formatter)
        self.addHandler(consolhandler)

        # create file handler which logs on the levels ""warnings"" and above
        log_file = get_setting(["error", "log"])
        log_file_path = pathlib.Path(get_project_path()).joinpath(log_file)
        assert log_file_path.exists(), "Path to Logfile does not exist."

        filehandler = logging.FileHandler(log_file_path)
        filehandler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - function:%(funcName)s - %(message)s')
        filehandler.setFormatter(file_formatter)
        self.addHandler(filehandler)

        # TODO: implement email logging handler, SCRATCH:
        if False:#self.config.getboolean("error", "send_email"):
            self.info(f"Using E-Mail Logging handler")
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
            self.addHandler(error_handler)
            self.error("TEST")

"""
OLD (obsolete because of get_settings() function:
class MyConfigurator(configparser.ConfigParser()):

    def __init__(self):
        super().__init__()
        #self = configparser.ConfigParser()
        path = pathlib.Path.cwd().parent.joinpath("config").joinpath("config.ini")
        print(path)
        self.settings = self.read(str(path))
        print(self.get("general", "data_dir"))

    def read_settings(self):
        path = pathlib.Path.cwd().parent.joinpath("config").joinpath("config.ini")
        print(path)
        self.read(str(path))
"""

if __name__ == "__main__":

    tempLogger = MyLogger()
    print (tempLogger)
    tempLogger.setup_handlers()
    tempLogger.info(f"Logging works!")

    #tempConnect = MyConnector()
