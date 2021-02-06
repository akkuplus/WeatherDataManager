import logging
import logging.handlers
import pathlib
import configparser
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker


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

    def __init__(self):

        self.logger = None
        self.logger = MyLogger()
        self.logger.setup_handlers()

        self.connection = None
        self.stations = None
        self.measures = None
        self.metadata = None

        try:
            #TODO: FIX
            connect_info = get_setting(["mysql","host"])
            self.logger.debug("Imported settings from file configfile")
        except Exception:
            self.logger.exception(f"Error reading settings")

        try:
            self.connect_database()
        except Exception:
            self.logger.exception(f"Error connecting to database with settings")

    def connect_database(self):
        """Connect to database."""
        connect_string = get_setting(["mysql", "drivername"]) + "://"\
                         + get_setting(["mysql", "user"]) + ":"\
                         + get_setting(["mysql", "passwd"]) + "@"\
                         + get_setting(["mysql", "host"]) + ":"\
                         + get_setting(["mysql", "port"]) + "/"\
                         + get_setting(["mysql", "database_name"])
        self.logger.info(f"Connecting to: {get_setting(['mysql', 'host'])}")

        engine = db.create_engine(connect_string)  # create connection to database
        self.connection = engine.connect()  # connect for interaction with database
        self.metadata = db.MetaData()

        self.logger.info(f"Successful connected to database. Database contains tables: {self.test_query()}")

    def test_query(self):
        return self.connection.execute("show tables;").fetchall()

    def create_table_stations(self):
        """Create schema for table of weather data stations."""

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
        try:
            self.stations.create(self.connection, checkfirst=True)  # Create the table
            self.logger.info(f"Created a new table 'Wetterstation'")
        except Exception:
            self.logger.exception("Error creating Table in Database")

    def create_table_measures(self, table_name=None):
        """Create schema for temporary table of weather data measures that extend existing data measures."""

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
        try:
            self.measures.create(self.connection, checkfirst=True)  # Create the table
            self.logger.info(f"Created a new table 'Wettermessung'")
        except Exception:
            self.logger.exception("Error creating Table in Database")

    def reset_temporal_table(self):
        """Recreate temporal weather resources table with identical schema in database."""

        try:
            session = sessionmaker()
            session.configure(bind=self.connection)
            temporary_session = session()

            # create schema for table measure
            # TODO: request name-parameter from configuration_file
            self.create_table_measures('temp')

            # reset the temporary weather data table in database pertaining the schema
            self.measures.drop(self.connection, checkfirst=True)
            temporary_session.commit()

            self.measures.create(self.connection, checkfirst=True)
            temporary_session.commit()
            self.logger.info(f"Reset table '{self.measures.name}' consisting of 'Wettermessung'")
        except Exception:
            self.logger.exception("Error transferring DataFrame to Database")


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
