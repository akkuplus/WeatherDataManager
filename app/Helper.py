import logging
import logging.handlers
import pathlib
import configparser
from socket import *
from sqlalchemy import create_engine, Column, DateTime, BigInteger, Float, Integer, MetaData, Table, String
from sqlalchemy.orm import sessionmaker, scoped_session


def get_setting(value: list):
    """

    :param value: List containing [<an empty list>], ["<section>"], ["<section>","<option in section>"]
    :return: all sections (if list empty),
                all option (if list contains section), or option (if list contains section and option)
    """
    configs = configparser.ConfigParser()
    project_path = get_project_path()
    path = pathlib.Path(project_path).absolute().joinpath("config").joinpath("config.ini")
    successfully_read = configs.read(str(path))

    assert len(successfully_read) > 0, "Keine Konfiguration geladen."

    if len(value) == 0:
        return configs.sections()
    elif len(value) == 1:
        return configs.options(value[0])  # returns all option in section
    elif len(value) == 2:
        return configs.get(value[0], value[1])  # returns given option ins section


def get_project_path(project_path=None):
    if project_path is None:
        project_path = pathlib.Path.cwd()
    if "app" in project_path.parts:
        project_path = project_path.parent
    if "tests" in project_path.parts:
        project_path = project_path.parent
    return project_path


def check_host(host='localhost', port=1025, response_code="220", check_response=True):
    """
    Test if (email) host is healthy.
    :param host:
    :return: True if healthy, else False.
    """

    # Create socket and establish a TCP connection
    client_socket = socket(AF_INET, SOCK_STREAM)
    error_code = client_socket.connect_ex((host, int(port)))
    # function connect_ex. It doesn't throw an exception.
    # Instead of that, returns a C style integer value (referred to as errno in C)
    # See: https://stackoverflow.com/questions/177389/testing-socket-connection-in-python

    # Evaluate answer
    recv = client_socket.recv(1024)
    # Example recv == b'220 mailhog.example ESMTP MailHog\r\n'
    if check_response:
        client_socket.close()
        return True if response_code in str(recv) else False

    else:
        return True if error_code == 0 else False


class MyConnector(object):

    def __init__(self, db_system_name: str = "postgresql"):

        self.logger = None
        self.logger = MyLogger()
        self.logger.setup_handlers(["EMAIL", "FILE", "CONSOLE"])

        self.db_system_name = db_system_name
        self.connection = None
        self.engine = None
        self.stations = None
        self.measures = None
        self.metadata = None

        try:
            connect_info = get_setting([db_system_name, "drivername"])
            assert bool(connect_info), f"Error reading settings."
            self.logger.debug(f"Imported settings for {connect_info} from configfile")

            is_connected = self.connect_database(db_system_name)
            assert bool(is_connected), f"Error connecting to database and creating tables."
            tables = self.test_query()
            self.logger.debug(f"Successfully connected to database. Database contains tables: {tables}")
        except AssertionError as ae:
            self.logger.exception(f"Error connecting to database since assertion {ae}")
        except Exception as ex:
            self.logger.exception(f"Error connecting to database. Error: {ex}")

    def connect_database(self, db_system_name: str = "postgresql"):
        """Connect to database."""

        assert (db_system_name in ["postgresql", "mysql"]), "Name of dbms is unknown"

        DATABASE_URI = get_setting([db_system_name, "drivername"]) + "://"\
                         + get_setting([db_system_name, "user"]) + ":"\
                         + get_setting([db_system_name, "passwd"]) + "@"\
                         + get_setting([db_system_name, "host"]) + ":"\
                         + get_setting([db_system_name, "port"]) + "/"\
                         + get_setting([db_system_name, "database_name"])

        self.logger.debug(f"Connecting to: {get_setting([db_system_name, 'host'])}")

        try:
            self.engine = create_engine(DATABASE_URI, echo=False)
            self.connection = self.engine.connect()
            self.metadata = MetaData(self.engine)
            self.create_all_tables()
        except Exception as ex:
            self.logger.warning(f"Error connecting to Database. {ex}")

        return self.required_tables_exist()

    def create_all_tables(self):
        assert bool(self.metadata), f"Metadata for schemas does not exist."

        self.create_table_stations()
        self.create_table_measures("measures")
        self.create_table_measures("temporal_measures", CHECKFIRST=True)

    def required_tables_exist(self):
        return {"stations", "measures"}.issubset(set(self.metadata.tables))

    def test_query(self):
        #self.create_table_measures('temporal_measures')
        #self.create_table_measures('measures')
        #self.create_table_stations('stations')
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
            self.logger.debug(f"Created table 'stations'")
        except Exception:
            self.logger.exception("Error creating Table in Database")

    def create_table_measures(self, table_name="temporal_measures", CHECKFIRST=True):
        """Create schema for temporary table of weather data measures that extend existing data measures."""

        if table_name is None:
            table_name = input("Name of new table for weather data measures, e.g. 'temp'? ")

        table_measures = Table(
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
            extend_existing=False
        )
        try:
            self.logger.debug(f"Checkfirst is '{CHECKFIRST}' for creating tables {table_name} ")
            table_measures.create(self.connection, checkfirst=CHECKFIRST)  # If NOT exists, create the table.
            if table_name == "measures":
                self.measures = table_measures
            self.logger.debug(f"Created table '{table_name}'")
        except Exception:
            self.logger.exception(f"Error creating Table {table_name} in Database")

    def clear_temporal_table(self):
        """Recreate temporal weather resources table with identical schema in database."""

        try:
            table_name = "temporal_measures"
            session = sessionmaker()
            session.configure(bind=self.connection)
            temporary_session = session()

            # clear the temporary weather data table in database pertaining the schema
            temporal_measures = self.metadata.tables[table_name]
            clear_statement = temporal_measures.delete()
            res = self.connection.execute(clear_statement)

            temporary_session.commit()

            self.logger.debug(f"Cleared table '{table_name}'")
        except Exception:
            self.logger.exception("Error clearing Database")


class MyLogger(logging.Logger):

    def __init__(self):
        super().__init__(__name__)
        #self = logging.getLogger()
        self.setLevel(logging.DEBUG)

    def setup_handlers(self, list_of_logger: list = []):

        if "CONSOLE" in list_of_logger:
            # create CONSOLE handler with a log level "DEBUG"
            consolhandler = logging.StreamHandler()
            consolhandler.setLevel(logging.DEBUG)
            consol_formatter = logging.Formatter(
                '%(levelname)s function %(funcName)s: %(message)s'
                )
            consolhandler.setFormatter(consol_formatter)

            self.addHandler(consolhandler)
            print("Set console handler")

        if "FILE" in list_of_logger:
            # create FILE handler which logs on the levels ""warnings"" and above
            log_file = get_setting(["error", "log"])
            log_file_path = pathlib.Path(get_project_path()).joinpath(log_file)
            assert log_file_path.exists(), "Path to Logfile does not exist."

            filehandler = logging.FileHandler(log_file_path)
            filehandler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - function:%(funcName)s - %(message)s')
            filehandler.setFormatter(file_formatter)

            self.addHandler(filehandler)
            print("Set file handler")

        if "EMAIL" in list_of_logger:
            #get_setting(["error","send_email"]).upper() == "TRUE":

            mailhost = {"host": 'smtp', "port": 1025,
                        "response_code": "220", "check_response": True}
            #mailhost = {"host": 'localhost', "port": 1025}
            assert check_host(**mailhost), "Email host is not available"

            connection_paras = (mailhost["host"], mailhost["port"])
            email_handler = logging.handlers.SMTPHandler(connection_paras, 'WeatherData@Docker.Service',
                                              ['status@corporate.admin'],
                                              'WeatherDataService', credentials=None, secure=None)
            email_handler.setLevel(logging.INFO)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - function:%(funcName)s - %(message)s')
            email_handler.setFormatter(formatter)

            self.addHandler(email_handler)
            print("Set email handler")
            self.info("Initialized email logging.")


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
