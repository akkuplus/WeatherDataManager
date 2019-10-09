class DataRequester(object):
    """Request and save remote weather data into a local zip file."""

    def __init__(self):
        import pathlib
        import configparser

        self.config = configparser.ConfigParser()
        self.config.read(pathlib.Path.cwd().joinpath("config").joinpath("config.ini"))

        self.get_logger()

        self.last_zip_file = ""
        self.saved_zipfile_path = ""
        self.url = "https://dbup2date.uni-bayreuth.de/blocklysql/"  # static link

    def run(self):
        try:
            self.logger.info(f"DataRequester running...")
            self.get_distant_filename()
            self.save_zip_locally()
            self.logger.info(f"DataRequester closed!")
        except Exception as ex:
            self.logger.exception(f"Error while running DataRequester")

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

            #emailhandler = logging.handlers.SMTPHandler(
            #    "mailhost",
            #    "fromaddr",
            #    self.config.get("error", "recipients"),
            #    "subject",
            #    credentials=None)
            #emailhandler.setLevel(logging.ERROR)
            #emailhandler.setFormatter(formatter)
            #self.logger.addHandler(emailhandler)

    def get_distant_filename(self):
        """Scrap HTML page to extract newest link."""
        from bs4 import BeautifulSoup
        import requests
        from urllib.parse import urljoin

        # extract current link in html file
        html_file = "wetterdaten.html"  # static filename
        html_path = urljoin(self.url, html_file)
        zip_file_url = None

        try:
            resp = requests.get(html_path, verify=False)
            soup = BeautifulSoup(resp.content, features="html.parser")
            self.logger.debug(f"Found website and parsed html-file")
        except Exception:
            self.logger.exception(f"Connection to {html_path} failed")

        try:
            # get distant zip file path
            for link in soup.find_all('a', href=True):
                if "CSV" in link['href']:
                    zip_file_url = link['href']  # example: "downloads\wetterdaten\2019-07-07_wetterdaten_CSV.zip"
                    break
            if zip_file_url:
                self.last_zip_file = zip_file_url
                self.logger.debug(f"Found a zipfile in HTML-code")
            else:
                raise FileNotFoundError("Can't find a zipfile '*wetterdaten_CSV.zip' in HTML")
        except Exception:
            self.logger.exception(f"Error while extracting zipfile in HTML-code")

    def save_zip_locally(self):
        """Load last weather data that is saved in zip."""
        # example: url = 'https://dbup2date.uni-bayreuth.de/blocklysql/downloads/wetterdaten/2019-06-21_wetterdaten.zip'
        import requests
        import urllib.parse
        from pathlib import Path

        # load last remote zip file from web
        r = None
        full_url_zip = None
        try:
            # load last remote zip file
            full_url_zip = urllib.parse.urljoin(self.url, self.last_zip_file)
            r = requests.get(full_url_zip, allow_redirects=True, verify=False)

            # get the filename of this zipfile
            zip_file_name = self.last_zip_file.rsplit(sep="/", maxsplit=1)[-1]  # get the last element of url

            # and create the "filepath" at "/data/*.zip"
            self.saved_zipfile_path = Path.cwd().joinpath("data").joinpath(zip_file_name)
            self.logger.debug(f"Created filepath for new zipfile")
        except Exception:
            self.logger.exception(f"Error loading {self.saved_zipfile_path}")

        # save the loaded file under "filepath"
        try:
            open(self.saved_zipfile_path, 'wb').write(r.content)
            self.logger.info(f"Saved {self.saved_zipfile_path} from {full_url_zip}")
            dir(self.saved_zipfile_path)
        except Exception:
            self.logger.exception(f"Error writing {self.saved_zipfile_path}")
