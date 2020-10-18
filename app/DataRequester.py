from bs4 import BeautifulSoup
import requests
import app.Helper
from urllib.parse import urljoin
from pathlib import Path


class DataRequester(object):
    """Request and save remote weather data locally in zip-file."""

    def __init__(self, url: str = "https://dbup2date.uni-bayreuth.de/downloads/wetterdaten/wetterdaten_Wettermessung.csv"):
        """Initialize Data Requester with default settings.
            :param url Path to remote data source.
        """

        self.logger = app.Helper.MyLogger()
        self.logger.setup_handlers()

        self.last_zip_file = ""
        self.saved_zipfile_path = ""
        self.url = url
        # OLD self.url = "https://dbup2date.uni-bayreuth.de/blocklysql/"  # static link

        return

    def get_distant_filename(self):
        """Scrap HTML page to extract newest link."""

        html_path = self.url
        zip_file_url = None

        # extract current link in html file
        try:
            resp = requests.get(html_path, verify=False)
            soup = BeautifulSoup(resp.content, features="html.parser")
            self.logger.debug(f"Found website and parsed html-file")

            if resp.status_code == 404:
                raise requests.HTTPError

        except requests.HTTPError as e:
            self.logger.error(f"Connection to {html_path} failed. URL does not exist ({e}).")
            raise

        # get distant zip file path
        try:
            for link in soup.find_all('a', href=True):
                if "CSV.zip" in link['href']:
                    zip_file_url = link['href']  # example: "downloads\wetterdaten\2019-07-07_wetterdaten_CSV.zip"
                    break

            assert "_wetterdaten_CSV.zip" in zip_file_url, "Can't find a zipfile '*wetterdaten_CSV.zip' in HTML"
            self.last_zip_file = zip_file_url
            self.logger.debug(f"Found a zipfile in HTML-code")

        except Exception as ex:
            self.logger.exception(f"Error while extracting zipfile in HTML-code ({ex})")
            raise

        return

    def save_zip_locally(self):
        """Load last weather data from extracted link."""
        # example: url = 'https://dbup2date.uni-bayreuth.de/blocklysql/downloads/wetterdaten/2019-06-21_wetterdaten.zip'

        # REQUEST file
        try:
            # load last remote zip file
            full_url_zip = urljoin(self.url, self.last_zip_file)
            resp = requests.get(full_url_zip, allow_redirects=True, verify=False)
            assert resp.status_code == 200, "Can't retrieve file"
            assert resp.content, "File has no content."
        except Exception as ex:
            self.logger.exception(f"Error loading {self.last_zip_file} ({ex})")
            raise

        # SAVE loaded file in filepath
        try:
            # CREATE the "filepath" at "/data/*.zip"
            data_dir = app.Helper.get_setting(["general", "data_dir"])
            self.saved_zipfile_path = Path.cwd().joinpath(data_dir).joinpath(self.last_zip_file)

            # SAVE
            open(self.saved_zipfile_path, 'wb').write(resp.content)
            self.logger.info(f"Saved {self.saved_zipfile_path} from {full_url_zip}")
        except Exception:
            self.logger.exception(f"Error writing {self.saved_zipfile_path}")
            raise

        return

    def run(self):
        """Process steps in order to collect new data."""
        try:
            self.logger.info(f"DataRequester running...")
            self.get_distant_filename()
            self.save_zip_locally()
            self.logger.info(f"DataRequester closed!")
        except Exception as ex:
            self.logger.exception(f"Error while running DataRequester ({ex})")

        return