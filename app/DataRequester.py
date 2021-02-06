from bs4 import BeautifulSoup
import requests
from requests.models import PreparedRequest
import requests.exceptions
from urllib.parse import urljoin
from pathlib import Path

import app.Helper


class DataRequester(object):
    """
    Request and save remote weather data locally in zip-file.
    """

    def __init__(self, url: str = "https://dbup2date.uni-bayreuth.de/downloads/wetterdaten/"):
        """
        Initialize DataRequester with default settings.

        :param url Path to remote data source.
        """

        self.logger = app.Helper.MyLogger()
        self.logger.setup_handlers()

        self.last_zip_file = ""
        self.saved_zipfile_path = ""
        self.url = url  # wetterdaten_Wettermessung.csv"
        # OLD self.url = "https://dbup2date.uni-bayreuth.de/blocklysql/"  # static link

        return

    def get_checked_url(self):
        """
        Validate URL. See https://stackoverflow.com/questions/827557/how-do-you-validate-a-url-with-a-regular-expression-in-python
        """

        prepared_request = PreparedRequest()
        try:
            prepared_request.prepare_url(self.url, None)
            return prepared_request.url
        except (requests.exceptions.MissingSchema, Exception):
            raise requests.HTTPError

    def get_distant_filename(self):
        """
        Scrap HTML page to extract newest link.
        """

        try:
            # CHECK RegEx-Url-Pattern
            self.get_checked_url()  # raises httperror
            html_path = self.url

            # EXTRACT current link in html file
            resp = requests.get(html_path, verify=False)
            soup = BeautifulSoup(resp.content, features="html.parser")
            if (resp.status_code == 404) or not bool(soup):
                raise requests.HTTPError

            self.logger.debug(f"Found website and parsed html-file")

        except (requests.HTTPError, Exception) as e:
            self.logger.error(f"Connection to {html_path} failed. URL does not exist ({e}).")
            raise

        # GET distant zip file path
        zip_file_url = None
        try:
            for link in soup.find_all('a', href=True):
                if "wetterdaten_CSV.zip" in link['href']:
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
            open(self.saved_zipfile_path.absolute(), 'wb').write(resp.content)
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
