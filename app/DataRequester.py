class DataRequester(object):
    """Requests and saves distant weather data into a local zip file."""

    def __init__(self):
        self.last_zip_file = ""
        self.saved_zipfile_path = ""
        self.url = "https://dbup2date.uni-bayreuth.de/blocklysql/"  # static link

    def run(self):
        try:
            print("\nDataRequester running..."
                  "\n------------------------")
            self.get_distant_filename()
            self.save_zip_locally()
            print("\n------------------------\n"
                  "\nDataRequester closed.")
        except Exception as ex:
            print(ex)

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
        except Exception as ex:
            print("Connection to {} failed.".format(html_path))
            print(ex)

        # get distant zip file path
        for link in soup.find_all('a', href=True):
            if "CSV" in link['href']:
                zip_file_url = link['href']  # example: "downloads\wetterdaten\2019-07-07_wetterdaten_CSV.zip"
                break
        if zip_file_url:
            self.last_zip_file = zip_file_url
        else:
            raise FileNotFoundError("Can't find a zipfile '*wetterdaten_CSV.zip' in HTML")


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

        except Exception as ex:
            print("Error loading {}".format(self.saved_zipfile_path))
            print(ex)

        # save the loaded file under "filepath"
        try:
            open(self.saved_zipfile_path, 'wb').write(r.content)
            print("Saved {} from {}".format(self.saved_zipfile_path, full_url_zip))
            dir(self.saved_zipfile_path)
        except Exception as ex:
            print("Error writing {}".format(self.saved_zipfile_path))
            print(ex)