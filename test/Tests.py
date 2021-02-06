import pytest
import requests

import app.DataManager, app.DataRequester, app.Helper

def test_get_distant_filename01():

    dr = app.DataRequester.DataRequester(url = "http://does.not/work" )
    with pytest.raises(requests.HTTPError):
        dr.get_distant_filename() # wrong address raises error

    return

