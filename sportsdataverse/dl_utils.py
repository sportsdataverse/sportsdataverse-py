import time
import http.client
import urllib.request
from urllib.error import URLError, HTTPError, ContentTooShortError
from datetime import datetime
from itertools import chain, starmap

def download(url, num_retries=5):
    try:
        html = urllib.request.urlopen(url).read()
    except (URLError, HTTPError, ContentTooShortError, http.client.HTTPException, http.client.IncompleteRead) as e:
        print('Download error:', url)
        html = None
        if num_retries > 0:
            if hasattr(e, 'code') and 500 <= e.code < 600:
                time.sleep(10)
                # recursively retry 5xx HTTP errors
                return download(url, num_retries - 1)
        if num_retries > 0:
            if e == http.client.IncompleteRead:
                time.sleep(10)
                return download(url, num_retries - 1)
    return html