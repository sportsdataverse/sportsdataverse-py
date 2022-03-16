
import numpy as np
import time
import http.client
import urllib.request
from urllib.error import URLError, HTTPError, ContentTooShortError
from datetime import datetime
from itertools import chain, starmap

def download(url, num_retries=10):
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
        if num_retries == 0:
            print("Retry Limit Exceeded")
    return html

def flatten_json_iterative(dictionary, sep = '.', ind_start = 0):
    """Flattening a nested json file"""

    def unpack_one(parent_key, parent_value):
        """Unpack one level (only one) of nesting in json file"""
        # Unpacking one level
        if isinstance(parent_value, dict):
            for key, value in parent_value.items():
                t1 = parent_key + sep + key
                yield t1, value
        elif isinstance(parent_value, list):
            i = ind_start
            for value in parent_value:
                t2 = parent_key + sep +str(i)
                i += 1
                yield t2, value
        else:
            yield parent_key, parent_value
    # Continue iterating the unpack_one function until the terminating condition is satisfied
    while True:
        # Continue unpacking the json file until all values are atomic elements (aka neither a dictionary nor a list)
        dictionary = dict(chain.from_iterable(starmap(unpack_one, dictionary.items())))
        # Terminating condition: none of the values in the json file are a dictionary or a list
        if not any(isinstance(value, dict) for value in dictionary.values()) and \
        not any(isinstance(value, list) for value in dictionary.values()):
            break
    return dictionary

def key_check(obj, key, replacement = np.array([])):
    if key in obj.keys():
        obj_key = obj[key]
    else:
        obj_key = replacement
    return obj_key