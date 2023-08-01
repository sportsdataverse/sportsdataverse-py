import os

from sportsdataverse.dl_utils import download


def mlbam_copyright_info(saveFile=False, returnFile=False, **kwargs):
    """
    Displays the copyright info for the MLBAM API.

    Args:
            saveFile (boolean) = False
            If saveFile is set to True, the copyright file generated is saved.

            returnFile (boolean) = False
            If returnFile is set to True, the copyright file is returned.

    """
    url = "http://gdx.mlb.com/components/copyright.txt"
    resp = download(url=url, **kwargs)
    try:
        if resp is not None:
            l_string = str(resp.text(), "UTF-8")
            with open("mlbam_copyright.txt", "w+", encoding="utf-8") as file:
                file.writelines(l_string)

            with open("mlbam_copyright.txt", "r", encoding="utf-8") as file:
                mlbam = file.read()

            if saveFile is False and os.path.exists("mlbam_copyright.txt"):
                os.remove("mlbam_copyright.txt")
            print(mlbam)

            if returnFile is True:
                return mlbam

        else:
            print("Could not connect to the internet. \nPlease fix this issue to be able to use this package.")
    except Exception as e:
        print("Could not connect to the internet. \nPlease fix this issue to be able to use this package.", e)
