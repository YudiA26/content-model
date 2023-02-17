import math

import requests


class GoogleSearch:
    _GCLOUD_KEY = "AIzaSyDBuRusElclyc3Kf4H4yz2Q4saknW8mJwQ"
    _CX_GOOGLE_KEY = "445f8dec387320bda"
    _URL_SEARCH_GOOGLE = "https://customsearch.googleapis.com/customsearch/v1?"

    def search_google(self, word, limit=100):
        "Obtener data google"
        try:
            result = []
            page = math.ceil(limit / 10) + 1
            i = 0
            for i in range(1, page):
                url = f"{self._URL_SEARCH_GOOGLE}q={word}&cx={self._CX_GOOGLE_KEY}&key={self._GCLOUD_KEY}&alt=json&start={i}"
                data = requests.get(url)
                if data.status_code == 200:
                    info = data.json()
                    if "items" in info and len(info["items"]) > 0:
                        for item in info["items"]:
                            result.append(
                                {
                                    "link": item["link"],
                                    "title": item["title"],
                                    "metadata": {"positions": i, "engine": "google"},
                                }
                            )
                            i += 1
            return result
        except Exception as e:
            print(e)
            raise ValueError(str(e))