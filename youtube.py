import requests


class YoutubeSearch:
    _GCLOUD_KEY = "AIzaSyAAE45DbhBDsHtcZiU-ybVurjc_amWhnb4"
    _URL_SEARCH_YOUTUBE = "https://www.googleapis.com/youtube/v3/search?"

    def search_youtube(self, word, limit=100):
        "Obtener data youtube"
        try:
            url = f"{self._URL_SEARCH_YOUTUBE}q={word}&key={self._GCLOUD_KEY}&type=video&maxResults={limit}&part=id,snippet"
            data = requests.get(url)
            result = []
            if data.status_code == 200:
                info = data.json()
                if "items" in info and len(info["items"]) > 0:
                    i = 0
                    for item in info["items"]:
                        video_id = item["id"]["videoId"]
                        result.append(
                            {
                                "link": f"https://www.youtube.com/watch?v={video_id}",
                                "title": item["snippet"]["title"],
                                "metadata": {"positions": i, "engine": "youtube"},
                            }
                        )
                        i += 1
                    return result
            return []
        except Exception as e:
            print(e)
            raise ValueError(str(e))