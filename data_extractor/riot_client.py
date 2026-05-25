import requests
from config import HEADERS, GAME_NAME, TAG_LINE

ACCOUNT_BASE_URL = "https://americas.api.riotgames.com"
MATCH_BASE_URL = "https://americas.api.riotgames.com"


class RiotClient:
    def __init__(self):
        self.headers = HEADERS

    def get_puuid(self, game_name: str, tag_line: str) -> str:
        url = (
            f"{ACCOUNT_BASE_URL}"
            f"/riot/account/v1/accounts/by-riot-id/"
            f"{game_name}/{tag_line}"
        )

        response = requests.get(url, headers=self.headers)
        response.raise_for_status()

        data = response.json()

        return data["puuid"]

    def get_recent_match_ids(
        self,
        puuid: str,
        count: int = 10
    ) -> list[str]:

        url = (
            f"{MATCH_BASE_URL}"
            f"/lol/match/v5/matches/by-puuid/"
            f"{puuid}/ids"
        )

        params = {
            "start": 0,
            "count": count
        }

        response = requests.get(
            url,
            headers=self.headers,
            params=params
        )

        response.raise_for_status()

        return response.json()

    def get_match_details(self, match_id: str) -> dict:
        url = (
            f"{MATCH_BASE_URL}"
            f"/lol/match/v5/matches/{match_id}"
        )

        response = requests.get(url, headers=self.headers)
        response.raise_for_status()

        return response.json()
