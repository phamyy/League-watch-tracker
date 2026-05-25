from dataclasses import dataclass
from datetime import date


@dataclass
class MatchEvent:
    match_id: str
    gameMode: str
    gameDuration: int
    gameDate: date
    champion: str
    kills: int
    deaths: int
    assists: int
    position: str
    win: bool

    def to_dict(self):
        return {
            "match_id": self.match_id,
            "gameMode": self.gameMode,
            "gameDuration": self.gameDuration,
            "gameDate": self.gameDate,
            "champion": self.champion,
            "kills": self.kills,
            "deaths": self.deaths,
            "assists": self.assists,
            "position": self.position,
            "win": self.win,
        }