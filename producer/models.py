from dataclasses import dataclass
from datetime import date


@dataclass
class MatchEvent:
    gameMode: str
    gameDuration: int
    gameDate: str
    champion: str
    kills: int
    deaths: int
    assists: int
    position: str
    win: bool
    tier: str
    rank: str
    lp: int
    numWins: int
    numLosses: int

    def to_dict(self):
        return {
            "gameMode": self.gameMode,
            "gameDuration": self.gameDuration,
            "gameDate": self.gameDate,
            "champion": self.champion,
            "kills": self.kills,
            "deaths": self.deaths,
            "assists": self.assists,
            "position": self.position,
            "win": self.win,
            "tier": self.tier,
            "rank": self.rank,
            "lp": self.lp,
            "numWins": self.numWins,
            "numLosses": self.numLosses
        }