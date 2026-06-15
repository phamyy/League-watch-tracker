import json
from pathlib import Path


STATE_FILE = Path("database/seen_matches.json")


class MatchState:
    def __init__(self):
        self.seen_matches = self._load_seen_matches()

    def _load_seen_matches(self):
        if not STATE_FILE.exists():
            return set()

        try:
            with open(STATE_FILE, "r") as file:
                data = json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            self.seen_matches = []

        return set(data)

    def has_seen(self, match_id: str) -> bool:
        return match_id in self.seen_matches

    def add_match(self, match_id):
        self.seen_matches.add(match_id)
        self._save()

    def _save(self):
        STATE_FILE.parent.mkdir(exist_ok=True)

        with open(STATE_FILE, "w") as file:
            json.dump(list(self.seen_matches), file, indent=2)