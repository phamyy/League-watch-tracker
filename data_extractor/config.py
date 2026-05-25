from dotenv import load_dotenv
import os

load_dotenv()

RIOT_API_KEY = os.getenv("RIOT_API_KEY")
GAME_NAME = os.getenv("GAME_NAME")
TAG_LINE = os.getenv("TAG_LINE")

HEADERS = {
    "X-Riot-Token": RIOT_API_KEY
}