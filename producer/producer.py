import time
from datetime import datetime

from config import GAME_NAME, TAG_LINE
from models import MatchEvent
from riot_client import RiotClient
from state import MatchState

import psycopg2
import json

from kafka import KafkaProducer


POLL_INTERVAL = 60


class MatchProducer:
    def __init__(self):
        self.client = RiotClient()
        self.state = MatchState()

        self.connection = psycopg2.connect(
            host="localhost",
            port=5433,
            database="riot_analytics",
            user="riot",
            password="riotpassword"
        )
        self.cursor = self.connection.cursor()

        try:
            self.producer = KafkaProducer(
                bootstrap_servers='localhost:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        except Exception as e:
            print ('Waiting for Kafka')
            time.sleep(10)

    def start(self):
        print("Starting Riot match producer...")

        puuid = self.client.get_puuid(
            GAME_NAME,
            TAG_LINE
        )

        while True:
            try:
                self.poll_matches(puuid)

            except Exception as error:
                print(f"Producer error: {error}")

            time.sleep(POLL_INTERVAL)

    def poll_matches(self, puuid: str):
        match_ids = self.client.get_recent_match_ids(
            puuid,
            count=10
        )

        for match_id in reversed(match_ids):
            
            #if the match has been seen / ingested already, pass
            # self.cursor.execute("""
            #     select match_id from raw_matches
            #     """ )
            # match_ids = self.cursor.fetchall()
            # if match_id in [match[0] for match in match_ids]:
            #     pass

            event = self.build_match_event(match_id)

            self.send_producer_data_to_kafka(match_id = match_id, payload= event.to_dict())

            # self.state.add_match(match_id)

    def build_match_event(
        self,
        match_id: str
    ) -> MatchEvent:

        puuid = self.client.get_puuid(game_name=GAME_NAME, tag_line=TAG_LINE)
        match_data = self.client.get_match_details(match_id)
        """
        Need to add in Summoner Rank + win / loss + LP -> need to add this to the model
        """

        tier = self.client.get_summoner_rank(puuid=puuid)[0]['tier']
        rank = self.client.get_summoner_rank(puuid=puuid)[0]['rank']
        lp = self.client.get_summoner_rank(puuid=puuid)[0]['leaguePoints']
        wins = self.client.get_summoner_rank(puuid=puuid)[0]['wins']
        losses = self.client.get_summoner_rank(puuid=puuid)[0]['losses']

        info = match_data["info"]

        participants = info["participants"]

        target_player = next(
            player
            for player in participants
            if player["puuid"] == puuid
        )

        return MatchEvent(
            gameMode=info['gameMode'],
            gameDuration=info['gameDuration'] // 60,
            gameDate=datetime.fromtimestamp((info['gameCreation']) / 1000.0).strftime('%Y-%m-%d'),
            champion=target_player["championName"],
            kills=target_player["kills"],
            deaths=target_player["deaths"],
            assists=target_player["assists"],
            position=target_player['individualPosition'],
            win=target_player["win"],
            tier=tier,
            rank=rank,
            lp=lp,
            numWins=wins,
            numLosses=losses
        )

    def emit_event(self, event: MatchEvent):
        print("\nNEW MATCH EVENT")
        print("=" * 50)

        print(event.to_dict())

    def send_producer_data_to_kafka(self, match_id,payload):
        self.producer.send(
            "match-events",
            {
                "match_id": match_id,
                "payload": payload
            }
        )
        print(f"{match_id} sent to kafka")