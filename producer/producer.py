import time
from datetime import datetime

from .config import GAME_NAME, TAG_LINE
from .models import MatchEvent
from .riot_client import RiotClient

import psycopg2
import json

from kafka import KafkaProducer


POLL_INTERVAL = 60


"""
1. Change the start up so that even if services are not up and running, we wait for them to init and then connect so that it doesn't crash
2. Change the connection string? kafka?


"""


class MatchProducer:
    def __init__(self):
        print('Initializing Match Producer...')
        self.client = RiotClient()
        print('RiotClient initialized')

        self.connection = self.connect_to_postgresql()
        print('Postgresql initialized')

        self.cursor = self.connection.cursor()

        self.producer = self.connect_to_kafka()
        print('Kafka initialized')
    
    def connect_to_kafka(self):
        while True:
            try:
                producer = KafkaProducer(
                    bootstrap_servers='kafka:9092',
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                print('Producer connected to Kafka')
                return producer
            
            except Exception as e:
                print ('Producer waiting for Kafka')
                time.sleep(5)

    def connect_to_postgresql(self):
        while True:
            try:
                connection = psycopg2.connect(
                    host="postgres",
                    port=5432,
                    database="riot_analytics",
                    user="riot",
                    password="riotpassword"
                )
                print('Producer connected to Postgresql')
                return connection
            except Exception as e:
                print(f'Producer Postgresql unavailable: {e}')
                time.sleep(5)


    def start(self):
        print("Starting Riot match producer...")

        puuid = self.client.get_puuid(
            GAME_NAME,
            TAG_LINE
        )

        print(f"PUUID: {puuid}")

        while True:
            try:
                self.poll_matches(puuid)

            except Exception as error:
                print(f"The error is occuring here: {error}")

            time.sleep(POLL_INTERVAL)

    def poll_matches(self, puuid: str):
        match_ids = self.client.get_recent_match_ids(
            puuid,
            count=10
        )
        self.cursor.execute("""
                select match_id from raw_matches
                """ )

        existing_matches = [
            row[0]
            for row in self.cursor.fetchall()
        ]

        print(f"Existing Matches: {existing_matches}")
        print(f"Type of existing matches: {type(existing_matches)}")
        print(f"Matches Pulled from API: {match_ids}")
        print(f"Type of matches pulled from API: {type(match_ids)}")

        for match_id in match_ids:
            #if the match has been seen / ingested already, pass
            print(f"Now Evaluating: {match_id}")
            if match_id in existing_matches:
                print(f"{match_id} already contained in database")
                continue
            
            print(f"now building match event for {match_id}")
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
        rank_data = self.client.get_summoner_rank(puuid=puuid)

        tier = rank_data[0]['tier']
        rank = rank_data[0]['rank']
        lp = rank_data[0]['leaguePoints']
        wins = rank_data[0]['wins']
        losses = rank_data[0]['losses']

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
        self.producer.flush()

    
def run_producer():
    print('Run Producer called')
    producer = MatchProducer()
    print('Starting producer')
    producer.start()


if __name__ == "__main__":
    run_producer()