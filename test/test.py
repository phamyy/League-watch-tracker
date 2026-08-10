# from datetime import datetime
from ..producer.riot_client import RiotClient
from ..producer.config import HEADERS, GAME_NAME, TAG_LINE
# import psycopg2
# import json
# from kafka import KafkaProducer
# from consumer.consumer import AnalyticsConsumer

# # date = datetime.fromtimestamp(1778134925197 / 1000.0).date()
# # print(date)

client = RiotClient()

puuid = client.get_puuid(GAME_NAME, TAG_LINE)
print(f"PUUID: {puuid}")

print(f"SUMMONER_ID: {client.get_summoner_rank(puuid)}")
print(f"Tier: {client.get_summoner_rank(puuid)[0]['tier']}")
print(f"Rank: {client.get_summoner_rank(puuid)[0]['rank']}")
print(f"LP: {client.get_summoner_rank(puuid)[0]['leaguePoints']}")
print(f"Wins: {client.get_summoner_rank(puuid)[0]['wins']}")
print(f"Losses: {client.get_summoner_rank(puuid)[0]['losses']}")

# # connection = psycopg2.connect(
# #             host="localhost",
# #             port=5433,
# #             database="riot_analytics",
# #             user="riot",
# #             password="riotpassword"
# #         )
# # cursor = connection.cursor()

# # cursor.execute("""
# #         select match_id from raw_matches
# #         """ )

# # match_ids = cursor.fetchall()

# # test_match = 'NA1_5551144168'

# # if test_match in [match[0] for match in match_ids]:
# #     print("test match_id is in list")

# consumer = AnalyticsConsumer()
# consumer.print_messages_in_queue()

    