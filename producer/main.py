# from .config import GAME_NAME, TAG_LINE
# from .riot_client import RiotClient
# from datetime import datetime
# from producer import MatchProducer

# # def main():
# #     client = RiotClient()
# #     puuid = client.get_puuid(game_name=GAME_NAME, tag_line=TAG_LINE)
# #     print(f"PUUID: {puuid}")
    

# #     print("Getting match histories......")

# #     match_ids = client.get_recent_match_ids(puuid=puuid)
# #     for match in match_ids:
# #         match_data = client.get_match_details(match_id=match)

# #         print(f"- {match}")
# #         print(f"Game Mode: {match_data['info']['gameMode']}")
# #         print(f"Game Duration: {match_data['info']['gameDuration'] // 60} mins")
# #         print(f"Game Date: {datetime.fromtimestamp((match_data['info']['gameCreation']) / 1000.0).date()}")
# #         players = match_data["info"]["participants"]

# #         for player in players:
# #             if player["puuid"] == puuid:
# #                 print(f"Champion: {player['championName']}")
# #                 print(
# #                     f"KDA: "
# #                     f"{player['kills']}/"
# #                     f"{player['deaths']}/"
# #                     f"{player['assists']}"
# #                 )
# #                 print(f"Position: {player['individualPosition']}")
# #                 print(f"Win: {player['win']}")


# def main():
#     producer = MatchProducer()
#     producer.start()


# if __name__ == "__main__":
#     main()