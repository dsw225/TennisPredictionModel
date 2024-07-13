from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .models import TennisMatch, Player
from . serializer import TennisMatchSerializer, PlayerViewSerializer

class TennisMatchView(APIView):
    serializer_class = TennisMatchSerializer

    def get(self, request):
        matches = TennisMatch.objects.all()
        output = [
            {
                "tourney_id": match.tourney_id,
                "tourney_name": match.tourney_name,
                "surface": match.surface,
                "draw_size": match.draw_size,
                "tourney_level": match.tourney_level,
                "tourney_date": match.tourney_date,
                "match_num": match.match_num,
                "best_of": match.best_of,
                "round": match.round,
                "minutes": match.minutes,
                "winner_id": match.winner_id,
                "winner_seed": match.winner_seed,
                "winner_entry": match.winner_entry,
                "winner_name": match.winner_name,
                "winner_hand": match.winner_hand,
                "winner_ht": match.winner_ht,
                "winner_ioc": match.winner_ioc,
                "winner_age": match.winner_age,
                "loser_id": match.loser_id,
                "loser_seed": match.loser_seed,
                "loser_entry": match.loser_entry,
                "loser_name": match.loser_name,
                "loser_hand": match.loser_hand,
                "loser_ht": match.loser_ht,
                "loser_ioc": match.loser_ioc,
                "loser_age": match.loser_age,
                "winner_odds": match.winner_odds,
                "loser_odds": match.loser_odds,
                "w1": match.w1,
                "w2": match.w2,
                "w3": match.w3,
                "w4": match.w4,
                "w5": match.w5,
                "w_ace": match.w_ace,
                "w_df": match.w_df,
                "w_svpt": match.w_svpt,
                "w_1stIn": match.w_1stIn,
                "w_1stWon": match.w_1stWon,
                "w_2ndWon": match.w_2ndWon,
                "w_SvGms": match.w_SvGms,
                "w_bpSaved": match.w_bpSaved,
                "w_bpFaced": match.w_bpFaced,
                "l1": match.l1,
                "l2": match.l2,
                "l3": match.l3,
                "l4": match.l4,
                "l5": match.l5,
                "l_ace": match.l_ace,
                "l_df": match.l_df,
                "l_svpt": match.l_svpt,
                "l_1stIn": match.l_1stIn,
                "l_1stWon": match.l_1stWon,
                "l_2ndWon": match.l_2ndWon,
                "l_SvGms": match.l_SvGms,
                "l_bpSaved": match.l_bpSaved,
                "l_bpFaced": match.l_bpFaced,
                "winner_rank": match.winner_rank,
                "loser_rank": match.loser_rank,
            }
            for match in matches
        ]
        return Response(output)

    def post(self, request):
        serializer = TennisMatchSerializer(data=request.data)
        if serializer.is_valid(raise_exception=True):
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class PlayerView(APIView):
    serializer_class = PlayerViewSerializer

    def get(self, request):
        players = Player.objects.all()
        output = [
            {
                "player_id": player.player_id,
                "player_name": player.player_name,
                "player_slug": player.player_slug,
                "player_hand": player.player_hand,
                "player_dob": player.player_dob,
                "player_ioc": player.player_ioc,
                "player_ht": player.player_ht,
                "player_sofa_id": player.player_sofa_id,
                "player_active": player.player_active,
            }
            for player in players
        ]
        return Response(output)

    def post(self, request):
        serializer = PlayerViewSerializer(data=request.data)
        if serializer.is_valid(raise_exception=True):
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
