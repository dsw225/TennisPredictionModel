from django.urls import path
from .views import TennisMatchView, PlayerView

urlpatterns = [
    path('tennis-matches/', TennisMatchView.as_view(), name='tennis_match_list'),
    path('players/', PlayerView.as_view(), name='player_view_list'),
]
