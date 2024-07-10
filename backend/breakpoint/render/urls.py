from django.urls import path
from .views import TennisMatchView, PlayerViewView

urlpatterns = [
    path('tennis-matches/', TennisMatchView.as_view(), name='tennis_match_list'),
    path('players/', PlayerViewView.as_view(), name='player_view_list'),
]
