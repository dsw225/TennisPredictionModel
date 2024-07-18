from django.urls import path
from .views import *

urlpatterns = [
    path('mens-tennis-matches/', MensTennisMatchView.as_view(), name='mens-tennis-match-list'),
    path('womens-tennis-matches/', WomensTennisMatchView.as_view(), name='womens-tennis-match-list'),
    path('mens-players/', MensPlayerView.as_view(), name='mens-player-view-list'),
    path('womens-players/', WomensPlayerView.as_view(), name='womens-player-view-list'),
    path('mens-full-elos/', MensFullEloStatsView.as_view(), name='mens-full-elos-list'),
    path('mens-hard-elos/', MensHardEloStatsView.as_view(), name='mens-hard-elos-list'),
    path('mens-clay-elos/', MensClayEloStatsView.as_view(), name='mens-clay-elos-list'),
    path('mens-grass-elos/', MensGrassEloStatsView.as_view(), name=' mens-grass-elos-list'),
    path('womens-full-elos/', WomensFullEloStatsView.as_view(), name='womens-full-elos-list'),
    path('womens-hard-elos/', WomensHardEloStatsView.as_view(), name='womens-hard-elos-list'),
    path('womens-clay-elos/', WomensClayEloStatsView.as_view(), name='womens-clay-elos-list'),
    path('womens-grass-elos/', WomensGrassEloStatsView.as_view(), name='womens-grass-elos-list'),
    path('mens-tennis-match-stats/', MensTennisMatchStatsView.as_view(), name='mens-tennis-match-stats-list'),
    path('womens-tennis-match-stats/', WomensTennisMatchStatsView.as_view(), name='womens-tennis-match-stats-list'),
]
