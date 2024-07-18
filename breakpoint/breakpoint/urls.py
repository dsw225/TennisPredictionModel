"""
URL configuration for breakpoint project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import re_path, include
from render.views import *

urlpatterns = [
    re_path('admin/', admin.site.urls),
    re_path('mens-tennis-matches/', MensTennisMatchView.as_view(), name='mens-tennis-match-list'),
    re_path('womens-tennis-matches/', WomensTennisMatchView.as_view(), name='womens-tennis-match-list'),
    re_path('mens-players/', MensPlayerView.as_view(), name='mens-player-view-list'),
    re_path('womens-players/', WomensPlayerView.as_view(), name='womens-player-view-list'),
    re_path('mens-full-elos/', MensFullEloStatsView.as_view(), name='mens-full-elos-list'),
    re_path('mens-hard-elos/', MensHardEloStatsView.as_view(), name='mens-hard-elos-list'),
    re_path('mens-clay-elos/', MensClayEloStatsView.as_view(), name='mens-clay-elos-list'),
    re_path('mens-grass-elos/', MensGrassEloStatsView.as_view(), name=' mens-grass-elos-list'),
    re_path('womens-full-elos/', WomensFullEloStatsView.as_view(), name='womens-full-elos-list'),
    re_path('womens-hard-elos/', WomensHardEloStatsView.as_view(), name='womens-hard-elos-list'),
    re_path('womens-clay-elos/', WomensClayEloStatsView.as_view(), name='womens-clay-elos-list'),
    re_path('womens-grass-elos/', WomensGrassEloStatsView.as_view(), name='womens-grass-elos-list'),
    re_path('mens-tennis-match-stats/', MensTennisMatchStatsView.as_view(), name='mens-tennis-match-stats-list'),
    re_path('womens-tennis-match-stats/', WomensTennisMatchStatsView.as_view(), name='womens-tennis-match-stats-list'),
]
