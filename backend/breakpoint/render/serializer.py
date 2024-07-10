from rest_framework import serializers
from .models import *

class TennisMatchSerializer(serializers.ModelSerializer):
    class Meta:
        model = TennisMatch
        fields = '__all__'

class PlayerViewSerializer(serializers.ModelSerializer):
    class Meta:
        model = PlayerView
        fields = '__all__'
