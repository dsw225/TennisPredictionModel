from rest_framework import serializers
from .models import *

class MensTennisMatchSerializer(serializers.ModelSerializer):
    class Meta:
        model = MensTennisMatch
        fields = '__all__'

class WomensTennisMatchSerializer(serializers.ModelSerializer):
    class Meta:
        model = WomensTennisMatch
        fields = '__all__'

class MensPlayerViewSerializer(serializers.ModelSerializer):
    class Meta:
        model = MensPlayer
        fields = '__all__'

class WomensPlayerViewSerializer(serializers.ModelSerializer):
    class Meta:
        model = WomensPlayer
        fields = '__all__'

class MensFullEloStatsViewSerializer(serializers.ModelSerializer):
    class Meta:
        model = MensFullEloStats
        fields = '__all__'

class MensHardEloStatsViewSerializer(serializers.ModelSerializer):
    class Meta:
        model = MensHardEloStats
        fields = '__all__'

class MensClayEloStatsViewSerializer(serializers.ModelSerializer):
    class Meta:
        model = MensClayEloStats
        fields = '__all__'

class MensGrassEloStatsViewSerializer(serializers.ModelSerializer):
    class Meta:
        model = MensGrassEloStats
        fields = '__all__'

class WomensFullEloStatsViewSerializer(serializers.ModelSerializer):
    class Meta:
        model = WomensFullEloStats
        fields = '__all__'

class WomensHardEloStatsViewSerializer(serializers.ModelSerializer):
    class Meta:
        model = WomensHardEloStats
        fields = '__all__'

class WomensClayEloStatsViewSerializer(serializers.ModelSerializer):
    class Meta:
        model = WomensClayEloStats
        fields = '__all__'

class WomensGrassEloStatsViewSerializer(serializers.ModelSerializer):
    class Meta:
        model = WomensGrassEloStats
        fields = '__all__'