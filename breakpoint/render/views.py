from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.pagination import LimitOffsetPagination
from .paginations import MyLimitOffsetPagination
from .models import *
from .serializer import *

class MensTennisMatchView(APIView):
    serializer_class = MensTennisMatchSerializer

    def get(self, request):
        matches = MensTennisMatch.objects.all()
        paginator = MyLimitOffsetPagination()
        paginator.page_size = 5  # Ensure the page size is set to 5
        paginated_matches = paginator.paginate_queryset(matches, request)
        serializer = MensTennisMatchSerializer(paginated_matches, many=True)
        return paginator.get_paginated_response(serializer.data)

    def post(self, request):
        serializer = MensTennisMatchSerializer(data=request.data)
        if serializer.is_valid(raise_exception=True):
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
class WomensTennisMatchView(APIView):
    serializer_class = WomensTennisMatchSerializer

    def get(self, request):
        matches = WomensTennisMatch.objects.all()
        paginator = MyLimitOffsetPagination()
        paginator.page_size = 5  # Ensure the page size is set to 5
        paginated_matches = paginator.paginate_queryset(matches, request)
        serializer = WomensTennisMatchSerializer(paginated_matches, many=True)
        return paginator.get_paginated_response(serializer.data)

    def post(self, request):
        serializer = WomensTennisMatchSerializer(data=request.data)
        if serializer.is_valid(raise_exception=True):
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class MensPlayerView(APIView):
    serializer_class = MensPlayerViewSerializer

    def get(self, request):
        players = MensPlayer.objects.all()
        paginator = MyLimitOffsetPagination()
        paginator.page_size = 5  # Ensure the page size is set to 5
        paginated_players = paginator.paginate_queryset(players, request)
        serializer = MensPlayerViewSerializer(paginated_players, many=True)
        return paginator.get_paginated_response(serializer.data)

    def post(self, request):
        serializer = MensPlayerViewSerializer(data=request.data)
        if serializer.is_valid(raise_exception=True):
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
class WomensPlayerView(APIView):
    serializer_class = WomensPlayerViewSerializer

    def get(self, request):
        players = WomensPlayer.objects.all()
        paginator = MyLimitOffsetPagination()
        paginator.page_size = 5  # Ensure the page size is set to 5
        paginated_players = paginator.paginate_queryset(players, request)
        serializer = WomensPlayerViewSerializer(paginated_players, many=True)
        return paginator.get_paginated_response(serializer.data)

    def post(self, request):
        serializer = WomensPlayerViewSerializer(data=request.data)
        if serializer.is_valid(raise_exception=True):
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class MensFullEloStatsView(APIView):
    serializer_class = MensFullEloStatsViewSerializer

    def get(self, request):
        stats = MensFullEloStats.objects.all()
        paginator = MyLimitOffsetPagination()
        paginator.page_size = 5  # Ensure the page size is set to 5
        paginated_players = paginator.paginate_queryset(stats, request)
        serializer = MensFullEloStatsViewSerializer(paginated_players, many=True)
        return paginator.get_paginated_response(serializer.data)

    def post(self, request):
        serializer = MensFullEloStatsViewSerializer(data=request.data)
        if serializer.is_valid(raise_exception=True):
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
class MensHardEloStatsView(APIView):
    serializer_class = MensHardEloStatsViewSerializer

    def get(self, request):
        stats = MensHardEloStats.objects.all()
        paginator = MyLimitOffsetPagination()
        paginator.page_size = 5  # Ensure the page size is set to 5
        paginated_players = paginator.paginate_queryset(stats, request)
        serializer = MensHardEloStatsViewSerializer(paginated_players, many=True)
        return paginator.get_paginated_response(serializer.data)

    def post(self, request):
        serializer = MensHardEloStatsViewSerializer(data=request.data)
        if serializer.is_valid(raise_exception=True):
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
class MensClayEloStatsView(APIView):
    serializer_class = MensClayEloStatsViewSerializer

    def get(self, request):
        stats = MensClayEloStats.objects.all()
        paginator = MyLimitOffsetPagination()
        paginator.page_size = 5  # Ensure the page size is set to 5
        paginated_players = paginator.paginate_queryset(stats, request)
        serializer = MensClayEloStatsViewSerializer(paginated_players, many=True)
        return paginator.get_paginated_response(serializer.data)

    def post(self, request):
        serializer = MensClayEloStatsViewSerializer(data=request.data)
        if serializer.is_valid(raise_exception=True):
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
class MensGrassEloStatsView(APIView):
    serializer_class = MensGrassEloStatsViewSerializer

    def get(self, request):
        stats = MensGrassEloStats.objects.all()
        paginator = MyLimitOffsetPagination()
        paginator.page_size = 5  # Ensure the page size is set to 5
        paginated_players = paginator.paginate_queryset(stats, request)
        serializer = MensGrassEloStatsViewSerializer(paginated_players, many=True)
        return paginator.get_paginated_response(serializer.data)

    def post(self, request):
        serializer = MensGrassEloStatsViewSerializer(data=request.data)
        if serializer.is_valid(raise_exception=True):
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
class WomensFullEloStatsView(APIView):
    serializer_class = WomensFullEloStatsViewSerializer

    def get(self, request):
        stats = WomensFullEloStats.objects.all()
        paginator = MyLimitOffsetPagination()
        paginator.page_size = 5  # Ensure the page size is set to 5
        paginated_players = paginator.paginate_queryset(stats, request)
        serializer = WomensFullEloStatsViewSerializer(paginated_players, many=True)
        return paginator.get_paginated_response(serializer.data)

    def post(self, request):
        serializer = WomensFullEloStatsViewSerializer(data=request.data)
        if serializer.is_valid(raise_exception=True):
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
class WomensHardEloStatsView(APIView):
    serializer_class = WomensHardEloStatsViewSerializer

    def get(self, request):
        stats = WomensHardEloStats.objects.all()
        paginator = MyLimitOffsetPagination()
        paginator.page_size = 5  # Ensure the page size is set to 5
        paginated_players = paginator.paginate_queryset(stats, request)
        serializer = WomensHardEloStatsViewSerializer(paginated_players, many=True)
        return paginator.get_paginated_response(serializer.data)

    def post(self, request):
        serializer = WomensHardEloStatsViewSerializer(data=request.data)
        if serializer.is_valid(raise_exception=True):
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
class WomensClayEloStatsView(APIView):
    serializer_class = WomensClayEloStatsViewSerializer

    def get(self, request):
        stats = WomensClayEloStats.objects.all()
        paginator = MyLimitOffsetPagination()
        paginator.page_size = 5  # Ensure the page size is set to 5
        paginated_players = paginator.paginate_queryset(stats, request)
        serializer = WomensClayEloStatsViewSerializer(paginated_players, many=True)
        return paginator.get_paginated_response(serializer.data)

    def post(self, request):
        serializer = WomensClayEloStatsViewSerializer(data=request.data)
        if serializer.is_valid(raise_exception=True):
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
class WomensGrassEloStatsView(APIView):
    serializer_class = WomensGrassEloStatsViewSerializer

    def get(self, request):
        stats = WomensGrassEloStats.objects.all()
        paginator = MyLimitOffsetPagination()
        paginator.page_size = 5  # Ensure the page size is set to 5
        paginated_players = paginator.paginate_queryset(stats, request)
        serializer = WomensGrassEloStatsViewSerializer(paginated_players, many=True)
        return paginator.get_paginated_response(serializer.data)

    def post(self, request):
        serializer = WomensGrassEloStatsViewSerializer(data=request.data)
        if serializer.is_valid(raise_exception=True):
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)