from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.pagination import LimitOffsetPagination
from .paginations import MyLimitOffsetPagination
from .models import TennisMatch, Player
from .serializer import TennisMatchSerializer, PlayerViewSerializer

class TennisMatchView(APIView):
    serializer_class = TennisMatchSerializer

    def get(self, request):
        matches = TennisMatch.objects.all()
        paginator = MyLimitOffsetPagination()
        paginator.page_size = 5  # Ensure the page size is set to 5
        paginated_matches = paginator.paginate_queryset(matches, request)
        serializer = TennisMatchSerializer(paginated_matches, many=True)
        return paginator.get_paginated_response(serializer.data)

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
        paginator = MyLimitOffsetPagination()
        paginator.page_size = 5  # Ensure the page size is set to 5
        paginated_players = paginator.paginate_queryset(players, request)
        serializer = PlayerViewSerializer(paginated_players, many=True)
        return paginator.get_paginated_response(serializer.data)

    def post(self, request):
        serializer = PlayerViewSerializer(data=request.data)
        if serializer.is_valid(raise_exception=True):
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
