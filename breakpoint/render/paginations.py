from rest_framework.pagination import LimitOffsetPagination

class MyLimitOffsetPagination(LimitOffsetPagination):
    # pass
    
    # Two records are shown in a single page
    default_limit = 5