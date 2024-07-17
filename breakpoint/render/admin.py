from django.contrib import admin
from .models import *

# Register your models here.
admin.site.register(MensTennisMatch)
admin.site.register(WomensTennisMatch)

admin.site.register(MensPlayer)
admin.site.register(WomensPlayer)

admin.site.register(MensFullEloStats)
admin.site.register(MensHardEloStats)
admin.site.register(MensClayEloStats)
admin.site.register(MensGrassEloStats)

admin.site.register(WomensFullEloStats)
admin.site.register(WomensHardEloStats)
admin.site.register(WomensClayEloStats)
admin.site.register(WomensGrassEloStats)