import asyncio
from asgiref.sync import sync_to_async
from channels.db import database_sync_to_async 
from django.db import models, connection

@database_sync_to_async
def delete_all_objects(db: models.Model):
    db.objects.all().delete()

@database_sync_to_async
def reset_primary_key_sequence(db: models.Model):
    with connection.cursor() as cursor:
        # cursor.execute(f"ALTER SEQUENCE {db._meta.db_table}_id_seq RESTART WITH 1") #psql
        cursor.execute(f"DELETE FROM sqlite_sequence WHERE name='{db._meta.db_table}'") #sqlite

async def reset_database(db: models.Model):
    await delete_all_objects(db)
    await reset_primary_key_sequence(db)


