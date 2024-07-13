from sqlalchemy import create_engine, MetaData, Table

devengine = create_engine("sqlite:///bets_sqllite.db")
connection = devengine.connect()

metadata = MetaData()

# Reflect only the tables you want to delete
metadata.reflect(bind=devengine)

exclude_tables = ['AllMatches', 'Elo_AllMatches_All', 'Elo_AllMatches_Clay', 'Elo_AllMatches_Hard', 'Elo_AllMatches_Grass',
                  'results_all_1', 'results_clay_1', 'results_grass_1', 'results_hard_1']

# Drop all tables except those in the exclude list
for table_name in metadata.tables:
    if table_name not in exclude_tables:
        table = Table(table_name, metadata, autoload_with=devengine)
        table.drop(devengine)

# Close the connection
connection.close()

# Close the connection
connection.close()