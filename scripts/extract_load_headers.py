import pandas as pd
from sqlalchemy import create_engine

df = pd.read_csv('/home/athach/projects/canpl_analytics/data/raw/players/games/CPLPlayerByGame2019.csv', nrows=0)

column_names = df.columns.tolist()

print(f"{len(column_names)}, added.")

# What I could probably do is make a list of files, then loop through each file and pull the headers and write to a table in my database
# Let me get this to work first.

engine = create_engine('postgresql://duriandaddy:beanpole@localhost:5432/canpl_dev')

df.to_sql('player_games_raw_2019', engine, schema='players_raw', if_exists='replace', index=False)