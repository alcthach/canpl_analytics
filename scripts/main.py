import os
import regex as re
import pandas as pd

directory = '/home/athach/projects/canpl_analytics/data/raw/players/games'  # Replace with the actual directory path

files = os.listdir(directory)

for file in files:
    print(file)

    year = re.findall(r'\d+', file)
    year_str = ''.join(year)
    table_name = f'player_games_{year_str}'
  
    print(table_name)
    print('')


def write_to_database():
    df = pd.read_csv(file, nrows=0)

    column_names = df.columns.tolist()

    engine = create_engine('postgresql://duriandaddy:beanpole@localhost:5432/canpl_dev')
    schema = 'players_raw'

    df.to_sql(table_name, engine, schema=schema, if_exists='replace', index=False)

# Might want to consider how you're going to loop through and make a table for each file
# Just get it working, then do what you need to make it 'production ready'

def main():
    print('Hello world!')

if __name__ == '__main__':
    main()
