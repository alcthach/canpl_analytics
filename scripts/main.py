import os
import regex as re
import pandas as pd
from sqlalchemy import create_engine


# Requirements:
# To take the headers for each csv file and write to its own table in my database
# Right now I'm just going to get the thing working for player games, but I'll extend it somehow...
# Also the cognitive load seems very high for this type of work right now...
# And I'm in my least productive window; around 2-4 I would say...

# Mapping out the control flow
# Read in csv; just the header
# Take that df and load to database


# I think it makes sense for me to have schemas for players and teams
# With each schema having tables for game data and totals

schemas_lst = ['players_raw', 'teams_raw']


# Does this mean that I can dynamically code 'directory?'
# I mean for now, I could probably just have one for each directory?

directory = '/home/athach/projects/canpl_analytics/data/raw/'

def list_files(directory):
    files_lst = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            files_lst.append(file_path)  # Or do whatever you want with the file path
    return files_lst

# Control flow: use regex and string operations to figure out what type of table the csv should
# be written to.


def print_file_table_names():
    for file in files:
        print(file)

        year = re.findall(r'\d+', file)
        year_str = ''.join(year)
        table_name = f'player_games_{year_str}'
      
        print(table_name)
        print('')

#def write_to_database(file, table_name, schema):
 #   df = pd.read_csv(file, nrows=0) 

    # column_names = df.columns.tolist()

# Might want to consider how you're going to loop through and make a table for each file
# Just get it working, then do what you need to make it 'production ready'
# A little bit messy, but main() basically has what's required to write each file to the corresponding table


# What behaviour am I looking to see for this script
# Write each corresponding csv to its own table on the database

def get_schema_name():
    pass
    # return schema_name

def get_table_name():
    pass
    # return table_name

def write_to_database(file, table_name, schema):
    df = pd.read_csv(file)
    
    engine = create_engine('postgresql://duriandaddy:beanpole@localhost:5432/canpl_dev')

    df.to_sql(table_name, engine, schema, if_exists='replace', index=False)




def main():
    for file in list_files(directory):
        year_lst = (re.findall('\d+', file))
        year_str = ', '.join(year_lst)
        if re.search(r".PlayerByGame.", file):
            schema = "players_raw"
            table_name = "player_games_" + year_str
            print("Writing to " + table_name + "...")
            write_to_database(file, table_name, schema)
            print("Finished writing to " + table_name + ".")
            
        elif re.search(r".PlayerTotals.", file):
            schema = "players_raw"
            table_name = "player_totals_" + year_str
            print("Writing to " + table_name + "...")
            write_to_database(file, table_name, schema)
            print("Finished writing to " + table_name + ".")

        elif re.search(r".TeamByGame.", file):
            schema = "teams_raw"
            table_name = 'team_games_' + year_str
            print("Writing to " + table_name + "...")
            write_to_database(file, table_name, schema)
            print("Finished writing to " + table_name + ".")

        elif re.search(r".TeamTotals", file):
            schema = "teams_raw"
            table_name = "team_totals_" + year_str   
            print("Writing to " + table_name + "...")
            write_to_database(file, table_name, schema)
            print("Finished writing to " + table_name + ".")

        else:
            print("Something else.")
        print('')

if __name__ == '__main__':
    main()
