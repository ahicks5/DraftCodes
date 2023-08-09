import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from combineDBs import load_database

# Create a connection to the database
#connection = mysql.connector.connect(
#    host='3.12.171.148',  # Server, usually localhost
#    user='root',  # your username, e.g., root
#    password='pAsSwOrD1!2?3$',  # your password
#    database='draftcodes'  # Name of the database
#)

# Define your MySQL database connection credentials
host = '3.12.171.148'
user = 'root_pc'
password = 'pAsSwOrD1!2?3$'
database = 'draftcodes'


def download_db(db_name, db_csv):
    df = load_database(db_name)
    df.to_csv(db_csv, index=False)
    print('DB Saved')


def upload_db(db_name, db_csv):
    # Create a SQLAlchemy engine
    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

    df = pd.read_csv(db_csv)

    # Upload the DataFrame to MySQL as a new table
    df.to_sql(name=db_name, con=engine, if_exists='replace', index=False)

    print('DB Uploaded')


if __name__ == '__main__':
    download_db('combined_data', 'combined_data_8623.csv')
    #upload_db()