import mysql.connector
import pandas as pd
from sqlalchemy import create_engine

# Create a connection to the database
connection = mysql.connector.connect(
    host='3.12.171.148',  # Server, usually localhost
    user='root',  # your username, e.g., root
    password='pAsSwOrD1!2?3$',  # your password
    database='draftcodes'  # Name of the database
)

# Define your MySQL database connection credentials
host = '3.12.171.148'
user = 'root_pc'
password = 'pAsSwOrD1!2?3$'
database = 'draftcodes'

# Create a SQLAlchemy engine
engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

df = pd.read_csv('Bovada_Data.csv')

# Upload the DataFrame to MySQL as a new table
df.to_sql(name='bovada_data', con=engine, if_exists='replace', index=False)


# Create a cursor object
#cursor = connection.cursor()

# Execute a SQL query
#cursor.execute("""
#    SHOW TABLES;
#""")

# Fetch all rows
#rows = cursor.fetchall()

# Loop through rows
#for row in rows:
#    print(row)

# Close the cursor
#cursor.close()

# Close the connection
connection.close()