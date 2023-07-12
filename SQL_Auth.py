import mysql.connector
from sqlalchemy import create_engine

# Define your MySQL database connection credentials
host = '3.12.171.148'
user = 'root_pc'
password = 'pAsSwOrD1!2?3$'
database = 'draftcodes'



# Create a SQLAlchemy engine
engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")
