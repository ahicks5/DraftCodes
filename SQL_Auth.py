import socket
from sqlalchemy import create_engine

# Define MySQL database connection credentials for local
local_host = '3.12.171.148'
local_user = 'root_pc'
local_password = 'pAsSwOrD1!2?3$'
local_database = 'draftcodes'

# Define MySQL database connection credentials for AWS
aws_host = 'localhost'
aws_user = 'root'
aws_password = 'pAsSwOrD1!2?3$'
aws_database = 'draftcodes'

# Get the hostname of the current machine
hostname = socket.gethostname()

# Check if the script is running on your local computer
if 'DESKTOP-CK5LK0P' in hostname:
    host = local_host
    user = local_user
    password = local_password
    database = local_database
else:
    # Assuming any other hostname is your AWS server
    host = aws_host
    user = aws_user
    password = aws_password
    database = aws_database


# Create a SQLAlchemy engine
engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")
