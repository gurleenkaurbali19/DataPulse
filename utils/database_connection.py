import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

# Loading environment variables
load_dotenv()

def create_connection():
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            port=os.getenv("DB_PORT")
        )
        if connection.is_connected():
            print("Connected to DataPulse_db successfully!")
        return connection
    except Error as e:
        print(f"Can't connect to DataPulse_db: {e}")
        return None
