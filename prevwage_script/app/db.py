import mysql.connector
from app.config import DB_CONFIG

def get_db_conn():
    return mysql.connector.connect(**DB_CONFIG)