import psycopg2
from config import connection

conn = psycopg2.connect(**connection)

cursor = conn.cursor()
cursor.execute("DROP TABLE zno")
cursor.close()
conn.commit()
conn.close()