import psycopg2
from config import connection
import csv
import os
conn = psycopg2.connect(**connection)
cursor = conn.cursor()
query = '''
SELECT regname, ZNOYear, avg(histBall100)
FROM zno
WHERE histTestStatus = 'Зараховано'
GROUP BY regname, ZNOYear; '''


cursor.execute(query)
try:
	os.remove("temporary.csv")
except OSError:
	pass
with open('Results.csv','w',encoding="utf-8") as res:
			csv_writer = csv.writer(res, lineterminator='\n')
			csv_writer.writerow(['Область', 'Рік', 'Середній бал з Історії України'])
			for row in cursor:
				csv_writer.writerow(row)























