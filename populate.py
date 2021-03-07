import psycopg2
#from config import connection
import csv
import time
import os
import itertools
import re
import logging
connection = {
    "database": "postgres",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": 5432,
}
conn = psycopg2.connect(**connection)


def populate(file, conn):
    cursor = conn.cursor()
    try:
        os.remove('temporary.csv')
    except OSError:
        pass

    with open(file, 'r', encoding='windows-1251') as data:
        csv_reader = csv.reader(data, delimiter=';', quoting=csv.QUOTE_ALL)
        year = re.findall(r'Odata(\d{4})File.csv', file)

        next(csv_reader)
        count = 0

        with open("temporary.csv", "w+") as buf:
            buf.seek(0, 0)
            buf.truncate(0)
            for line in csv_reader:
                for i in range(len(line)):
                    if re.match('^\\d+,\\d+$', line[i]):
                        line[i] = line[i].replace(',', '.')

                line = ';'.join(line)
                line = line + ';' + year[0] + '\n'

                buf.write(line)
                count += 1
                if count == 100:
                    buf.seek(0, 0)
                    cursor.copy_from(buf, 'zno', sep=';', null='null')
                    conn.commit()
                    buf.seek(0, 0)
                    buf.truncate(0)
                    count = 0


            if count > 0:
                buf.seek(0, 0)
                cursor.copy_from(buf, 'zno', sep=';', null='null')
                conn.commit()



populate("Odata2019File.csv", conn)
populate("Odata2020File.csv", conn)