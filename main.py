import psycopg2
import psycopg2.errorcodes
import csv
import datetime
import itertools


def connect_db():
    conn = psycopg2.connect(database="postgres", user="postgres", password="12zx34", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    return conn, cur

def reconnect_db(log):
    print("Падіння бази даних. Очікування відновлення.")
    log.write(str(datetime.datetime.now()) + " - З'єднання перервано\n")
    connection_restored = False
    while not connection_restored:
        try:
            conn, cur = connect_db()
            log.write(str(datetime.datetime.now()) + " - З'єднання відновлено\n")
            connection_restored = True
        except psycopg2.OperationalError as e:
            pass
    print("З'єднання відновлено.")
    return conn, cur
def create_table(filename, conn, cur):
    row = []
    with open(filename, "r", encoding="cp1251") as csv_file:
        header = csv_file.readline().split(';')
        row = [s.strip('"') for s in header]
        row[-1] = row[-1].rstrip('"\n')
        csv_file.close()
    # формуємо запит для створення колонок таблиці
    columns = "\n\tYear INT,"
    for s in row:
        # тип поля 'рік народження' - ціле число
        if s == 'Birth':
            columns += '\n\t' + s + ' INT,'
        # тип поля з оцінками - дійсне число
        elif 'Ball' in s:
            columns += '\n\t' + s + ' REAL,'
        # поле 'outid' головний ключ таблиці
        elif s == 'OUTID':
            columns += '\n\t' + s + ' VARCHAR(40) PRIMARY KEY,'
        # всі інші поля створюємо текстовими
        else:
            columns += '\n\t' + s + ' VARCHAR(255),'
    # сам запит на створення таблиці
    create_table_query = '''CREATE TABLE IF NOT EXISTS ZNO_table (''' + columns.rstrip(',') + '\n);'
    cur.execute(create_table_query)
    conn.commit()
    return conn, cur, row

# запис даних у таблицю
def insert_from_file(filename, header, year, conn, cur, log):
    start_time = datetime.datetime.now()
    log.write(str(start_time) + " - Відкрито файл " + filename + '\n')
    with open(filename, "r", encoding="cp1251") as csv_file:
        print("Зчитування файлу " + filename)
        csv_reader = csv.DictReader(csv_file, delimiter=';')
        lines = 0
        max_lines = 100
        inserted_all = False
        while not inserted_all:
            try:
                query = '''INSERT INTO ZNO_table (year, ''' + ', '.join(header) + ') VALUES '
                count = 0
                for row in csv_reader:
                    count += 1
                    for s in row:
                        if row[s] == 'null':
                            pass
                        elif 'ball100' in s.lower():
                            row[s] = row[s].replace(',', '.')
                        elif ('ball' not in s.lower()) and (s.lower() != 'birth'):
                            row[s] = "'" + row[s].replace("'", "''") + "'"
                    query += '\n\t(' + str(year) + ', ' + ','.join(row.values()) + '),'
                    if count == max_lines:
                        count = 0
                        query = query.rstrip(',') + ';'
                        cur.execute(query)
                        conn.commit()
                        lines += 1
                        query = '''INSERT INTO ZNO_table (year, ''' + ', '.join(header) + ') VALUES '
                if count != 0:
                    query = query.rstrip(',') + ';'
                    cur.execute(query)
                    conn.commit()
                inserted_all = True
            # якщо з'єднання втрачено
            except psycopg2.OperationalError as e:
                if e.pgcode == psycopg2.errorcodes.ADMIN_SHUTDOWN:
                    conn, cur = reconnect_db(log)
                    csv_file.seek(0, 0)
                    csv_reader = itertools.islice(csv.DictReader(csv_file, delimiter=';'),
                                                  lines * max_lines, None)
    end_time = datetime.datetime.now()
    log.write(str(end_time) + " - Файл зчитаний\n")
    log.write('Витрачений час - ' + str(end_time - start_time) + '\n\n')
    return conn, cur


def write_result(result_file, conn, cur):
    print("Виконання запиту")
    query = '''
    SELECT REGNAME, Year, min(histBall100) 
    FROM ZNO_table
    WHERE histTestStatus = 'Зараховано' 
    GROUP BY REGNAME, Year;'''
    cur.execute(query)
    with open(result_file, 'w', newline='', encoding="cp1251") as csv_file:
        csv_writer = csv.writer(csv_file)
        # Зберігаємо заголовки
        csv_writer.writerow(['Область', 'Рік', 'Найгірший бал з Історії України'])
        # Збергіаємо результати запиту
        for row in cur:
            csv_writer.writerow(row)
    return conn, cur


logs_file = open('logs.txt', 'w')
connect, cursor = connect_db()
cursor.execute('DROP TABLE IF EXISTS ZNO_table;')
connect.commit()
# Створюємо таблицю
connect, cursor, headline = create_table('Odata2020File.csv', connect, cursor)
# Читаємо інформацію з файлів та записуємо в таблицю
connect, cursor = insert_from_file("Odata2019File.csv", headline, 2019, connect, cursor, logs_file)
connect, cursor = insert_from_file("Odata2020File.csv", headline, 2020, connect, cursor, logs_file)
# Створюємо запрос та зберігаємо результат в файл
connect, cursor = write_result('result.csv', connect, cursor)
# Закриваємо з'єднання
cursor.close()
connect.close()
logs_file.close()
