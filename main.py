import psycopg2
import csv
import datetime
import itertools


def connect_db():
    conn = psycopg2.connect(database="postgres", user="postgres", password="12zx34", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    return conn, cur


def create_table(filename, conn, cur):
    line = []
    with open(filename, "r", encoding="cp1251") as csv_file:
        header = csv_file.readline().split(';')
        line = [word.strip('"') for word in header]
        line[-1] = line[-1].rstrip('"\n')
        csv_file.close()
    # формуємо запит для створення колонок таблиці
    columns = "\n\tYear INT,"
    for word in line:
        # тип поля 'рік народження' - ціле число
        if word == 'Birth':
            columns += '\n\t' + word + ' INT,'
        # тип поля з оцінками - дійсне число
        elif 'Ball' in word:
            columns += '\n\t' + word + ' REAL,'
        # поле 'outid' головний ключ таблиці
        elif word == 'OUTID':
            columns += '\n\t' + word + ' VARCHAR(40) PRIMARY KEY,'
        # всі інші поля створюємо текстовими
        else:
            columns += '\n\t' + word + ' VARCHAR(255),'
    # сам запит на створення таблиці
    create_table_query = '''CREATE TABLE IF NOT EXISTS ZNO_table (''' + columns.rstrip(',') + '\n);'
    cur.execute(create_table_query)
    conn.commit()
    return conn, cur, line


def insert_from_file(filename, header, year, conn, cur, log):
    """Заповнює таблицю даними з заданого csv-файлу. Оброблює ситуації, пов'язані з
    втратою з'єднання з базою даних. Створює файл, в який записує, скільки минуло часу на
    виконання запиту.
    filename -- назва csv-файлу з даними.
    year -- рік, якому відповідає даний csv-файл.
    conn -- об'єкт з'єднання з БД.
    cursor -- курсор для даної БД.
    log -- файл для запису логів.
    Повертає conn i cursor (оскільки ці об'єкти можуть бути оновлені). """
    start_time = datetime.datetime.now()
    log.write(str(start_time) + " -- відкриття файлу " + filename + '\n')
    with open(filename, "r", encoding="cp1251") as csv_file:
        # починаємо читати дані з csv-файлу та формувати insert-запит
        # дані зчитуються партіями
        print("Читаємо файл " + filename)
        csv_reader = csv.DictReader(csv_file, delimiter=';')
        batches_inserted = 0
        batch_size = 100
        inserted_all = False
        # поки не вставили всі рядки
        while not inserted_all:
            try:
                insert_query = '''INSERT INTO ZNO_table (year, ''' + ', '.join(header) + ') VALUES '
                count = 0
                for row in csv_reader:
                    count += 1
                    # обробляємо запис: оточуємо всі текстові рядки одинарними лапками, замінюємо в числах кому на крапку
                    for key in row:
                        if row[key] == 'null':
                            pass
                        elif (key.lower() != 'birth') and ('ball' not in key.lower()):
                            row[key] = "'" + row[key].replace("'", "''") + "'"
                        elif 'ball100' in key.lower():
                            row[key] = row[key].replace(',', '.')
                    insert_query += '\n\t(' + str(year) + ', ' + ','.join(row.values()) + '),'
                    # якщо набралося 100 рядків -- коммітимо транзакцію
                    if count == batch_size:
                        count = 0
                        insert_query = insert_query.rstrip(',') + ';'
                        cur.execute(insert_query)
                        conn.commit()
                        batches_inserted += 1
                        insert_query = '''INSERT INTO ZNO_table (year, ''' + ', '.join(header) + ') VALUES '
                # якщо досягли кінця файлу -- коммітимо транзакцію
                if count != 0:
                    insert_query = insert_query.rstrip(',') + ';'
                    cur.execute(insert_query)
                    conn.commit()
                inserted_all = True
            except psycopg2.OperationalError as e:
                # якщо з'єднання з базою даних втрачено
                if e.pgcode == psycopg2.errorcodes.ADMIN_SHUTDOWN:
                    print("База даних впала -- чекаємо на відновлення з'єднання")
                    log.write(str(datetime.datetime.now()) + " -- втрата з'єднання\n")
                    connection_restored = False
                    while not connection_restored:
                        try:
                            # намагаємось підключитись до бази даних
                            conn, cur = connect_db()
                            log.write(str(datetime.datetime.now()) + " -- відновлення з'єднання\n")
                            connection_restored = True
                        except psycopg2.OperationalError as e:
                            pass
                    print("З'єднання відновлено! Продовжуємо роботу")
                    csv_file.seek(0, 0)
                    csv_reader = itertools.islice(csv.DictReader(csv_file, delimiter=';'),
                                                  batches_inserted * batch_size, None)
    end_time = datetime.datetime.now()
    log.write(str(end_time) + " -- файл повністю оброблено\n")
    log.write('Витрачено часу на даний файл -- ' + str(end_time - start_time) + '\n\n')
    return conn, cur


def write_result(result_file, conn, cur):
    """Виконує запит до таблиці та записує результат у новий csv-файл. """
    print("Робимо запрос та записуємо результат у файл " + result_file)
    query = '''
    SELECT regname, year, min(histBall100) 
    FROM zno_table
    WHERE histTestStatus = 'Зараховано' 
    GROUP BY regname, year;'''
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
cursor.execute('DROP TABLE IF EXISTS zno_table;')
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
