import MySQLdb
from MySQLdb.cursors import SSCursor
from datetime import datetime, timezone, date
import time
from csv import reader
from getpass import getpass
import re

def connectDB(user: str, password: str, database: str, host: str = 'localhost', port: int = 3306):
    return MySQLdb.connect(user = user, password = password, host = host, port = port, cursorclass = SSCursor)

def createDB(user: str, password: str, database: str, host: str = 'localhost', port: int = 3306):
    db = connectDB(user, password, database, host, port)
    cursor = db.cursor()
    cursor.execute(f'DROP DATABASE IF EXISTS {database}')
    cursor.execute(f'CREATE DATABASE {database}')
    cursor.execute(f'USE {database}')
    return db

def setUpDB(cursor):
    cursor.execute('''
        CREATE TABLE Location(
            id INT(9) UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,
            country VARCHAR(60) NOT NULL,
            province VARCHAR(90) NOT NULL,
            description TEXT,
            PRIMARY KEY(id),
            CONSTRAINT location_info UNIQUE(country, province)
        );
        ''')

    cursor.execute('''
        CREATE TABLE Winery(
            name VARCHAR(255) NOT NULL UNIQUE,
            location_id INT(9) UNSIGNED NOT NULL,
            address VARCHAR(100),
            mail VARCHAR(350) UNIQUE,
            phone INT(15) UNIQUE,
            description TEXT,
            PRIMARY KEY(name),
            FOREIGN KEY(location_id) REFERENCES Location(id)
        );
        ''')

    cursor.execute('''
        CREATE TABLE Taster(
            id INT(9) UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,
            name VARCHAR(255) DEFAULT '' NOT NULL,
            twitter_handle VARCHAR(90) DEFAULT '' NOT NULL,
            bio TEXT,
            PRIMARY KEY(id),
            CONSTRAINT taster_info UNIQUE(name, twitter_handle)
        );
        ''')

    cursor.execute('''
        CREATE TABLE Wine(
            winery_name VARCHAR(255) NOT NULL,
            id INT(9) UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,
            price FLOAT(6),
            name VARCHAR(255) NOT NULL,
            variety VARCHAR(255),
            year int(4) NOT NULL,
            description TEXT,
            PRIMARY KEY(id),
            FOREIGN KEY(winery_name) REFERENCES Winery(name),
            CONSTRAINT wine_info UNIQUE(name, year)
        );
        ''')

    cursor.execute('''
        CREATE TABLE Review(
            wine_id INT(9) UNSIGNED NOT NULL REFERENCES Wine(id),
            taster_id INT(9) UNSIGNED NOT NULL,
            id INT(9) UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,
            title VARCHAR(255) NOT NULL,
            points INT(3) NOT NULL,
            comment TEXT,
            PRIMARY KEY(id),
            FOREIGN KEY(taster_id) REFERENCES Taster(id)
        );
        ''')

def getLocationId(country, province):
    query = '''
    SELECT id
    FROM Location
    WHERE
        country = %s AND
        province = %s
    '''
    values = [country, province]

    cursor.execute(query, values)
    return cursor.fetchone()[0]

def getWineId(wine_name, wine_year):
    query = '''
    SELECT id
    FROM Wine
    WHERE
        name = %s AND
        year = %s
    '''
    values = [wine_name, wine_year]

    cursor.execute(query, values)
    return cursor.fetchone()[0]

def getTasterId(taster_name, taster_twitter_handle):
    query = '''
    SELECT id
    FROM Taster
    WHERE
        name = %s AND
        twitter_handle = %s
    '''
    values = [taster_name, taster_twitter_handle]

    cursor.execute(query, values)
    return cursor.fetchone()[0]

def addData(cursor,
            country,
            description,
            designation,
            points,
            price,
            province,
            region_1,
            region_2,
            taster_name,
            taster_twitter_handle,
            title,
            variety,
            winery):
    
    if(country == '' or title == '' or winery == '' or points == ''):
        return 0

    if(taster_name == '' and taster_twitter_handle == ''):
        taster_name = 'Unregistered user'
            
    year = re.findall(r"[0-9]{4}", title)

    if(len(year) == 0):
        return 0

    if(len(year) > 1):
        year = list(map(int, year))
        year = min(year, key = lambda x: abs(x - 2000))
    else:
        year = int(year[0])

    title = re.sub('[\(\[].*?[\)\]]', '', title)

    wine_name = title[: title.index(str(year))].strip()

    if(wine_name == winery):
        wine_name = title[title.index(str(year)) + 4:].strip()

    if(wine_name == ''):
        return 0

    price = float(price) if price != '' else None
    points = int(points) if points != '' else None
    variety = None if variety == '' else variety

    query = 'INSERT INTO Location(country, province) VALUES (%s, %s) ON DUPLICATE KEY UPDATE country = country'
    values = [country, province]
    cursor.execute(query, values)

    location_id = getLocationId(country, province)

    query = 'INSERT INTO Winery(name, location_id) VALUES (%s, %s) ON DUPLICATE KEY UPDATE name = name'
    values = [winery, location_id]
    cursor.execute(query, values)

    query = 'INSERT INTO Taster(name, twitter_handle) VALUES (%s, %s) ON DUPLICATE KEY UPDATE id = id'
    values = [taster_name, taster_twitter_handle]
    cursor.execute(query, values)

    query = 'INSERT INTO Wine(winery_name, price, name, variety, year, description) VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE id = id'
    values = [winery, price, wine_name, variety, year, description]
    cursor.execute(query, values)

    wine_id = getWineId(wine_name, year)
    taster_id = getTasterId(taster_name, taster_twitter_handle)

    query = 'INSERT INTO Review(wine_id, taster_id, title, points) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE id = id'
    values = [wine_id, taster_id, title, points]
    cursor.execute(query, values)

    return 1
    
def importData(fileName: str, cursor):
    with open(fileName, 'r') as file:
        csvReader = reader(file)
        next(csvReader)
        
        for _, country, description, designation, points, price, province, region_1, region_2, taster_name, taster_twitter_handle, title, variety, winery in csvReader:
            addData(cursor, country, description, designation, points, price, province, region_1, region_2, taster_name, taster_twitter_handle, title, variety, winery)

def runQuery(cursor, query, values = []):
    cursor.execute(query, values)
    return cursor.fetchall()

if __name__ == '__main__':
    user = input('user: ')
    password = getpass()
    
    if(True):
        startTime = time.time()
        db = createDB(user, password, 'wineReviews')
        setUpDB(db.cursor())
        print(f'Constructed database in {time.time() - startTime} seconds')

        
        db.commit()
        db.close()

    if(True):
        db = connectDB(user, password, 'wineReviews')
        cursor = db.cursor()
        cursor.execute(f'USE wineReviews;')

        startTime = time.time()
        importData('wineData.csv', cursor)
        print(f'Imported data in {time.time() - startTime} seconds')
        db.commit()
        db.close()
