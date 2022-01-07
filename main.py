import MySQLdb
from getpass import getpass
from a3 import connectDB, runQuery

def getInt(instruction, error_msg = 'invalid input'):
    while True:
        try:
            return int(input(instruction))
        except Exception:
            print(error_msg)

def run():
    main_menu = '''
Main menu
------------------
-1. Display main menu
1. Display wine reviews
2. Display all wines
3. Average price of wines
4. Average points awarded per taster
5. Wines sorted by average points awarded
6. Wineries per country

100. Run custom query

0. Exit
------------------
'''

    user = input('user: ')
    password = getpass()
    database = 'wineReviews'

    db = connectDB(user, password, database)
    cursor = db.cursor()
    cursor.execute(f'USE {database};')

    print(main_menu)

    while True:
        
        choice = getInt('Choose an action: ')

        if(choice == 0):
            db.close()
            return

        elif(choice == -1):
            print(main_menu)

        elif(choice == 1):
            min_year = getInt('vintage min: ')
            max_year = getInt('vintage max: ')

            min_points = getInt('min points: ')
            max_points = getInt('max points: ')

            query = '''
SELECT Taster.name taster, Wine.name wine, Wine.variety, Wine.year, 
    Review.points, Wine.price, Review.comment
FROM Taster RIGHT JOIN 
	Review ON Taster.id = Review.taster_id LEFT JOIN 
    Wine ON Wine.id = Review.wine_id
WHERE
    points >= %s AND
    points <= %s AND
    year >= %s AND
    year <= %s AND
    price IS NOT NULL
ORDER BY points DESC;
'''
            values = [min_points, max_points, min_year, max_year]
            result = runQuery(cursor, query, values)

            print('taster | wine | variety | vintage | points | price | comment')
            for row in result:
                print(*row, sep = ' | ')

        elif(choice == 2):
            query = 'Select Wine.name, Wine.variety, Wine.year from Wine'

            result = runQuery(cursor, query, [])

            print('wine | variety | vintage')
            for row in result:
                print(*row, sep = ' | ')

        elif(choice == 3):
            min_year = getInt('vintage min: ')
            max_year = getInt('vintage max: ')

            query = '''
SELECT AVG(price)
FROM Wine
WHERE
    year >= %s AND
    year <= %s;
'''
            result = runQuery(cursor, query, [min_year, max_year])
            print(f'Average price: {result[0][0]}')

        elif(choice == 4):
            query = '''
SELECT Taster.name, AVG(Review.points), COUNT(Review.points)
FROM Review LEFT JOIN Taster
    ON Review.taster_id = Taster.id
GROUP BY Taster.id
ORDER BY Taster.name
'''
            result = runQuery(cursor, query)

            print('taster | average points | number reviews')
            for row in result:
                print(*row, sep = ' | ')

        elif(choice == 5):
            runQuery(cursor, 'DROP VIEW IF EXISTS wine_avg_scores', [])
            
            query = '''
CREATE VIEW wine_avg_scores AS
SELECT Wine.name, AVG(Review.points) points_avg, COUNT(Review.points) reviews
FROM Review LEFT JOIN Wine
	ON Review.wine_id = Wine.id
GROUP BY Wine.name
ORDER BY points_avg DESC
'''

            runQuery(cursor, query)
            db.commit()

            query = 'SELECT * FROM wine_avg_scores'
            result = runQuery(cursor, query, [])

            print('wine | average points awarded | nb reviews')
            for row in result:
                print(*row, sep = ' | ')

        elif(choice == 6):
            query = '''
SELECT Location.country, COUNT(Winery.name) wineries
FROM Location INNER JOIN Winery ON
	Location.id = Winery.location_id
GROUP BY Location.country
ORDER BY wineries DESC
'''

            result = runQuery(cursor, query)
            print('Country | nv wineries')
            for row in result:
                print(*row, sep = ' | ')

        elif(choice == 100):
            query = input('Enter query: ')
            result = runQuery(cursor, query)

            for row in result:
                print(*row, sep = ' | ')

if __name__ == '__main__':
    run()
