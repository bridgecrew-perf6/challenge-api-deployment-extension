import sqlite3
import pandas as pd

def create_immo_table():
    """
    This function makes the SQLite database
    """
    connection = sqlite3.connect('immo_data.db')
    cursor = connection.cursor()

    cursor.execute('CREATE TABLE IF NOT EXISTS immo(ID integer NOT NULL PRIMARY KEY, property_type_HOUSE integer, '
                   'property_type_OTHERS integer, property_type_APARTMENT integer, price integer, rooms_number float,'
                   'area float, equipped_kitchen integer, furnished integer, terrace integer, garden integer, '
                   'facades_number integer, province_Brussels_Capital_Region integer, province_Liège integer,'
                   'province_Walloon_Brabant integer, province_West_Flanders integer, province_Flemish_Brabant integer,'
                   'province_Luxembourg integer, province_Antwerp integer, province_East_Flanders integer, '
                   'province_Hainaut integer, province_Limburg integer, province_Namur integer)')

    connection.commit()
    connection.close()

def load_csv_df():

    connection = sqlite3.connect('immo_data.db')
    cursor = connection.cursor()

    # load the data into a Pandas DataFrame
    immo_df = pd.read_csv('ready_to_model_df.csv')

    immo_df.to_sql('immo', connection, if_exists='append', index=False)

    connection.commit()
    connection.close()

    print(immo_df.columns)


def read_immo_table():
    """
    This function reads the SQLite databasepredic
    """
    connection = sqlite3.connect('immo_data.db')
    cursor = connection.cursor()

    cursor.execute('SELECT * FROM immo')

    immo_table = [row for row in cursor.fetchall()]
    connection.close()

    df = pd.DataFrame(immo_table)

    df.columns = ['ID', 'property_type_HOUSE', 'property_type_OTHERS', 'property_type_APARTMENT', 'price', 'rooms_number', 'area', 'equipped_kitchen', 'furnished', 'terrace', 'garden', 'facades_number', 'province_Brussels_Capital_Region', 'province_Liège', 'province_Walloon_Brabant', 'province_West_Flanders', 'province_Flemish_Brabant', 'province_Luxembourg', 'province_Antwerp', 'province_East_Flanders', 'province_Hainaut', 'province_Limburg', 'province_Namur']

    print(df.head())

create_immo_table()
load_csv_df()
read_immo_table()