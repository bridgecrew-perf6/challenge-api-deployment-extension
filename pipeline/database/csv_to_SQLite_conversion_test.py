

import sqlite3
import pandas as pd

def create_immo_table():
    """
    This function makes the SQLite database
    """
    connection = sqlite3.connect('immo_data.db')
    cursor = connection.cursor()

    cursor.execute('CREATE TABLE IF NOT EXISTS immo(ID integer primary key, property_type_HOUSE integer, '
                   'property_type_OTHERS integer, property_type_APARTMENT integer, price integer, rooms_number float,'
                   'area float, equipped_kitchen integer, furnished integer, terrace integer, garden integer, '
                   'facades_number integer, province_Brussels_Capital_Region integer, province_Liège integer,'
                   'province_Walloon_Brabant integer, province_West_Flanders integer, province_Flemish_Brabant integer,'
                   'province_Luxembourg integer, province_Antwerp integer, province_East_Flanders integer, '
                   'province_Hainaut integer, province_Limburg integer, province_Namur integer)')

    connection.commit()
    connection.close()

def create_immo_table_TEST():
    """
    This function makes the SQLite database
    """
    connection = sqlite3.connect('immo_data_TEST.db')
    cursor = connection.cursor()

    cursor.execute('CREATE TABLE IF NOT EXISTS immoTEST(ID integer primary key, property_type_HOUSE integer, '
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
    # write the data to a sqlite table
    immo_df.to_sql('immo', connection, if_exists='append', index=False)

    connection.commit()
    connection.close()

def load_csv_df_TEST():

    connection = sqlite3.connect('immo_data_TEST.db')
    cursor = connection.cursor()

    # load the data into a Pandas DataFrame
    immo_df = pd.read_csv('test-dataframe.csv')
    # write the data to a sqlite table
    immo_df.to_sql('immoTEST', connection, if_exists='append', index=False)

    connection.commit()
    connection.close()

def read_immo_table():
    """
    This function makes the SQLite database
    """
    connection = sqlite3.connect('immo_data.db')
    cursor = connection.cursor()

    cursor.execute('SELECT * FROM immo')
 #   immo_table = cursor.fetchall()
    immo_table = [row for row in cursor.fetchall()]
    connection.close()

    df = pd.DataFrame(immo_table)
    df.columns = ['index', 'property_type_HOUSE', 'property_type_OTHERS',
        'property_type_APARTMENT', 'rooms_number', 'area', 'equipped_kitchen',
        'furnished', 'terrace', 'garden', 'facades_number',
        'province_Brussels_Capital_Region', 'province_Liège', "price",
        'province_Walloon_Brabant', 'province_West_Flanders',
        'province_Flemish_Brabant', 'province_Luxembourg', 'province_Antwerp',
        'province_East_Flanders', 'province_Hainaut', 'province_Limburg',
        'province_Namur']
    print(df.head())

def read_immo_table_TEST():
    """
    This function makes the SQLite database
    """
    connection = sqlite3.connect('immo_data_TEST.db')
    cursor = connection.cursor()

    cursor.execute('SELECT * FROM immoTEST')
 #   immo_table = cursor.fetchall()
    immo_table_TEST = [row for row in cursor.fetchall()]
    connection.close()

    df = pd.DataFrame(immo_table_TEST)
    df.columns = ['index', 'property_type_HOUSE', 'property_type_OTHERS',
        'property_type_APARTMENT', 'rooms_number', 'area', 'equipped_kitchen',
        'furnished', 'terrace', 'garden', 'facades_number',
        'province_Brussels_Capital_Region', 'province_Liège', "price",
        'province_Walloon_Brabant', 'province_West_Flanders',
        'province_Flemish_Brabant', 'province_Luxembourg', 'province_Antwerp',
        'province_East_Flanders', 'province_Hainaut', 'province_Limburg',
        'province_Namur']
    print(df.head())
#books = [{'name': row[0], 'author': row[1], 'read': row[2]} for row in cursor.fetchall()]

create_immo_table()
create_immo_table_TEST()
load_csv_df()
load_csv_df_TEST()
read_immo_table()
read_immo_table_TEST()