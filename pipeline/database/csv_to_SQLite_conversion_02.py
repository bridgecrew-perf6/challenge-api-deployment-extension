

import sqlite3
import pandas as pd

def create_immo_table():
    """
    This function makes the SQLite database
    """
    connection = sqlite3.connect('immo_data.db')
    cursor = connection.cursor()

    cursor.execute('CREATE TABLE IF NOT EXISTS immo(ID integer primary key, property_type_HOUSE integer, '
                   'property_type_OTHERS integer, property_type_APARTMENT integer, price float, rooms_number float,'
                   'area float, equipped_kitchen float, furnished float, terrace float, garden float, '
                   'facades_number float, province_Brussels_Capital_Region int, province_Liège integer,'
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


def create_immo_table_TEST():
    """
    This function makes the SQLite database
    """
    connection = sqlite3.connect('immo_data_TEST.db')
    cursor = connection.cursor()

    cursor.execute('CREATE TABLE IF NOT EXISTS immoTEST(ID integer primary key, property_type_HOUSE integer, '
                   'property_type_OTHERS integer, property_type_APARTMENT integer, price float, rooms_number float,'
                   'area float, equipped_kitchen float, furnished float, terrace float, garden float, '
                   'facades_number float, province_Brussels_Capital_Region int, province_Liège integer,'
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

create_immo_table()
create_immo_table_TEST()
load_csv_df()
load_csv_df_TEST()