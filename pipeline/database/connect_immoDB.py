import sqlite3
import pandas as pd


def read_immo_table():
    """
    This function reads the SQLite database and transforms it to a variable
    """
    connection = sqlite3.connect('immo_data.db')
    cursor = connection.cursor()

    cursor.execute("SELECT * FROM immo")
    immo_table = [row for row in cursor.fetchall()]

    connection.close()


    ## RETURN THE TABLE TO THE CALLING FILE ##
    ##########################################
    return immo_table


#def read_immo_table_TEST():
    """
    This function reads the SQLite database and transforms it to a variable
    """
#    connection = sqlite3.connect('immo_data_TEST.db')
#    cursor = connection.cursor()

#    cursor.execute("SELECT * FROM immoTEST")
#    immo_table_TEST = [row for row in cursor.fetchall()]

#    connection.close()

    ## RETURN THE TABLE TO THE CALLING FILE ##
    ##########################################
#    return immo_table_TEST