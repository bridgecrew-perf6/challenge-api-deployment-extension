import sqlite3
import pandas as pd
import numpy as np
#import missingno as msno
import re


def define_province(new_df, code):
    if ((code >= 1000) & (code < 1299)):
        new_df["province_Brussels_Capital_Region"] = 1
    elif ((code >= 1300) & (code < 1499)):
        new_df["province_Walloon_Brabant"] = 1
    elif (((code >= 1500) & (code < 1999)) | ((code >= 3000) & (code < 3499))):
        new_df["province_Flemish_Brabant"] = 1
    elif ((code >= 2000) & (code < 2999)):
        new_df["province_Antwerp"] = 1
    elif ((code >= 3500) & (code < 3999)):
        new_df["province_Limburg"] = 1
    elif ((code >= 4000) & (code < 4999)):
        new_df["province_Liège"] = 1
    elif ((code >= 5000) & (code < 5999)):
        new_df["province_Namur"] = 1
    elif (((code >= 6000) & (code < 6599)) | ((code >= 7000) & (code < 7999))):
        new_df["province_Hainaut"] = 1
    elif ((code >= 6600) & (code < 6999)):
        new_df["province_Luxembourg"] = 1
    elif ((code >= 8000) & (code < 8999)):
        new_df["province_West_Flanders"] = 1
    elif ((code >= 9000) & (code < 9999)):
        new_df["province_East_Flanders"] = 1

    return new_df


def define_property(new_df, type):
    if type == "HOUSE":
        new_df["property_type_HOUSE"] = 1
    elif type == "APARTMENT":
        new_df["property_type_APARTMENT"] = 1
    elif type == "OTHERS":
        new_df["property_type_OTHERS"] = 1
    
    return new_df


def preprocess(df):
    mandatory = ["area", "property_type", "rooms_number", "zip_code"]

    check_data = 0
    check_zip = 0
    check_type = 0

    for m in mandatory:
        if m not in df.columns:
            check_data = 1

    if df["property_type"].values[0] not in ["APARTMENT", "HOUSE", "OTHERS"]:
        check_type = 1
    
    if df["zip_code"].values[0] < 1000 or df["zip_code"].values[0] >= 9999:
        check_zip = 1

    message = ""
    if check_data == 1:
        message += "Mandatory data missing"
    if check_zip == 1:
        if len(message)> 1:
            message +=" - "
        message += "Invalid Zip Code, must be [1000-9998]"
    if check_type == 1:
        if len(message)> 1:
            message +=" - "
        message += "Wrong property type. Must be HOUSE, APARTMENT or OTHERS"
    
    if len(message) > 1:
        return message

    new_df = pd.read_csv("pipeline/database/test-dataframe.csv")

    new_df.drop('ID', axis=1, inplace=True)
    new_df = define_province(new_df, df["zip_code"].values[0])
    new_df = define_property(new_df, df["property_type"].values[0])
#    print(new_df.columns)
    columns = [column for column in df.columns if column not in ["property_type", "zip_code"]]
    new_df[columns] = df[columns]
    
    return new_df


