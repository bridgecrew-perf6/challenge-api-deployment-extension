import sqlite3
import pandas as pd
import numpy as np
#import missingno as msno
import re

import pandas as pd
import numpy as np
import missingno as msno
import re

url = "https://raw.githubusercontent.com/adamflasse/dataCleaning/main/updated_1.csv"
df = pd.read_csv(url)


# Define functions for data preprocessing tasks

def general_clean(df):
    """Remove duplicates and unnecessary columns"""

    # Drop duplicates
    df.drop_duplicates(subset=['price', 'area', 'rooms_number'], inplace=True)

    # Delete postcode and house_is
    df.drop(
        [
            "postcode",
            "house_is",
            "region",
            'building_state',
            'swimming_pool_has'
        ],
        axis=1,
        inplace=True,
    )

    """Handling with Missing values"""

    # Replace Not Specified by np.nan
    df.replace("Not specified", np.nan, inplace=True)

    """Replace True and False by numerical values"""

    df.replace([True, "True", False, "False"], [1, 1, 0, 0], inplace=True)

    """Cleaning PRICE column: removing anomalies and outliers"""

    # Delete price rows with anomalies (extreme, mistaken values, e.g. 123456789)
    df.drop(df[df["price"] > 20000000].index, inplace=True)
    df.drop(df[df["price"] == 12345678].index, inplace=True)

    # Remove price outliers
    index_price = df[(df["price"] < 10000)].index
    df.drop(index_price, inplace=True)

    # Remove anomalies based on price and area features
    index_area_price = df[(df["area"] > 1000) & (df["price"] < 200000)].index
    df.drop(index_area_price, inplace=True)

    # Remove anomalies based on price and room features
    index_rooms_price = df[(df["rooms_number"] < 4) & (df["price"] > 1000000)].index
    df.drop(index_rooms_price, inplace=True)

    """Cleaning AREA column"""

    # Remove area outliers
    index_area = df[(df["area"] < 5)].index
    df.drop(index_area, inplace=True)

    """Cleaning ROOMS_NUMBER column"""

    # Replace room_number values that are == to area by NaN
    df.loc[(df.rooms_number == df.area), "rooms_number"] = np.nan

    """Cleaning PROPERTY_SUBTYPE column"""

    # Formatting values
    df["property_subtype"] = df["property_subtype"].str.lower()
    df["property_subtype"].replace(to_replace="-", value="_", regex=True, inplace=True)

    "Reduce property_subtype groups"

    # Reduce number of property_subtypes by grouping lower frequencied ones into "other" category
    # ("ground_floor" acted as threshold). This is done to reduce noise in the model
    df["property_subtype"] = df["property_subtype"].replace(
        [
            "exceptional_property",
            "flat_studio",
            "mansion",
            "town_house",
            "loft",
            "country_cottage",
            "service_flat",
            "bungalow",
            "farmhouse",
            "triplex",
            "other_property",
            "manor_house",
            "chalet",
            "castle",
            "kot",
            "penthouse",
            "duplex",
            "mixed_use_building",
            "villa",
            "apartment_block",
            "ground_floor"
        ],
        "OTHERS",
    )
    df["property_subtype"].replace({"apartment": "APARTMENT", "house": "HOUSE"}, inplace=True)

    # change column names with proper ones

    df = df.rename(
        columns={
            "property_subtype": "property-type",
            "kitchen_has": "equipped-kitchen",
            "facades_number": "facades-number",
            "rooms_number": "rooms-number",
        }
    )

    df.columns = df.columns.str.replace(' ', '')

    """Leave dataframe ready for next step"""
    # Reset index after dropping
    df = df.reset_index(drop=True)
    return df


def remove_na_all(df):
    # Remove all observations containing missing values
    df.dropna(axis=0, inplace=True)
    return df


def remove_nas_above30perc(df):
    # Remove observations containing more than 30% of missing values.
    # Missing values will be imputed
    df.dropna(axis=0, thresh=8, inplace=True)
    return df


def preprocessing(df):
    # Use sklearn libraries to create dummy variables to make one-hot encoder for our categorical values
    from sklearn.preprocessing import OneHotEncoder
    import category_encoders as ce

    ohe = ce.OneHotEncoder(handle_unknown="ignore", use_cat_names=True)
    df_ohe = ohe.fit_transform(df)
    return df_ohe


df = general_clean(df)
df = remove_nas_above30perc(df)
df = preprocessing(df)

# Impute missing values with means (remember that we have no observations with more than 2 missing values)

from sklearn.impute import SimpleImputer

imputer = SimpleImputer(missing_values=np.NaN, strategy="mean")

df["terrace"] = imputer.fit_transform(df["terrace"].values.reshape(-1, 1))[:, 0]
df["equipped-kitchen"] = imputer.fit_transform(
    df["equipped-kitchen"].values.reshape(-1, 1)
)[:, 0]
df["rooms-number"] = imputer.fit_transform(df["rooms-number"].values.reshape(-1, 1))[
                     :, 0
                     ]
df["garden"] = imputer.fit_transform(df["garden"].values.reshape(-1, 1))[:, 0]
df["furnished"] = imputer.fit_transform(df["furnished"].values.reshape(-1, 1))[:, 0]

# we have stored the clean df as csv file to model

processed_csv = df.to_csv("ready_to_model_df.csv")

#from pipeline.database.connect_immoDB import read_immo_table_TEST

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

#    cnx = sqlite3.connect('pipeline/database/immo_data_TEST.db')
#    new_df = pd.read_sql_query("SELECT * FROM immoTEST", cnx)
    ## INSERTING DATA FROM THE DATABASE ##
#    df_HELP = read_immo_table_TEST()
#    new_df = pd.DataFrame(df_HELP)

#    print(new_df.head())
    new_df = pd.read_csv("pipeline/database/test-dataframe.csv")
#    new_df.drop(["Unnamed: 0"], axis=1, inplace=True)

#    new_df.columns = ['property_type_HOUSE', 'property_type_OTHERS',
    new_df.columns = ['property_type_HOUSE', 'property_type_OTHERS',
        'property_type_APARTMENT', 'rooms_number', 'area', 'equipped_kitchen',
        'furnished', 'terrace', 'garden', 'facades_number',
        'province_Brussels_Capital_Region', 'province_Liège', "price",
        'province_Walloon_Brabant', 'province_West_Flanders',
        'province_Flemish_Brabant', 'province_Luxembourg', 'province_Antwerp',
        'province_East_Flanders', 'province_Hainaut', 'province_Limburg',
        'province_Namur']

    new_df = define_province(new_df, df["zip_code"].values[0])
    new_df = define_property(new_df, df["property_type"].values[0])
    print(new_df.columns)
    columns = [column for column in df.columns if column not in ["property_type", "zip_code"]]
    new_df[columns] = df[columns]
    
    return new_df


