import pickle

model_pkl = pickle.load(open('pipeline/model/model.pkl', 'rb'))


def predict(df):
    """
    function takes the input data from flask
    parameters: df -> dataframe, model->pickle
    return: result -> predicted price
    """
    # ID column was added since in app.py the scalar dataset required an index #
    df["ID"] = int(df["ID"])
    df["property_type_HOUSE"] = int(df["property_type_HOUSE"])
    df["property_type_OTHERS"] = int(df["property_type_OTHERS"])
    df["property_type_APARTMENT"] = int(df["property_type_APARTMENT"])
    df["rooms_number"] = float(df["rooms_number"])
    df["area"] = float(df["area"])
    df["province_Brussels_Capital_Region"] = int(df["province_Brussels_Capital_Region"])
    df["province_Liège"] = int(df["province_Liège"])
    df["province_Walloon_Brabant"] = int(df["province_Walloon_Brabant"])
    df["province_West_Flanders"] = int(df["province_West_Flanders"])
    df["province_Flemish_Brabant"] = int(df["province_Flemish_Brabant"])
    df["province_Luxembourg"] = int(df["province_Luxembourg"])
    df["province_Antwerp"] = int(df["province_Antwerp"])
    df["province_East_Flanders"] = int(df["province_East_Flanders"])
    df["province_Hainaut"] = int(df["province_Hainaut"])
    df["province_Limburg"] = int(df["province_Limburg"])
    df["province_Namur"] = int(df["province_Namur"])

    result = model_pkl.predict(df)

    result = str(result).replace('[', ' ').replace(']', ' ')

    return result
