import sqlite3
import pandas as pd
from flask import Flask, request, jsonify

from pipeline.predict.prediction import predict
from pipeline.preprocessing.cleaning_data import preprocess




app = Flask(__name__)


@app.route("/", methods=["GET"])
def home():
    """
    function returns string on welcome page
    parameters: GET
    return: "Alive!"
    """
    return "Alive!"


@app.route('/welcome', methods=["GET"])
def welcome():
    """
    function returns string on home page
    parameters: GET
    return: "Welcome to API Deployment"
    """
    return "Welcome to API Deployment"


@app.route("/predict", methods=["GET", "POST"])
def predict_api():
    """
    function returns predicted price
    parameters: GET,POST
    return: "The predicted price is VALUE"
    """
    if request.method == "POST":
        data = request.get_json()
#        dataset = pd.DataFrame(data)
        dataset = pd.DataFrame(data, index=[0, ])
        dataset.replace(True, int(1), inplace=True)
        dataset.replace(False, int(0), inplace=True)
        new_df = preprocess(dataset)

        if isinstance(new_df, str):
            message = {
                "ERROR": new_df
            }
            return jsonify(message)
        else:
            result = float(predict(new_df).strip())
            message = {
                "Predicted price": round(result, 2)
            }
            return jsonify(message)
    elif request.method == "GET":
        message = "The page accept a POST request of data in following format:\n"
        data = "<p>{<br>area': int,<br>'property_type': 'APARTMENT' | 'HOUSE' \
            | 'OTHERS',<br>'rooms_number': int,<br>'zip_code': int,<br>'garden'\
            : Optional[bool],<br>'equipped_kitchen': Optional[bool],<br>\
            'furnished': Opional[bool],<br>'terrace': Optional[bool],<br>\
            'facades_number': Optional[int]<br>}"
        
        return (message+data)


if __name__ == '__main__':
    app.run(debug=True)
