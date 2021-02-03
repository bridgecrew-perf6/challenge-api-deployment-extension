# challenge-api-deployment-extension

### Introduction:
This project is an BeCode Bouman group challange for 5 days (Deadline:08/12/2020). 

The project was done by [Davy Nimbona](https://github.com/davymariko) (team leader), [Manasa Devinoolu](https://github.com/manasanoolu7), [Christophe Schellinck](https://github.com/christopheschellinck) and [Selma Esen](https://github.com/selmaesen). 

### Goal:

In that project we have created a prediction model to predict new selling prices for the new properties according to our dataset. The real estate data has been scrapped, celeaned and analized in our previous projects. The goal of this project is creating an API to get an new property input and return a price as output. The API Wrapped by a Docker file and deployed by Heroku. 
EXTENSION: This project is an extension because it was improved for deployment and instead of csv files, we made use of SQLite Database files. As such we were learning how to use, to create and consult a Database

### The input:
```json
{
    "area": int,
    "property-type": "APARTMENT" | "HOUSE" | "OTHERS",
    "rooms-number": int,
    "zip-code": int,
    "garden": Optional[bool],
    "equipped-kitchen": Optional[bool],
    "furnished": Opional[bool],
    "terrace": Optional[bool],
    "facades-number": Optional[int]
}
```

Area, property-type, rooms-number and zip-code are required(mandatory) in order to run the application

### The output:
```json
{
    "prediction": [float],
}
```
In case we have the right data
```json
{
    "error": Optional[str]
}
```
In case we have wrong or missing data

### File structure:

    .
    ├── ...
    ├── docker                    
    │   ├── Dockerfile                           
    ├── pipeline                    
    │   ├── model
    │       ├── model.py
    │       ├── model.pkl
    │       ├── ready_to_model_df.csv
    │   ├── predict
    │       ├── prediction.py
    │   ├── preprocessing 
    │       ├── cleaning_data.py
    │       │── test-dataframe.csv
    ├── Procfile
    ├── app.py
    ├── requirements.txt
    ├── README.md
    



### Details:

The ***model_featured.py*** file makes use of Linear regression and dumps the regressor into a pickle file.

The ***prediction.py*** file imports and loads the pickle file with data for the prediction and executes the prediction being a price for given inputs.

The ***data_cleaning.py*** file deals with cleaning and preprocessing. Processing the data ready to feed the model and make predictions. Also dealing with error messages if the user provides incorrect input.


### Run
You can access the application on this [Link](https://davy-api.herokuapp.com/).
- Home: "/"
- Predict page: "/predict":
* GET: Returns the data format you need to input
* POST: Returns the predicted price or error message in case of error

To run it on your local machine:
- Clone the project
- Install the requirements for this project by running:
```bash
pip install -r requirements.txt
```
- Then run file:
```bash
app.py
```
- Test it using [Postman](https://www.postman.com/)

## Docker File

#### image creation
docker build -f docker/Dockerfile . -t image_name:tag_name

#### docker run
docker run -it image_name:tag_name


## Heroku 

### You can find the link [here](http://davy-api.herokuapp.com/)

- access welcome page by */welcome*. 

- access price prediction page by */predict* (Use Postman)

## Extension part
- database application instead of csv file with the immo data
