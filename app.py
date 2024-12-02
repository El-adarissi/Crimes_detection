import pandas as pd
from flask import Flask, render_template, request
from pickle import load
import folium
import numpy as np
from sklearn.calibration import LabelEncoder
from sklearn.preprocessing import MinMaxScaler
from sklearn.base import BaseEstimator, TransformerMixin

app = Flask(__name__)

class MultiColumnLabelEncoder(BaseEstimator, TransformerMixin):
    def __init__(self, columns=None):
        self.columns = columns

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X_copy = X.copy()
        for col in self.columns:
            le = LabelEncoder()
            X_copy[col] = le.fit_transform(X_copy[col])
        return X_copy

# Load the model 
clf = load(open('model.pkl', 'rb'))
@app.route('/', methods=['GET'])
def Crime_model():
    result=''
    proba_crime=0
    input_data=0
    return  render_template('index.html')


@app.route('/predict',methods=['POST','GET'])
def predict():
    district_value = request.form.get('district')
    month_value = int(request.form.get('Month'))
    day_of_week_value = request.form.get('dayOfWeek')
    hour_value = int(request.form.get('hour'))
    lat_value = float(request.form.get('lat'))
    long_value = float(request.form.get('long'))
    minute_value = int(request.form.get('minute'))
    type_value = request.form.get('type')
   
    # Create a DataFrame with the input data
    input_data = pd.DataFrame({
    'DISTRICT': [district_value],
    'MONTH': [month_value],
    'DAY_OF_WEEK': [day_of_week_value],
    'HOUR': [hour_value],
    'Lat': [lat_value],
    'Long': [long_value],
    'minute': [minute_value],
    'Type': [type_value]
    })

    #prediction = clf.predict(input_data)
    proba_crime=clf.predict_proba(input_data)
    proba_crime = round(proba_crime[0][1], 2)
    return render_template('index.html',proba_crime=proba_crime)


if __name__ == '__main__':
 app.run(debug=True, port=5000)