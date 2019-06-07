from flask import Flask, render_template, jsonify
from .bhavcopy_parse import *

app = Flask(__name__)

@app.route('/')
def index():
    ''' Render index html page '''

    return render_template('index.html')

@app.route('/equity_data')
def equity_data():
    ''' Get latest 10 equity data from redis database '''

    data = { "data" : get_latest_equity()}
    
    return jsonify(data)

@app.route('/load_dataset')
def load_dataset():
    ''' Download latest equity file from BSE and
    Store its data into redis db
    '''
    
    load_bhavcopy_data()    
    data = { "status" : "Success"}

    return jsonify(data)

def load_bhavcopy_data():
    ''' Load all data when server started '''

    download_bhavcopy()
    equity_filename = extract_bhavcopy_zip()
    store_bhavcopy_data(equity_filename)

# Load all data when server started
load_bhavcopy_data()