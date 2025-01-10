import sqlite3
import requests
from tqdm import tqdm

from flask import Flask, request, jsonify
import json 
import numpy as np
import pandas as pd
from datetime import datetime

app = Flask(__name__) 

#################### Routes ####################

#home
@app.route('/')
def home():
    return 'Hello World'

#get all stations
@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

#get stations by id
@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    station_id = get_station_id(station_id, conn)
    return station_id.to_json()

#get all trips
@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()

#get trip by id
@app.route('/trips/<trip_id>')
def route_trip_id(trip_id):
    conn = make_connection()
    trip_id = get_trip_id(trip_id, conn)
    return trip_id.to_json()

#Handling JSON data as input
@app.route('/json', methods=['POST']) 
def json_example():
    
    req = request.get_json(force=True)
    
    name = req['name']
    age = req['age']
    address = req['address']
    
    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')

#add stations
@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

#add trips
@app.route('/trips/add', methods=['POST'])
def route_add_trips():
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result

#get all average durations from station A to stasion B
@app.route('/trips/average_duration/') 
def route_average_stations():
    conn = make_connection()
    average_time_to_each_stations = get_average_duration(conn)
    return average_time_to_each_stations

#get average duration per bike
@app.route('/trips/average_duration/<bike_id>') 
def route_duration_perbike(bike_id):
    conn = make_connection()
    average_time_perbike = get_average_duration_perbike(bike_id, conn)
    return average_time_perbike

#update station status
@app.route('/stations/status', methods=['POST'])
def route_update_station_status():

    data = request.get_json(force=True) 
    
    station_id = data.get('station_id')
    status = data.get('status')

    if not station_id or not status:
        return jsonify({"error": "station_id and status are required"}), 400

    conn = make_connection()

    modified_date = update_station_status(station_id, status, conn)

    if modified_date is None:
        return jsonify({"error": "Station not found"}), 404

    return jsonify({
        "message": f"Station {station_id} status updated to {status}",
        "station_id": station_id,
        "status": status,
        "modified_date": modified_date
    }), 200

#################### Routes ####################
    

#################### Functions ####################

# Make connections to sql server
def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection
conn = make_connection()

def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

def get_station_id(station_id, conn):
    query_station_id = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query_station_id, conn)
    return result 

def get_all_trips(conn):
    query_trips_all = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query_trips_all, conn)
    return result

def get_trip_id(trip_id, conn):
    query_trip_id = f"""SELECT * FROM trips WHERE id = {trip_id}"""
    result = pd.read_sql_query(query_trip_id, conn)
    return result

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def insert_into_trips(data_trips, conn):
    query_trips = f"""INSERT INTO trips values {data_trips}"""
    try:
        conn.execute(query_trips)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def get_average_duration (conn):
    query_average_duration = f'''SELECT * FROM trips'''
    result = pd.read_sql_query(query_average_duration, conn)
    result_avg = result.groupby(['start_station_name', 'end_station_name'])['duration_minutes'].mean().reset_index()
    return result_avg.to_json(orient='records')

def get_average_duration_perbike(bike_id, conn):
    query_bike_id = f"""SELECT * FROM trips WHERE bikeid = {bike_id}"""
    result = pd.read_sql_query(query_bike_id, conn, parse_dates='start_time')
    result['days']=result['start_time'].dt.day_name()
    result_avg = pd.pivot_table(
                    data=result,
                    index='days',
                    values='duration_minutes',
                    aggfunc='mean'
                    )
    return result_avg.to_json()

def update_station_status(station_id, status, conn):

    modified_date = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    query = """
    UPDATE stations
    SET status = ?, modified_date = ?
    WHERE station_id = ?
    """
    result = conn.execute(query, (status, modified_date, station_id))
    conn.commit()

    if result.rowcount == 0:
        return None  
    
    return modified_date  

#################### Functions ####################















if __name__ == '__main__':
    app.run(debug=True, port=5000)

