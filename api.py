###############################################################################
# Imports
###############################################################################
import numpy as np
import pandas as pd
import datetime as dt

###############################################################################
# Import SqlAlchemy
###############################################################################
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

###############################################################################
# Import Flask
###############################################################################
from flask import Flask,jsonify

###############################################################################
# Create an engine to connect to the hawaii.sqlite database
###############################################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

###############################################################################
# reflect an existing database into a new model
###############################################################################
Base = automap_base()
Base.prepare(engine, reflect=True)

###############################################################################
# Save references to each table
###############################################################################
Station = Base.classes.station
Measurement = Base.classes.measurement

###############################################################################
# Create our session (link) from Python to the DB
###############################################################################
session = Session(engine)

###############################################################################
# Find the most recent date in the data set.
###############################################################################
Recent_Date_str = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
Recent_Date = dt.date.fromisoformat(Recent_Date_str)

###############################################################################
# Calculate the date one year from the last date in data set.
###############################################################################
Prev_Recent_Date = dt.date(Recent_Date.year-1,Recent_Date.month,Recent_Date.day)
Prev_Recent_Date

###############################################################################
# Perform a query to retrieve the data and precipitation scores
###############################################################################
data_prcp = session.query(Measurement.date,func.max(Measurement.prcp)).\
    filter(Measurement.date >= func.strftime("%Y-%m-%d",Prev_Recent_Date)).\
    group_by(Measurement.date).\
    order_by(Measurement.date).all()

###############################################################################
# Save the query results as a Pandas DataFrame and set the index to the date column
###############################################################################
data_frame = pd.DataFrame(data_prcp, columns=['date', 'prcp'])
data_frame.set_index('date', inplace=True)


###############################################################################
# Use Pandas to calcualte the summary statistics for the precipitation data
###############################################################################
query_data = session.query(Measurement.date,Measurement.prcp).\
    filter(Measurement.date >= func.strftime("%Y-%m-%d",Prev_Recent_Date)).\
    order_by(Measurement.date).all()

prcp_data_frame = pd.DataFrame(query_data, columns=['date', 'prcp'])
prcp_data_frame.set_index('date', inplace=True)
prcp_data_frame
data_prcp_max = prcp_data_frame.groupby(["date"]).max()["prcp"] 
data_prcp_min = prcp_data_frame.groupby(["date"]).min()["prcp"] 
data_prcp_sum = prcp_data_frame.groupby(["date"]).sum()["prcp"] 
data_prcp_count = prcp_data_frame.groupby(["date"]).count()["prcp"] 

dict_data_prcp = {"Max": data_prcp_max
                 ,"Min":data_prcp_min
                 ,"Sum":data_prcp_sum
                 ,"Count":data_prcp_count 
                }

prcp_data_frame_summary = pd.DataFrame(dict_data_prcp)
prcp_data_frame_summary

###############################################################################
# Design a query to calculate the total number stations in the dataset
###############################################################################
total_stations = session.query(Station.station).count()

###############################################################################
# Design a query to find the most active stations (i.e. what stations have the most rows?)
# List the stations and the counts in descending order.
###############################################################################
most_active_stations = session.query(Measurement.station,func.count(Measurement.station)).\
    group_by(Measurement.station).\
    order_by(func.count(Measurement.station).desc())

all_most_total_stations = most_active_stations.all()
all_most_total_stations

###############################################################################
# Using the most active station id from the previous query, calculate the lowest, highest, and average temperature.
###############################################################################
most_active_station_id = most_active_stations.first()[0]
most_active_station_id

temp_summ = session.query(func.min(Measurement.tobs),func.max(Measurement.tobs),func.avg(Measurement.tobs)).\
    filter(Measurement.station == most_active_station_id).all()


###############################################################################
# Using the most active station id
# Query the last 12 months of temperature observation data for this station and plot the results as a histogram
###############################################################################
temperature_data = session.query(Measurement.date,Measurement.tobs).\
    filter(Measurement.date >= func.strftime("%Y-%m-%d",Prev_Recent_Date), Measurement.station == most_active_station_id).\
    order_by(Measurement.date).all()

data_frame_ann_tobs = pd.DataFrame(temperature_data, columns=['date', 'tobs'])
data_frame_ann_tobs.set_index('date', inplace=True)

qryStations = session.query(Station.station,Station.name, Station.latitude, Station.longitude, Station.elevation).all()
data_frame_stations = pd.DataFrame(qryStations, columns=['station', 'name','latitude','longitude','elevation'])
data_frame_stations.set_index('station', inplace=True) 

###############################################################################
# Close Session
###############################################################################
session.close()

###############################################################################
# Create a Flask App
###############################################################################
app = Flask(__name__)

###############################################################################
# Create a route that returns a list of all available api routes
###############################################################################
@app.route("/")
def index():
    return (
        f"Available Routes:<br/>"
        # Precipitation data
        f"<a href=./api/v1.0/precipitation>api/v1.0/precipitation</a><br/>"
        # Stations data
        f"<a href=./api/v1.0/stations>api/v1.0/stations</a><br/>"
        # Temperature data
        f"<a href=./api/v1.0/tobs>api/v1.0/tobs</a><br/>"
        # Temperature data for a given start date
        f"<a href=./api/v1.0/start_date>api/v1.0/start_date</a><br/>"
        # Temperature data for a given start and end date
        f"<a href=./api/v1.0/start_end_date>api/v1.0/start_end_date</a><br/>"
    )

###############################################################################
# Create a route that returns a list of all precipitation data
###############################################################################
@app.route("/api/v1.0/precipitation")
def precipitation():
    result={}
    for index, row in prcp_data_frame_summary.iterrows():
        result[index]=dict(row)
    return jsonify(result) 

###############################################################################
# Create a route that returns a list of all stations
###############################################################################
@app.route("/api/v1.0/stations")
def stations():
    result={}
    for index, row in data_frame_stations.iterrows():
        result[index]=dict(row)
    return jsonify(result) 

###############################################################################
# Create a route that returns a list of all temperature data
###############################################################################
@app.route("/api/v1.0/tobs")
def tobs():
    result={}
    for index, row in data_frame_ann_tobs.iterrows():
        result[index]=dict(row)   
    return jsonify(result)

###############################################################################
# Create a route that returns a list of all temperature data
###############################################################################
@app.route("/api/v1.0/<start>")
def fromstartdate(start):
    session = Session(engine)
    qry_fr_start_date = session.query(
            func.max(Measurement.tobs).label("TMAX"),
            func.avg(Measurement.tobs).label("TAVG"),
            func.min(Measurement.tobs).label("TMIN")
            ).\
        filter(Measurement.date >= start).all()

    data_frame_fr_start_date = pd.DataFrame(qry_fr_start_date, columns=['TMAX', 'TAVG', 'TMIN'])
    result = data_frame_fr_start_date.iloc[0].to_dict()

    session.close()
    return jsonify(result)

###############################################################################
# Create a route that returns a list of all temperature data
###############################################################################
@app.route("/api/v1.0/<start>/<end>")
def fromrange(start,end):
    session = Session(engine)
    qry_fromrange = session.query(
            func.max(Measurement.tobs).label("TMAX"),
            func.avg(Measurement.tobs).label("TAVG"),
            func.min(Measurement.tobs).label("TMIN")
            ).\
        filter(Measurement.date >= start, Measurement.date <= end).all()

    data_frame_fromrange = pd.DataFrame(qry_fromrange, columns=['TMAX', 'TAVG', 'TMIN'])
    result = data_frame_fromrange.iloc[0].to_dict()

    session.close()
    return jsonify(result)



    return f"Start {start}? End: {end}"

###############################################################################
# Create if main method
###############################################################################
if __name__ == "__main__":
    # Run app, set debug to true, and port to 5000
    app.run(debug=True)





