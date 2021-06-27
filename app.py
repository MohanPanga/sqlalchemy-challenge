from flask import Flask, jsonify
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
import datetime as dt
import numpy as np
import pandas as pd

app = Flask(__name__)
engine =  create_engine('sqlite:///./Resources/hawaii.sqlite')
Base =  automap_base()
Base.prepare(engine, reflect=True)
Measurement = Base.classes.measurement
Station = Base.classes.station


@app.route("/")
def index():
    return ("Available Routes:<br/>"
            f"/api/v1.0/precipitation<br/>"
            f"/api/v1.0/stations<br/>"
            f"/api/v1.0/tobs<br/>"
            f"/api/v1.0/<start><br/>" 
            f"/api/v1.0/<start>/<end><br/>")

@app.route("/api/v1.0/precipitation")
def  precipitation():
    session =  Session(bind=engine)
    precipitation = session.query(Measurement.date, Measurement.prcp).order_by(Measurement.date.desc())
    # Calculate the date 1 year ago from the last data point in the database
    # convert string to datetime format 
    last_date = dt.datetime.strptime(precipitation.first()[0], '%Y-%m-%d') 
    one_year_ago = last_date - dt.timedelta(days=365)
    result = precipitation.filter(Measurement.date > one_year_ago).all()
    # result_list = list(np.ravel(result))
    session.close()
    return jsonify(result)

@app.route("/api/v1.0/stations")
def station():
    session =  Session(bind=engine)
    stations = session.query(Station.station, Station.name).all()
    session.close()
    return jsonify(stations)

@app.route("/api/v1.0/tobs")
def temperature():
    session = Session(bind=engine)
    station_total =  session.query(Measurement.station, func.count(Measurement.tobs).label('Total')).group_by(Measurement.station).all()
    station_total_df = pd.DataFrame(station_total)
    station_most_active = station_total_df[station_total_df['Total']==station_total_df['Total'].max()]['station'].values[0]
    temperature = session.query(Measurement.date, Measurement.tobs).filter(Measurement.station == station_most_active).order_by(Measurement.date.desc())
    # Calculate the date 1 year ago from the last data point in the database
    # convert string to datetime format 
    last_date = dt.datetime.strptime(temperature.first()[0], '%Y-%m-%d') 
    one_year_ago = last_date - dt.timedelta(days=365)
    result = temperature.filter(Measurement.date > one_year_ago).all()
    session.close()
    return jsonify(result)

@app.route("/api/v1.0/<start>")
def tobs_start(start):
    start_time = dt.datetime.strptime(start,'%Y-%m-%d')
    session=Session(bind=engine)
    result =  session.query(Measurement.station, func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).group_by(Measurement.station).filter(Measurement.date>=start_time).all()
    session.close()
    return jsonify(result)

@app.route("/api/v1.0/<start>/<end>")
def tobs_start_end(start,end):
    session = Session(bind=engine)
    start_time = dt.datetime.strptime(start,'%Y-%m-%d')
    end_time = dt.datetime.strptime(end,'%Y-%m-%d')
    result =  session.query(Measurement.station, func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).group_by(Measurement.station).filter(Measurement.date>=start_time).filter(Measurement.date<=end_time).all()
    session.close()
    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True)