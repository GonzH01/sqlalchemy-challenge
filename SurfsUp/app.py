# Import required libraries
from flask import Flask, jsonify
import datetime as dt
from sqlalchemy import create_engine, func
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base

#################################################
# Database Connection Setup
#################################################

# Initialize connection to the climate database
climate_engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# Reflect the existing climate database
BaseModel = automap_base()
BaseModel.prepare(climate_engine, reflect=True)

# Map database tables to Python classes
ClimateMeasurement = BaseModel.classes.measurement
ClimateStation = BaseModel.classes.station

# Establish session link to the database
climate_session = Session(climate_engine)

#################################################
# Flask Application Configuration
#################################################
climate_app = Flask(__name__)

#################################################
# API Endpoints Configuration
#################################################
@climate_app.route("/")
def welcome():
    """List all available API endpoints."""
    return (
        f"Climate Data API Endpoints:<br/>"
        f"/api/v1.0/rainfall: Last 12 months of rainfall data<br/>"
        f"/api/v1.0/weather_stations: Weather station list<br/>"
        f"/api/v1.0/temperature_observations: Temperature data for the top station in the last year<br/>"
        f"/api/v1.0/start: Temperature stats from a start date<br/>"
        f"/api/v1.0/start/end: Temperature stats between start and end dates<br/>"
    )

@climate_app.route("/api/v1.0/rainfall")
def rainfall():
    """Retrieve last 12 months of rainfall data."""
    latest_date = climate_session.query(func.max(ClimateMeasurement.date)).scalar()
    one_year_prior = dt.datetime.strptime(latest_date, '%Y-%m-%d') - dt.timedelta(days=365)
    rainfall_data = climate_session.query(ClimateMeasurement.date, ClimateMeasurement.prcp).\
        filter(ClimateMeasurement.date >= one_year_prior).all()
    rainfall_dict = {date: prcp for date, prcp in rainfall_data}
    return jsonify(rainfall_dict)

@climate_app.route("/api/v1.0/weather_stations")
def weather_stations():
    """List all weather stations."""
    station_data = climate_session.query(ClimateStation.station).all()
    station_list = [station[0] for station in station_data]
    return jsonify(station_list)

@climate_app.route("/api/v1.0/temperature_observations")
def temp_observations():
    """Temperature data for the top station in the last year."""
    active_station = climate_session.query(ClimateMeasurement.station, func.count(ClimateMeasurement.station).label('station_activity')).\
        group_by(ClimateMeasurement.station).\
        order_by(func.count(ClimateMeasurement.station).desc()).first()
    active_station_id = active_station[0]
    recent_date_active = climate_session.query(func.max(ClimateMeasurement.date)).\
        filter(ClimateMeasurement.station == active_station_id).scalar()
    year_ago_active = dt.datetime.strptime(recent_date_active, '%Y-%m-%d') - dt.timedelta(days=365)
    temp_data_active = climate_session.query(ClimateMeasurement.date, ClimateMeasurement.tobs).\
        filter(ClimateMeasurement.station == active_station_id,
               ClimateMeasurement.date >= year_ago_active).all()
    temp_list_active = [{"date": date, "temperature": tobs} for date, tobs in temp_data_active]
    return jsonify(temp_list_active)

@climate_app.route("/api/v1.0/<start>")
@climate_app.route("/api/v1.0/<start>/<end>")
def temperature_stats(start, end=None):
    """Return temperature statistics for a specified date range."""
    temp_stats_query = [
        func.min(ClimateMeasurement.tobs).label('min_temp'),
        func.avg(ClimateMeasurement.tobs).label('avg_temp'),
        func.max(ClimateMeasurement.tobs).label('max_temp')
    ]
    if not end:
        start_date = dt.datetime.strptime(start, "%Y-%m-%d")
        results = climate_session.query(*temp_stats_query).\
            filter(ClimateMeasurement.date >= start_date).all()
    else:
        start_date = dt.datetime.strptime(start, "%Y-%m-%d")
        end_date = dt.datetime.strptime(end, "%Y-%m-%d")
        results = climate_session.query(*temp_stats_query).\
            filter(ClimateMeasurement.date >= start_date).\
            filter(ClimateMeasurement.date <= end_date).all()
    climate_session.close()
    temp_stats = list(np.ravel(results))
    return jsonify(temp_stats)

if __name__ == '__main__':
    climate_app.run(debug=True)
