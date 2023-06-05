import random
import string
import time
from flask import Flask, jsonify, request
from pymongo.mongo_client import MongoClient
import pandas as pd
import pytz
from datetime import datetime, timedelta

app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

# Database connection
client = MongoClient('mongodb+srv://aga:asdf@cluster0.cwaivaf.mongodb.net/loop_monitor?retryWrites=true&w=majority')
db = client['loop_monitor']
store_collection = db['stores']
report_collection = db['reports']

# Load CSV data into MongoDB
# Make sure to run these lines of code only if we want to add data to mongoDB else comment Load_csv_data
# def load_csv_data(csv_url, collection):
#     df = pd.read_csv(csv_url)
#     data = df.to_dict(orient='records')
#     collection.insert_many(data)

# # Load CSV data into MongoDB collections
# load_csv_data('store_status.csv', store_collection)
# load_csv_data('business_hours.csv', db['business_hours'])
# load_csv_data('timezones.csv', db['timezones'])

# Utility function to get store's timezone
def get_store_timezone(store_id):
    timezone_doc = db['timezones'].find_one({'store_id': store_id})
    if timezone_doc:
        return timezone_doc['timezone_str']
    return 'America/Chicago'

# Utility function to convert local time to UTC
def local_to_utc(local_time, store_id):
    local_tz = pytz.timezone(get_store_timezone(store_id))
    local_dt = local_tz.localize(local_time)
    utc_dt = local_dt.astimezone(pytz.utc)
    return utc_dt

# Utility function to convert UTC to local time
def utc_to_local(utc_time, store_id):
    store_timezone = get_store_timezone(store_id)
    local_tz = pytz.timezone(store_timezone)
    local_dt = utc_time.astimezone(local_tz)
    return local_dt

# Calculate uptime and downtime for a given store and time interval
def calculate_uptime_downtime(store_id, start_time, end_time):
    store_timezone = get_store_timezone(store_id)
    store_local_tz = pytz.timezone(store_timezone)
   
    business_hours = db['business_hours'].find_one({'store_id': store_id})
    print(business_hours,'chal',store_id)
    if not business_hours:
        return timedelta(), timedelta()  # Store open 24*7, no downtime

    day_of_week = start_time.weekday()
    start_time_local = datetime.combine(start_time.date(), datetime.strptime(business_hours['start_time_local'], '%H:%M:%S').time())
    end_time_local = datetime.combine(start_time.date(), datetime.strptime(business_hours['end_time_local'], '%H:%M:%S').time())
    start_time_local = store_local_tz.localize(start_time_local)
    end_time_local = store_local_tz.localize(end_time_local)
   
    if start_time_local >= end_time_local:
        # Store opens before midnight and closes after midnight
        if start_time_local <= start_time <= end_time_local:
            # Store is open for the entire interval
            return end_time - start_time, timedelta()
        elif start_time <= start_time_local:
            start_time = start_time_local
        else:
            start_time = start_time_local + timedelta(days=1)

        if start_time <= end_time_local:
            end_time = end_time_local
        else:
            return timedelta(), end_time - start_time

    if start_time_local <= start_time <= end_time_local:
        start_time = max(start_time, start_time_local)
    else:
        start_time = start_time_local if start_time < start_time_local else start_time_local + timedelta(days=1)

    if start_time_local <= end_time <= end_time_local:
        end_time = min(end_time, end_time_local)
    else:
        end_time = end_time_local if end_time > end_time_local else end_time_local - timedelta(days=1)

    return end_time - start_time, timedelta()
def generate_report(report_id):
    report_data = []

    # Retrieve the necessary data from the database
    stores = store_collection.find()
    for store in stores:
        print("hi")
        store_id = store['store_id']
        status_records = db['status_records'].find({'store_id': store_id}).sort('timestamp_utc')
        last_status = None
        last_status_time = None
        uptime_last_hour = timedelta()
        downtime_last_hour = timedelta()
        uptime_last_day = timedelta()
        downtime_last_day = timedelta()
        uptime_last_week = timedelta()
        downtime_last_week = timedelta()

        for status_record in status_records:
            timestamp_utc = status_record['timestamp_utc']
            status = status_record['status']

            if last_status is None:
                last_status = status
                last_status_time = timestamp_utc
                continue

            time_diff = timestamp_utc - last_status_time
            downtime, uptime = (time_diff, timedelta()) if last_status == 'active' else (timedelta(), time_diff)
            downtime_last_hour += downtime if time_diff <= timedelta(hours=1) else timedelta()
            downtime_last_day += downtime if time_diff <= timedelta(days=1) else timedelta()
            downtime_last_week += downtime if time_diff <= timedelta(weeks=1) else timedelta()
            uptime_last_hour += uptime if time_diff <= timedelta(hours=1) else timedelta()
            uptime_last_day += uptime if time_diff <= timedelta(days=1) else timedelta()
            uptime_last_week += uptime if time_diff <= timedelta(weeks=1) else timedelta()

            last_status = status
            last_status_time = timestamp_utc

        # Fill the entire business hours interval with uptime and downtime
        current_time = datetime.now(pytz.utc)  # Use current timestamp
        business_hours_interval = pd.date_range(start=current_time - timedelta(days=7), end=current_time, freq='H')
        for interval_start in business_hours_interval[:-1]:
            interval_end = interval_start + timedelta(hours=1)

            interval_start_local = utc_to_local(interval_start, store_id)
            interval_end_local = utc_to_local(interval_end, store_id)
            uptime, downtime = calculate_uptime_downtime(store_id, interval_start_local, interval_end_local)
            print(uptime,downtime,"meh")
            uptime_last_hour += uptime
            downtime_last_hour += downtime
            uptime_last_day += uptime
            downtime_last_day += downtime
            uptime_last_week += uptime
            downtime_last_week += downtime

        # Convert timedelta to minutes/hours
        uptime_last_hour_minutes = int(uptime_last_hour.total_seconds() / 60)
        uptime_last_day_hours = int(uptime_last_day.total_seconds() / 3600)
        downtime_last_hour_minutes = int(downtime_last_hour.total_seconds() / 60)
        downtime_last_day_hours = int(downtime_last_day.total_seconds() / 3600)
        downtime_last_week_hours = int(downtime_last_week.total_seconds() / 3600)

        if last_status_time is not None:
            update_last_week_hours = int((current_time - last_status_time).total_seconds() / 3600)
        else:
            update_last_week_hours = 0

       

        report_data.append({
            'store_id': store_id,
            'uptime_last_hour': uptime_last_hour_minutes,
            'uptime_last_day': uptime_last_day_hours,
            'update_last_week': update_last_week_hours,
            'downtime_last_hour': downtime_last_hour_minutes,
            'downtime_last_day': downtime_last_day_hours,
            'downtime_last_week': downtime_last_week_hours
        })
        print(report_data)
    
    # Store the report data in the database or generate a CSV file
    report_collection.update_one({'report_id': report_id}, {'$set': {'status': 'Complete'}})
    return report_data
# API Endpoint: /trigger_report
@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    report_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))  # Generate random report ID here
    start_time = datetime.now(pytz.utc)  # Use current timestamp as start time
    # Start a background task or create a separate thread to generate the report
    # Store report_id, start_time, and status in the database
    report_collection.insert_one({'report_id': report_id, 'start_time': start_time, 'status': 'Running'})
    
    return jsonify({'report_id': report_id})

# API Endpoint: /get_report
@app.route('/get_report', methods=['GET'])
def get_report():
    report_id = request.args.get('report_id')
    report = report_collection.find_one({'report_id': report_id})
    generate_report(report_id)
    if report:
        if report['status'] == 'Complete':
            # Fetch the report data from the database or CSV file
            # and return it along with "Complete" status
            return jsonify({'status': 'Complete', 'report_data': '<report_data_as_csv>'})
        else:
            return jsonify({'status': 'Running'})
    return jsonify({'status': 'Report not found'})



# Example usage:
if __name__ == '__main__':
    # Trigger the report generation
    with app.app_context():
        report_id = trigger_report()

    # Poll the status of the report
    with app.app_context():
        status = 'Running'
        while status == 'Running':
            response = get_report(report_id)
            status = response.json['status']
            if status == 'Complete':
                report_data = response.json['report_data']
                print(report_data)  # Display the report data or save it to a file
            else:
                time.sleep(1)  # Wait for 1 second before checking the status again
