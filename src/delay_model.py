import numpy as np
import pandas as pd
from src.util import *
from scipy.stats import norm


def process_istdaten_data(istdaten_path):
    df_istdaten = pd.read_parquet(istdaten_path)
    df_istdaten = process_column_names(df_istdaten)
    df_istdaten = df_istdaten[[
        "fahrt_bezeichner",
        "bpuic",
        "produkt_id",
        "ankunftszeit",
        "an_prognose",
        "an_prognose_status",
        "abfahrtszeit",
        "ab_prognose",
        "ab_prognose_status",
        "faellt_aus_tf"
    ]]   
    # Translate columns to English
    df_istdaten = df_istdaten.rename(columns={
        "fahrt_bezeichner" : "trip_id",
        "bpuic" : "stop_id",
        "produkt_id" : "transport_type",
        "ankunftszeit" : "arrival_time_scheduled",
        "an_prognose" : "arrival_time_actual",
        "an_prognose_status" : "arrival_time_calculation",
        "abfahrtszeit" : "departure_time_scheduled",
        "ab_prognose" : "departure_time_actual",
        "ab_prognose_status" : "departure_time_calculation",
        "faellt_aus_tf" : "trip_failed"
    })
    
    # Change data types
    df_istdaten = df_istdaten.convert_dtypes()
    df_istdaten["arrival_time_scheduled"] = pd.to_datetime(df_istdaten["arrival_time_scheduled"], dayfirst=True)
    df_istdaten["arrival_time_actual"] = pd.to_datetime(df_istdaten["arrival_time_actual"], dayfirst=True)
    df_istdaten["departure_time_scheduled"] = pd.to_datetime(df_istdaten["departure_time_scheduled"], dayfirst=True)
    df_istdaten["departure_time_actual"] = pd.to_datetime(df_istdaten["departure_time_actual"], dayfirst=True)
    
    # Filter for non-empty transport types
    df_istdaten = df_istdaten[df_istdaten["transport_type"] != ""]
    # Filter failed trips (this also filters out empty predictions)
    df_istdaten = df_istdaten[df_istdaten["trip_failed"] == "false"]
    # Filter null values
    df_istdaten = df_istdaten.dropna()
    
    # Calculating the delay and adding it as a new column
    df_istdaten['arrival_delay'] = (df_istdaten['arrival_time_actual'] - df_istdaten['arrival_time_scheduled']).dt.total_seconds()
    # Clip negative delays, because we are not interested in early arrivals
    df_istdaten['arrival_delay'] = df_istdaten['arrival_delay'].clip(lower=0)
    # Adding a column for the day of the week
    df_istdaten['day_of_week'] = df_istdaten['arrival_time_scheduled'].dt.day_name()
    # Extracting the time component from departure_time_scheduled
    df_istdaten['departure_time_only'] = df_istdaten['departure_time_scheduled'].dt.time
    # Adding a column to indicate if the day is a weekend
    df_istdaten['is_weekend'] = df_istdaten['arrival_time_scheduled'].dt.dayofweek >= 5
    # Adding a column to indicate the hour of the trip
    df_istdaten['trip_hour'] = df_istdaten['arrival_time_scheduled'].dt.hour
    # Group data for later use
    grouped_istdaten = df_istdaten.groupby(['stop_id', 'trip_hour', 'is_weekend']).agg(
        mean_delay=('arrival_delay', 'mean'),
        std_dev_delay=('arrival_delay', 'std'),
        count_entries=('arrival_delay', 'count')
    ).reset_index()
    return grouped_istdaten


def confidence_segment(stop_id, trip_hour, max_delay, grouped_istdaten):
    filtered_row = grouped_istdaten[(grouped_istdaten['stop_id'] == stop_id) &
                          (grouped_istdaten['trip_hour'] == trip_hour) &
                          (grouped_istdaten['is_weekend'] == False)]
    if not filtered_row.empty:
        mean_delay = filtered_row['mean_delay'].values[0] / 60
        std_dev_delay = filtered_row['std_dev_delay'].values[0] / 60
        confidence = norm.cdf(max_delay, loc=mean_delay, scale=std_dev_delay)
        return confidence
    else:
        return 1


def route_confidence(route, latest_expected_arrival, grouped_istdaten):
    confidence = 1
    latest_expected_arrival = time_to_minutes2(latest_expected_arrival)
    for i, node in enumerate(route):
        # print(confidence, i)
        (stop_id, predecessor, arrival_time) = node
        if not predecessor:
            continue
        (prev_stop_id, prev_arrival_time, trip_id, prev_departure_time) = predecessor
        prev_arrival_time = time_to_minutes2(prev_arrival_time)
        prev_departure_time = time_to_minutes2(prev_departure_time)
        trip_hour = prev_departure_time // 60

        # Transfer to another segment
        if "transfer" in prev_stop_id:
            prev_stop = prev_stop_id.split('-')[0]
            max_delay = 0.05 + prev_departure_time - prev_arrival_time
            confidence_seg = confidence_segment(prev_stop, trip_hour, max_delay, grouped_istdaten)
            confidence *= confidence_seg
            # print(f'trip_id: {trip_id}; i: {i}; confidence_seg = {confidence_seg}')
        # Walking
        elif trip_id == "walking":
            # Walking initially
            if i == 1:
                continue           
            # Walking in between two stops
            elif i < len(route) - 1:
                next_node_predecessor = route[i + 1][1]
                stop_id_before_walk = prev_stop_id
                arrival_time = time_to_minutes2(next_node_predecessor[1])
                departure_time = time_to_minutes2(next_node_predecessor[3])
                max_delay = departure_time - arrival_time
                confidence_seg = confidence_segment(stop_id_before_walk, trip_hour, max_delay, grouped_istdaten)
                confidence *= confidence_seg
                # print(f'trip_id: {trip_id}; i: {i}; confidence_seg = {confidence_seg}')
                
            # Walking to reach the end
            else:
                max_delay = latest_expected_arrival - arrival_time
                confidence_seg = confidence_segment(prev_stop_id, trip_hour, max_delay, grouped_istdaten)
                confidence *= confidence_seg
                # print(f'trip_id: {trip_id}; i: {i}; confidence_seg = {confidence_seg}')
        # Reached the end of the route
        elif i == len(route) - 1:
                max_delay = latest_expected_arrival - arrival_time
                confidence_seg = confidence_segment(prev_stop_id, trip_hour, max_delay, grouped_istdaten)
                confidence *= confidence_seg
                # print(f'trip_id: {trip_id}; i: {i}; confidence_seg = {confidence_seg}')
    return confidence          
