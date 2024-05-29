import pandas as pd
import numpy as np
import networkx as nx
from src.util import *

class TransportNetwork:
    def __init__(self, timetable_path, stop_to_stop_path, stops_path):
        self.timetable = pd.read_parquet(timetable_path)
        self.stop_to_stop = pd.read_csv(stop_to_stop_path, index_col=0)
        self.stops = pd.read_csv(stops_path, index_col=0)
        self.process_data()

    def process_data(self):
        """Process all initial data required for the graph."""
        process_column_names(self.timetable)
        process_column_names(self.stop_to_stop)
        process_column_names(self.stops)

        self.process_stop_names()
        self.extract_weekend()
        self.convert_times()

    def process_stop_names(self):
        """Clean up stop names in all related dataframes."""
        for df in [self.stops, self.timetable]:
            df['stop_id'] = df['stop_id'].apply(process_stop_names)
        self.stop_to_stop['stop_id_a'] = self.stop_to_stop['stop_id_a'].apply(process_stop_names)
        self.stop_to_stop['stop_id_b'] = self.stop_to_stop['stop_id_b'].apply(process_stop_names)
        
        # print("Before drop_duplicates:", self.stop_to_stop.shape)
        # self.stop_to_stop = self.stop_to_stop.drop_duplicates(['stop_id_a', 'stop_id_b'], ignore_index=True)
        self.stop_to_stop = self.stop_to_stop.loc[self.stop_to_stop.groupby(
            ['stop_id_a', 'stop_id_b'])['distance'].idxmax()].reset_index(drop=True)
        self.stop_to_stop = self.stop_to_stop[self.stop_to_stop['stop_id_a'] != self.stop_to_stop['stop_id_b']]
        # print("After drop_duplicates:", self.stop_to_stop.shape)
        self.stops = self.stops.drop_duplicates(['stop_id'], ignore_index=True)

    def extract_weekend(self):
        """Remove trips that only on weekends."""
        weekdays = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']
        weekend_cols = ['saturday', 'sunday']

        self.timetable[weekdays + weekend_cols] = self.timetable[weekdays + weekend_cols].replace({'TRUE': True, 'FALSE': False})

        self.timetable = self.timetable[self.timetable[weekdays].any(axis=1)]

    def convert_times(self):
        """Convert time columns into minutes past midnight for easier calculations."""
        self.timetable['departure_time_mins'] = self.timetable['departure_time'].apply(time_to_minutes)
        self.timetable['arrival_time_mins'] = self.timetable['arrival_time'].apply(time_to_minutes)

    def filter_timetable(self, start_time, duration):
        """Filter timetable for entries within the specified time window."""
        mask = ((self.timetable['arrival_time_mins'] >= start_time) &
                (self.timetable['arrival_time_mins'] <= start_time + duration))
        filtered = self.timetable[mask].copy()
        filtered.sort_values(by=['trip_id', 'arrival_time_mins', 'departure_time_mins'], inplace=True)
        filtered.reset_index(drop=True, inplace=True)
        return filtered

    def build_graph(self, start_time, expected_time):
        """Build a graph based on filtered timetable and walking paths."""
        start_time_mins = time_to_minutes2(start_time)
        expected_arrival_time_mins = time_to_minutes2(expected_time)
        duration = expected_arrival_time_mins - start_time_mins

        timetable_filtered = self.filter_timetable(start_time_mins, duration)
        G = nx.MultiDiGraph()

        # Add transit connections
        for i, row in timetable_filtered.iterrows():
            # Check if the next row is part of the same trip
            if i + 1 < len(timetable_filtered):
                next_row = timetable_filtered.iloc[i + 1]
                if row['trip_id'] == next_row['trip_id']:
                    travel_time = next_row['arrival_time_mins'] - row['departure_time_mins']
                    G.add_edge(row['stop_id'], next_row['stop_id'], weight=travel_time, trip_id=row['trip_id'],
                               departure_time_mins=row['departure_time_mins'], arrival_time_mins=next_row['arrival_time_mins'])

        # Add walking paths
        for _, row in self.stop_to_stop.iterrows():
            walking_time = round(row['distance'] / 50)  # Calculate walking time based on distance
            G.add_edge(row['stop_id_a'], row['stop_id_b'], weight=walking_time, trip_id="walking", walking_time=walking_time)

        return G
