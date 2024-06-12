from src.util import *


# +
###### 2) sanity check for the paths:

# departure time >= arrival time of the previous stop
# transfer time: >= 2 min
# walking time <= 10 min
# -
def path_sanity_check(paths):
    for path in paths:
        path, cost = path
        for i in range(len(path) - 1):
            node, predecessor, arrival_time = path[i]
            next_node, next_predecessor, next_arrival_time = path[i + 1]
            
            arrival_minutes = arrival_time   
            departure_minutes = time_to_minutes2(next_predecessor[3]) if next_predecessor \
                                                    and len(next_predecessor) > 3 else arrival_minutes

            # Departure time has to be later than the arrival time.
            assert arrival_minutes <= departure_minutes, f"Sanity check failed at path{i}: {node} \
                ({minutes_to_hours(arrival_minutes)} >= {minutes_to_hours(departure_minutes)})"
   
            # For transfer stop, the departure time should be at least 2 min after the last arrival.
            if node.endswith("-transfer") and i > 0:
                previous_arrival_time = path[i - 1][2]
                previous_arrival_minutes = previous_arrival_time
                # 1 min loose transfer step is already included in the travel time, 
                # so we only have to guarantee 1 min transfer between stops
                assert arrival_minutes - previous_arrival_minutes >= 1, f"Sanity check failed at path{i}: {node}\
                (Transfer arrival time less than 2 minutes after previous arrival: {minutes_to_hours(previous_arrival_minutes)}\
                to {minutes_to_hours(arrival_minutes)})"
            
            # Each walking segment has to be shorter than 10 min.
            if next_predecessor and next_predecessor[2] == 'walking':
                assert departure_minutes - arrival_minutes <= 10, f"Sanity check failed at path{i}: {node}\
                (Walking time more than 10 minutes: {minutes_to_hours(arrival_minutes)} to {minutes_to_hours(departure_minutes)})"
    print("All paths passed the sanity check.")  

