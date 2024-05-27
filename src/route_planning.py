import numpy as np
import heapq
import copy
from src.util import *

import logging
logging.basicConfig(level=logging.DEBUG)

def dijkstra(G, start_time, departure_id, destination_id, preds):
    
    start_time_mins = time_to_minutes2(start_time)
    min_arrival_time = {node: np.inf for node in G.nodes()}
    min_arrival_time[departure_id] = start_time_mins
    depart_time = {node: np.inf for node in G.nodes()}
    priority_queue = [(start_time_mins, departure_id)]
    predecessor = {node: None for node in G.nodes()}

    if preds != 0:
        predecessor[departure_id] = preds

    while priority_queue:
        current_time, current_stop = heapq.heappop(priority_queue)
                
        if current_stop == destination_id:
            break

        for neighbor in G[current_stop]:
            for key, edge_attr in G[current_stop][neighbor].items():
                trip_id = edge_attr['trip_id']
                # transfer time at the same stop
                transfer_time = 1 if trip_id != 'walking' and predecessor[current_stop] and \
                                        predecessor[current_stop][2] != trip_id and predecessor[current_stop][2] != "walking" else 0
                departure_time = edge_attr['departure_time_mins'] if trip_id != 'walking' else current_time
                travel_time = edge_attr['arrival_time_mins'] - departure_time if trip_id != 'walking' else edge_attr['walking_time']
                total_time = current_time + transfer_time + max(0, departure_time - current_time - transfer_time) + travel_time # current time + wait time + travel time + transfer time
                # step for transfer/walking
                transfer_step = 1 if transfer_time == 1 else 0
                walking_step = 1 if predecessor[current_stop] and predecessor[current_stop][2] == 'walking' else 0
                min_departure_time = current_time + transfer_time + transfer_step + walking_step
                
                if total_time < min_arrival_time[neighbor] and departure_time >= min_departure_time:
                    min_arrival_time[neighbor] = total_time
                    depart_time[current_stop] = departure_time
                    if transfer_time == 1:
                        transfer_stop = current_stop + "-transfer"
                        predecessor[transfer_stop] = (current_stop, minutes_to_hours(current_time), 'transfer', minutes_to_hours(current_time + transfer_time))
                        min_arrival_time[transfer_stop] = current_time + transfer_time
                        predecessor[neighbor] = (transfer_stop, minutes_to_hours(current_time + transfer_time), trip_id, minutes_to_hours(departure_time))
                    else:
                        predecessor[neighbor] = (current_stop, minutes_to_hours(current_time), trip_id, minutes_to_hours(departure_time))

                    heapq.heappush(priority_queue, (total_time, neighbor))

    # Reconstruct the path
    path = []
    node = destination_id
    while node != departure_id:
        if predecessor[node] is None:
            break
        path.append((node, predecessor[node], min_arrival_time[node]))
        node = predecessor[node][0]
    path.reverse()

    # include the start stop and the edge
    if path:
        start_node = path[0][1][0]  
        start_edge = (start_node, predecessor[start_node], min_arrival_time[start_node])
        path.insert(0, start_edge) 

    # compute actual travel time
    actual_departure_time = start_time_mins
    if len(path)>1:
        if (path[1][1] and path[1][1][2] != 'walking'):
            actual_departure_time = time_to_minutes2(path[1][1][3])
    actual_travel_time = min_arrival_time[destination_id] - actual_departure_time
    return path, actual_travel_time, actual_departure_time


def yen_ksp(G, start_time, departure_id, destination_id, K=5):
    paths = []
    path, cost, departure_time = dijkstra(G, start_time, departure_id, destination_id, 0)
    paths.append((path, cost))

    potential_paths = []

    for k in range(1, K):
        for i in range(len(paths[k-1][0]) - 1):
            spur_node = paths[k-1][0][i][0]
            if spur_node.endswith('-transfer'):
                continue
            root_path = paths[k-1][0][:i+1]


            removed_edges = []
            for path in paths:
                if path[0][:i+1] == root_path:
                    node_from = path[0][i][0]
                    node_to = path[0][i+1][0]
                    trip_id_path = path[0][i+1][1][2]

                    if G.has_edge(node_from, node_to):
                        for key, edge_attr in G[node_from][node_to].items():
                            if edge_attr.get('trip_id') == trip_id_path:
                                edge_attr_copy = copy.deepcopy(edge_attr)
                                removed_edges.append((node_from, node_to, key, edge_attr_copy))
                                G.remove_edge(node_from, node_to, key)
                                break

            preds = root_path[-1][1] if root_path[-1][1] is not None else 0
            spur_path, spur_cost, spur_departure_time = dijkstra(G, minutes_to_hours(root_path[-1][-1]), spur_node, destination_id, preds)

            total_path = root_path + spur_path[1:]
            actual_departure_time = time_to_minutes2(start_time)
            total_cost = total_path[-1][2] - departure_time  # Correctly calculate total cost

            if total_path not in [p[0] for p in paths + potential_paths]:
                potential_paths.append((total_path, total_cost))

            for node_from, node_to, key, edge_attr in removed_edges:
                G.add_edge(node_from, node_to, key=key, **edge_attr)

        if not potential_paths:
            break

        potential_paths.sort(key=lambda x: x[1])
        paths.append(potential_paths.pop(0))

    return paths



# +
def count_transfers(path):
    return sum(1 for step in path if step[0].endswith('-transfer'))

def calculate_walking_time(path):
    walking_time = 0
    for node, predecessor, time in path:
        if predecessor is not None and predecessor[2] == 'walking':
            walking_time += time - time_to_minutes2(predecessor[1])
    return int(walking_time)

def print_paths(paths, id_to_stop, stop_info, limit = None):
    sorted_paths = sorted(paths, key=lambda x: (x[1], count_transfers(x[0]), calculate_walking_time(x[0])))
    for i, (path, cost) in enumerate(sorted_paths):
        if limit is not None and i >= limit:
            break
        transfers = count_transfers(path)
        walking_time = calculate_walking_time(path)
        print(f"Path {i + 1}: Transfers: {transfers}, Cost: {int(cost)} minutes, Walking: {walking_time} minutes")
        
        for j, (node, predecessor, time) in enumerate(path):
            if j == 0:
                # For the first stop, use the departure time from the next node's predecessor
                try:
                    next_predecessor = path[j+1][1]
                    departure_time = next_predecessor[-1]
                    print(f"Depart from {id_to_stop[node]}({node}) at {departure_time}: ")
                except IndexError:
                    print(f"Depart from {id_to_stop[node]}({node}) at {minutes_to_hours(time)}: ")
            else:
                try:
                    transport_mode = predecessor[2]
                    if transport_mode not in ['transfer', 'walking']:
                        transport_mode = transport_mode.split('-')[1]
                    print(f"            {id_to_stop[node]}({node}) at {minutes_to_hours(time)} via {transport_mode}")
                except:
                    print(f"                          ({node}) at {minutes_to_hours(time)} via {transport_mode}")
        
        arrival_node, _, arrival_time = path[-1]
        print(f"Arrival at {id_to_stop[arrival_node]}({arrival_node}) at {minutes_to_hours(arrival_time)}")


