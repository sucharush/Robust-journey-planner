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

    # a = 0

    while priority_queue:
        # a+=1
        # print(a)
        current_time, current_stop = heapq.heappop(priority_queue)
                
        if current_stop == destination_id:
            break

        for neighbor in G[current_stop]:
            for key, edge_attr in G[current_stop][neighbor].items():
                trip_id = edge_attr['trip_id']
                # transfer time at the same stop
                transfer_time = 2 if trip_id != 'walking' and predecessor[current_stop] and predecessor[current_stop][2] != trip_id else 0
                departure_time = edge_attr['departure_time_mins'] if trip_id != 'walking' else current_time
                travel_time = edge_attr['arrival_time_mins'] - departure_time if trip_id != 'walking' else edge_attr['walking_time']
                total_time = current_time + transfer_time + max(0, departure_time - current_time - transfer_time) + travel_time # current time + wait time + travel time + transfer time

                if total_time < min_arrival_time[neighbor] and departure_time >= current_time+transfer_time:
                    min_arrival_time[neighbor] = total_time
                    depart_time[current_stop] = departure_time
                    if transfer_time == 2:
                        transfer_stop = current_stop + "-transfer"
                        predecessor[transfer_stop] = (current_stop, minutes_to_hours(current_time), 'transfer')
                        min_arrival_time[transfer_stop] = current_time + transfer_time
                        predecessor[neighbor] = (transfer_stop, minutes_to_hours(current_time + transfer_time), trip_id)
                    else:
                        predecessor[neighbor] = (current_stop, minutes_to_hours(current_time), trip_id)

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
        start_edge = (start_node, predecessor[start_node], min_arrival_time[node])
        path.insert(0, start_edge) 

    return path, min_arrival_time[destination_id]


def yen_ksp(G, start_time, departure_id, destination_id, K=5):
    paths = []
    path, cost = dijkstra(G, start_time, departure_id, destination_id, 0)
    paths.append((path, cost-time_to_minutes2(start_time)))

    potential_paths = []

    for k in range(1, K):

        for i in range(len(paths[k-1][0]) - 1):
            spur_node = paths[k-1][0][i][0]
            if spur_node.endswith('-transfer'):
                continue
            root_path = paths[k-1][0][:i+1]

            removed_edges = []
            for path in paths: 
                if path[0][:i+1] == root_path :
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
            # preds = 0
            spur_path, spur_cost = dijkstra(G, minutes_to_hours(root_path[-1][-1]), spur_node, destination_id, preds)                

            total_path = root_path + spur_path[1:]
            total_cost = total_path[-1][-1] - total_path[0][-1]

            if total_path not in [p[0] for p in paths + potential_paths]:
                potential_paths.append((total_path, total_cost))

            for node_from, node_to, key, edge_attr in removed_edges:
                G.add_edge(node_from, node_to, key=key, **edge_attr)

        if not potential_paths:
            break

        potential_paths.sort(key=lambda x: x[1])
        paths.append(potential_paths.pop(0))

    return paths

def print_paths(paths, id_to_stop):
    for index, (path, cost) in enumerate(paths):
        print(f"Path {index + 1}: Total Cost: {cost} minutes")
        for node, predecessor, time in path:
            if predecessor is None:
                print(f"Depart from {id_to_stop[node]}({node}) at {minutes_to_hours(time)}: ")
            else:
                try:
                    print(f"            {id_to_stop[node]}({node}) at {minutes_to_hours(time)} via {predecessor[2]}")
                except:
                    print(f"                          ({node}) at {minutes_to_hours(time)} via {predecessor[2]}")
        print()