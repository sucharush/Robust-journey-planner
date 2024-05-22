import networkx as nx
import numpy as np
import random

import random

def test_for_graph(G):
    
    failed_checks = []  

    for node in list(G.nodes()):
        edges = list(G.out_edges(node, data=True))
        for source, target, data in edges:
            # Check zero cycles
            if source == target:
                try:
                    assert data['weight'] != 0, "Detected zero cycle!"
                except AssertionError as e:
                    failed_checks.append((source, target, data, str(e)))
           
            # Check negative weights
            try:
                assert data['weight'] >= 0, "Edge weight is negative or missing."
            except AssertionError as e:
                failed_checks.append((source, target, data, str(e)))

            # Check walking time
            if data.get('trip_id') == "walking":
                try:
                    assert data['weight'] > 0, "Walking time is not positive."
                except AssertionError as e:
                    failed_checks.append((source, target, data, str(e)))

    if failed_checks:
        print("Random sampling test failed for the following node pairs or edges:")
        for failure in failed_checks:
            source, target, data, message = failure
            print(f"From {source} to {target} with data {data}: {message}")
    else:
        print(f"Random sampling test passed for {G.number_of_nodes()} nodes.")


