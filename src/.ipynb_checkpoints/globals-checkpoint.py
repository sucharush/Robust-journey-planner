import pandas as pd
import numpy as np
import copy
from src.util import *
from src.algorithm import *
from src.transport_network import *

sbb_network = TransportNetwork('./data/sbb_timetable_stop_times.parquet', './data/stop_to_stop.csv', './data/stops.csv')
stops = sbb_network.stops
stop_to_stop = sbb_network.stop_to_stop
id_to_stop = stops.set_index('stop_id')['stop_name'].to_dict()
stop_to_id = stops.set_index('stop_name')['stop_id'].to_dict()


