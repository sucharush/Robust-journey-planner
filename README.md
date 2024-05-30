# Robust Journey Planning
**Summary:** This is a robust journey planner for public transport that works across different regions of Switzerland. A user can simply select arrival & departure stops, time of departure, preferred time of arrival, and a minimal level of confidence at which the user wants a journey to succeed. After that, the model will crunch a large amount of data and predict the most optimal routes for a user to choose from.

## Team members

- Siyuan Cheng, siyuan.cheng@epfl.ch
- Ioannis Bantzis, ioannis.bantzis@epfl.ch
- Aidas Venckunas, aidas.venckunas@epfl.ch
- Xinyi Ding, xinyi.ding@epfl.ch
- Jingbang Liu, jingbang.liu@epfl.ch
- Victoria Catherine Arduini, victoria.arduini@epfl.ch

## Quickstart
```shell
git clone git@dslabgit.datascience.ch:group-zz/final-project.git
cd final-project
pip install -r requirements.txt
```
## Project Organization

```shell
.
├── data
│   ├── sbb_real_stop_times.parquet
│   ├── sbb_timetable_stop_times.parquet
│   ├── stops.csv
│   └── stop_to_stop.csv
├── img
│   ├── google_censuy.png
│   ├── google_epfl.png
│   └── google_gare.png
├── sanity_test
│   ├── graph_test.py
│   └── path_test.py
├── src
│   ├── delay_model.py
│   ├── route_planning.py
│   ├── transport_network.py
│   └── util.py
├── README.md
├── requirements.txt
├── data_generation.py
├── validation-delay.ipynb
├── validation.ipynb
└── vizsualization.ipynb

```

## How-to
### Generating the Dataset
Settle the dataset by executing the file `./data_generation.ipynb`. Select area by changing the `object_id` at the top of the notebook
```shell
python data_generation.py
```
**Note:** The current `./data` folder contains the dataset generated by setting `object_id = 1` (Lausanne area), please wait until the file finishes running to refresh the dataset whenever you change this parameter.

### Running the Interactive Planner
1. Open `./visualization.ipynb` and run all code cells. An interface with an input form will appear.

**Note:** For the stops and times, please type from the keyboard and select the suggested option, as using the dropdown alone may not display fully.

<img src="./img/input_form.png" alt="input form" width="400">


2. Select your preferred inputs, click on `Find Routes` and wait until the routes pop up.

**Note:** The display on the map might be corrupted due to incomplete stop information in the dataset. Please refer to the printed path details for complete information.

<img src="./img/paths.png" alt="input form" width="500">

Select a path you like the most and enjoy your trip.

<!-- ## Implementation details

### Datasets


### Methods
<!-- TODO --> 

