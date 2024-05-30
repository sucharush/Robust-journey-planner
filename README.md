# Robust Journey Planning
**Summary:** This is a robust journey planner for public transport that works across different regions of Switzerland. A user can simply select arrival & departure stops, time of departure, prefered time of arrival, and a minimal level of confidence at which the user wants a journey to succeed. After that, the model will crunch a large amount of data and predict the most optimal routes for a user to choose from.

## Team members

- Siyuan Cheng, siyuan.cheng@epfl.ch
- Ioannis Bantzis, ioannis.bantzis@epfl.ch
- Aidas Venckunas, aidas.venckunas@epfl.ch
- Xinyi Ding, xinyi.ding@epfl.ch
- Jingbang Liu, jingbang.liu@epfl.ch
- Victoria Catherine Arduini, victoria.arduini@epfl.ch

## Quickstart
### Requirements
```shell
git clone git@dslabgit.datascience.ch:group-zz/final-project.git
cd final-project
pip install -r requirements.txt
```

### Bootstraping the data
The data firstly needs to be boostraped. The file that needs to be executed is `./data_generation.ipynb`.

### Running the interactive planner
Open `./visualization.ipynb` and run all code cells. An interface with an input form will appear.

![image](./img/input_form.png)

Select your prefered inputs, click on `Find Routes` and wait until the routes pop up.

![image](./img/paths.png)

Select a path you like the most and enjoy your trip.

## Implementation details

### Datasets


### Methods
<!-- TODO -->


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