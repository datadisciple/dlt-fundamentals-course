import dlt

# Sample data containing pokemon details
data = [
    {"id": "1", "name": "bulbasaur", "size": {"weight": 6.9, "height": 0.7}},
    {"id": "4", "name": "charmander", "size": {"weight": 8.5, "height": 0.6}},
    {"id": "25", "name": "pikachu", "size": {"weight": 6, "height": 0.4}},
]

# Set pipeline name, destination, and dataset name
pipeline = dlt.pipeline(
    pipeline_name="quick_start",
    destination="duckdb",
    dataset_name="mydata",
    # if dev_mode=True dlt will add a timestamp to the dataset_name every time the pipeline is run
    dev_mode=True,
)

# Run the pipeline with data and table name
load_info = pipeline.run(data, table_name="pokemon")
print(load_info)