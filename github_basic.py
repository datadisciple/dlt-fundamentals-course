import dlt
from dlt.sources.helpers import requests

# Example resource
@dlt.resource(table_name='events')
def github_events():
    url = f"https://api.github.com/orgs/dlt-hub/events"
    response = requests.get(url)
    yield response.json()

# Defines additional github_repos resource
@dlt.resource(table_name='repos')
def github_repos():
    url = f"https://api.github.com/orgs/dlt-hub/repos"
    response = requests.get(url)
    yield response.json()

@dlt.transformer(data_from=github_repos, table_name='stargazers')
def github_stargazers(items):
    for item in items:

        print(f"Item: {item}\n") # <-- print the data we get from the `github_repos` resource

        owner = item["owner"]["login"]
        repo = item["name"]
        url = f"https://api.github.com/repos/{owner}/{repo}/stargazers"
        response = requests.get(url)
        details = response.json()

        print(f"Details: {details}\n") # <--- print the data we get back from the stargazers endpoint

        yield details

# Combines resources under a single "github" source
@dlt.source
def github():
    return github_events, github_repos, github_stargazers

# Set pipeline name, destination, and dataset name
pipeline = dlt.pipeline(
    pipeline_name="github_pipeline",
    destination="duckdb",
    dataset_name="github_data"
)

# Call the source in the pipeline.run() statement to load all its resources
load_info = pipeline.run(github())
print(load_info)