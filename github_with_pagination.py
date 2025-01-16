import os

import dlt
from dlt.sources.helpers import requests
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth

@dlt.source
def github_source(access_token=os.environ["GITHUB_ACCESS_TOKEN"]): # <--- set the secret variable "access_token" here:
    client = RESTClient(
            base_url="https://api.github.com",
            auth=BearerTokenAuth(token=access_token)
    )

    @dlt.resource
    def github_events():
        for page in client.paginate("orgs/dlt-hub/events"):
            yield page


    @dlt.resource
    def github_stargazers():
        for page in client.paginate("repos/dlt-hub/dlt/stargazers"):
            yield page

    return github_events, github_stargazers

# define new dlt pipeline
# without a pipeline name specified, the pipeline will be called "dlt_<filename>"
# without a dataset name specified, the dataset will be called "<pipeline>_dataset"
pipeline = dlt.pipeline(destination="duckdb")

# run the pipeline with the new resource
load_info = pipeline.run(github_source())
print(load_info)