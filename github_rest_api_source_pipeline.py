import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_source

config: RESTAPIConfig = {
    "client": {
        "base_url": "https://api.github.com",
        "auth": {
            "token": dlt.secrets["sources.access_token"], # <--- we already configured access_token above
        },
        "paginator": "header_link" # <---- set up paginator type
    },
    "resources": [  # <--- list resources
        {
            "name": "issues",
            "endpoint": {
                "path": "repos/dlt-hub/dlt/issues",
                "params": {
                    "state": "open",
                },
            },
        },
        {
            "name": "contributors",
            "endpoint": {
                "path": "repos/dlt-hub/dlt/contributors",
            }
        },
        {
            "name": "issue_comments", # <-- here we declare dlt.transformer
            "endpoint": {
                "path": "repos/dlt-hub/dlt/issues/{issue_number}/comments",
                "params": {
                    "issue_number": {
                        "type": "resolve", # <--- use type 'resolve' to resolve {issue_number} for transformer
                        "resource": "issues",
                        "field": "number",
                    },

                },
            },
        },
        {
            "name": "repos",
            "endpoint": {
                "path": "orgs/dlt-hub/repos",
            },
        },
        {
            "name": "pr_comments",
            "endpoint": {
                "path": "/repos/{owner}/{repo}/pulls/comments",
                "params": {
                    "owner": {
                        "type": "resolve",
                        "resource": "repos",
                        "field": "owner['login']",
                    },
                    "repo": {
                        "type": "resolve",
                        "resource": "repos",
                        "field": "name",
                    },
                    # using since parameter to incrementally load pr_comments
                    "since": {
                        "type": "incremental",
                        "cursor_path": "updated_at",
                        "initial_value": "2024-12-01"
                    },
                },
            },
        },
    ],
}

github_source = rest_api_source(config)


pipeline = dlt.pipeline(
    pipeline_name="rest_api_github",
    destination="duckdb",
    dataset_name="rest_api_data",
    progress="log",
    dev_mode=True,
)

load_info = pipeline.run(github_source)
print(load_info)