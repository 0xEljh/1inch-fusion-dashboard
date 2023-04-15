import json
import requests
import pandas as pd
import re

DEFAULT_HEADERS = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Content-Type": "application/json",
    "Origin": "https://dune.com",
    "Referer": "https://dune.com/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "TE": "trailers",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/111.0",
    "x-hasura-api-key": "",  # Fill in the API key if required
}

FETCH_EXECUTION_ID_PAYLOAD_DETAILS = {
    "operationName": "GetResult",
    "query": "query GetResult($query_id: Int!, $parameters: [Parameter!]!) {  get_result_v3(query_id: $query_id, parameters: $parameters) {    job_id    result_id    error_id    __typename  }}",
}

FETCH_QUERY_EXECUTION_PAYLOAD_DETAILS = {
    "operationName": "GetExecution",
    "query": "query GetExecution($execution_id: String!, $query_id: Int!, $parameters: [Parameter!]!) {\n  get_execution(\n    execution_id: $execution_id\n    query_id: $query_id\n    parameters: $parameters\n  ) {\n    execution_queued {\n      execution_id\n      execution_user_id\n      position\n      execution_type\n      created_at\n      __typename\n    }\n    execution_running {\n      execution_id\n      execution_user_id\n      execution_type\n      started_at\n      created_at\n      __typename\n    }\n    execution_succeeded {\n      execution_id\n      runtime_seconds\n      generated_at\n      columns\n      data\n      __typename\n    }\n    execution_failed {\n      execution_id\n      type\n      message\n      metadata {\n        line\n        column\n        hint\n        __typename\n      }\n      runtime_seconds\n      generated_at\n      __typename\n    }\n    __typename\n  }\n}",
}

DUNE_APP_API_URL = "https://app-api.dune.com/v1/graphql"
DUNE_CORE_API_URL = "https://core-hsr.dune.com/v1/graphql"


class DuneAPI:
    def __init__(
        self,
        query_id: int,
        headers=DEFAULT_HEADERS,
        app_api_url=DUNE_APP_API_URL,
        core_hsr_api_url=DUNE_CORE_API_URL,
    ):
        self.query_id = query_id
        self.headers = headers
        self.app_api_url = app_api_url
        self.core_hsr_api_url = core_hsr_api_url
        self._execution_id = None

    @property
    def execution_id(self):
        if self._execution_id is None:
            self._execution_id = self.fetch_execution_id()
        return self._execution_id

    @classmethod
    def from_url(cls, url: str, **kwargs):
        query_id = cls.get_query_id_from_url(url)
        return cls(query_id=query_id, **kwargs)

    @staticmethod
    def get_query_id_from_url(url: str):
        query_id_pattern = re.compile(r"/queries/(\d+)")
        query_id_match = query_id_pattern.search(url)

        if not query_id_match:
            raise ValueError("Invalid query url")
        return int(query_id_match.group(1))

    def fetch_execution_id(self):
        payload = {
            # ... payload for fetching execution_id ...
            "variables": {"query_id": self.query_id, "parameters": []},
            **FETCH_EXECUTION_ID_PAYLOAD_DETAILS,
        }
        response = requests.post(
            self.core_hsr_api_url, headers=self.headers, data=json.dumps(payload)
        )

        if response.status_code == 200:
            assert (
                "get_result_v3" in response.json()["data"]
            ), f"Failed to get execution_id: {response.json()}"
            return response.json()["data"]["get_result_v3"]["result_id"]
        else:
            raise ValueError(f"Request failed with status code {response.status_code}")

    def query_execution(self):
        payload = {
            # ... payload for querying execution ...
            "variables": {
                "execution_id": self.execution_id,
                "query_id": self.query_id,
                "parameters": [],
            },
            # ... rest of the payload ...
            **FETCH_QUERY_EXECUTION_PAYLOAD_DETAILS,
        }
        response = requests.post(
            self.app_api_url, headers=self.headers, data=json.dumps(payload)
        )

        if response.status_code == 200:
            return response.json()
        else:
            raise ValueError(f"Request failed with status code {response.status_code}")

    def get_dataframe(self):
        response_data = self.query_execution()
        assert response_data["data"]["get_execution"]["execution_succeeded"] is not None
        df = pd.DataFrame(
            response_data["data"]["get_execution"]["execution_succeeded"]["data"]
        )
        return df
