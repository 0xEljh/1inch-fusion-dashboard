import datetime
import requests
import pandas as pd
import json
import time


class EtherscanAPI:
    def __init__(self, api_key):
        self.api_key = api_key
        self.last_request_time = 0

    def _rate_limit(self):
        time_since_last_request = time.time() - self.last_request_time
        if time_since_last_request < 0.2:
            time.sleep(0.2 - time_since_last_request)
        self.last_request_time = time.time()

    def get_block_number_by_timestamp(self, timestamp):
        self._rate_limit()
        url = f"https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={timestamp}&closest=before&apikey={self.api_key}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()["result"]

    def get_transactions(self, address, block_number):
        self._rate_limit()
        url = f"https://api.etherscan.io/api?module=account&action=txlist&address={address}&startblock={block_number}&endblock=99999999&page=1&offset=10000&sort=asc&apikey={self.api_key}"
        response = requests.get(url)
        return response.json()["result"]

    @staticmethod
    def process_transactions(transactions):
        df = pd.DataFrame(transactions)
        return df.assign(
            blockNumber=pd.to_numeric(df["blockNumber"]),
            timeStamp=pd.to_numeric(df["timeStamp"]),
            gas=pd.to_numeric(df["gas"]),
            gasPrice=pd.to_numeric(df["gasPrice"]),
            gasUsed=pd.to_numeric(df["gasUsed"]),
            cumulativeGasUsed=pd.to_numeric(df["cumulativeGasUsed"]),
            nonce=pd.to_numeric(df["nonce"]),
            transactionIndex=pd.to_numeric(df["transactionIndex"]),
            value=pd.to_numeric(df["value"]),
            isError=df["isError"].astype("category"),
            to=df["to"].astype("category"),
            **{"from": df["from"].astype("category")},
            datetime=pd.to_datetime(df["timeStamp"], unit="s"),
        )


def get_timestamp_days_ago(days_ago):
    return round(
        (datetime.datetime.now() - datetime.timedelta(days=days_ago)).timestamp()
    )
