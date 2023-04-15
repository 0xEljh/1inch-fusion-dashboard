import datetime
import requests
import pandas as pd
import json
import time


class EtherscanAPI:
    def __init__(self, api_key, rate_limit=0.2, max_retries=3):
        self.api_key = api_key
        self.rate_limit = rate_limit
        self.last_request_time = 0
        self.max_retries = max_retries

    def _rate_limit(self):
        time_since_last_request = time.time() - self.last_request_time
        if time_since_last_request < 0.2:
            time.sleep(0.2 - time_since_last_request)
        self.last_request_time = time.time()

    def _make_request(self, url):
        retries = 0
        while retries < self.max_retries:
            try:
                self._rate_limit()
                response = requests.get(url)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException:
                retries += 1
                if retries == self.max_retries:
                    raise

    def get_block_number_by_timestamp(self, timestamp):
        url = f"https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={timestamp}&closest=before&apikey={self.api_key}"
        response_json = self._make_request(url)
        return response_json["result"]

    def get_transactions(self, address, block_number):
        transactions = []
        page = 1
        while True:
            url = f"https://api.etherscan.io/api?module=account&action=txlist&address={address}&startblock={block_number}&endblock=99999999&page={page}&offset=10000&sort=asc&apikey={self.api_key}"
            response_json = self._make_request(url)
            current_transactions = response_json["result"]
            if not current_transactions:
                break
            transactions.extend(current_transactions)
            page += 1
        return transactions

    def get_transaction_receipt(self, transaction_hash):
        url = f"""https://api.etherscan.io/api
           ?module=proxy
           &action=eth_getTransactionReceipt
           &txhash={transaction_hash}
           &apikey={self.api_key}""".replace(
            "\n", ""
        ).replace(
            " ", ""
        )
        response_json = self._make_request(url)
        return response_json["result"]

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
            # datetime=pd.to_datetime(df["timeStamp"], unit="s"),
        )

    @staticmethod
    def process_logs(receipt):
        def unpad_address(address):
            address = address[2:]
            address = address.lstrip("0")
            address = "0x" + address
            return address

        hash_functions = {
            "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822": "swap",
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef": "transfer",
        }

        logs = receipt["logs"]
        logs_df = pd.DataFrame([log for log in logs if len(log["topics"]) == 3])
        logs_df[["topic_hash", "from_address", "to_address"]] = pd.DataFrame(
            logs_df["topics"].tolist(), index=logs_df.index
        )
        logs_df = (
            logs_df.drop(columns=["topics"])
            .assign(
                from_address=lambda x: x["from_address"].map(unpad_address),
                to_address=lambda x: x["to_address"].map(unpad_address),
            )
            # assumption for now that all topic hashes have been covered
            .assign(transaction_type=lambda x: x["topic_hash"].map(hash_functions.get))
            .drop(columns=["topic_hash"])
        )

        return logs_df


def get_timestamp_days_ago(days_ago):
    return round(
        (datetime.datetime.now() - datetime.timedelta(days=days_ago)).timestamp()
    )
