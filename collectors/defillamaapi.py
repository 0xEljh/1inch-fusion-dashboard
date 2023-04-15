import requests


class DefillamaAPI:
    def __init__(self, rate_limit=1):
        self.headers = {"accept": "application/json"}

    def get_historical_token_price(self, token_address, timestamp):
        url = f"https://coins.llama.fi/prices/historical/{timestamp}/ethereum:{token_address}"
        response = requests.get(url, headers=self.headers)
        return response.json()["coins"][f"ethereum:{token_address}"]


if __name__ == "__main__":
    api = DefillamaAPI()
    print(
        api.get_historical_token_price(
            "0xb26631c6dda06ad89b93c71400d25692de89c068", 1681508555
        )
    )
