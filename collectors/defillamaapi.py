import requests


class DefillamaAPI:
    def __init__(self, rate_limit=1):
        # TODO: handle rate limit
        self.headers = {"accept": "application/json"}

    def get_historical_token_price(self, token_address, timestamp):
        url = f"https://coins.llama.fi/prices/historical/{timestamp}/ethereum:{token_address}"
        response = requests.get(url, headers=self.headers)
        return response.json()["coins"][f"ethereum:{token_address}"]

    def get_historical_token_prices(self, token_addresses, timestamp):
        # token_addresses is a list of token addresses
        # convert to comma separated string
        token_addresses = ",".join(
            [f"ethereum:{token_address}" for token_address in token_addresses]
        )
        url = f"https://coins.llama.fi/prices/historical/{timestamp}/{token_addresses}"
        response = requests.get(url, headers=self.headers)
        # rename the dictionary keys to remove the ethereum: prefix
        return {key[9:]: value for key, value in response.json()["coins"].items()}


if __name__ == "__main__":
    api = DefillamaAPI()
    print(
        api.get_historical_token_price(
            "0xb26631c6dda06ad89b93c71400d25692de89c068", 1681508555
        )
    )
