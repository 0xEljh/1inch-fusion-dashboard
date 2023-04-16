# 1inch-fusion-dashboard

This dashboard serves analytics on resolver behavior on 1inch's Fusion swap platform. Built for ETHTokyo 2023.

## Installation

1. Clone the repo
1. Install dependencies:
  - pandas
  - requests
  - `pip install .`
  - jupyter notebook support


## How it works

1. Fetch and process transactions for a specific Ethereum address (the settlement contract)
1. Extract internal ERC20 transfers from transaction logs
1. Fetch historical token prices (and metadata) for ERC20 transfers
1. Calculate profit and true volume for each transaction

## How to run

Run the `dashboard.ipynb` notebook.
Make adjustments to the analysis window by changing the `block_number` variable or editing this line:

    timestamp = get_timestamp_days_ago(1)

Running the notebook will generate an aggregation of the resolvers, their gas fees, profits, and fulfilment volume.

## Future Work

The following low hanging fruit exist:

- Internal transaction counts (how many internal transactions and swaps were made in a single settlement transaction)
- Profit, excluding gas fees
- Volume created on other platforms (e.g. Uniswap)
- Profit measured in native tokens
- Pairs preferred by resolvers
