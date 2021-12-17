# web3

### arbitrage strategy

This is a prototype of a simple arbitrage strategy, where we look for price distortions for a given BASE/QUOTE pair in different exchanges.

Curently using Binance, FTX and Kucoin REST APIs.

Order handling and Quantity criteria still to be implemented.

Executing among different exchanges will require efficient:
- processing (minimize latency) and;
- capital allocation (guarantee returns).


### Uniswap Pools 

Querying Uniswap data from GraphQL and Uniswap V3 Subgraph https://thegraph.com/hosted-service/subgraph/uniswap/uniswap-v3
