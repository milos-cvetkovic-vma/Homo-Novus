# Homo Novus Trading Bot

## Project Overview

**Homo Novus** is an automated trading bot designed to predict future price movements by analyzing historical data from stock markets, cryptocurrencies, and fiat currencies. The bot uses statistical methods and data analytics to develop and test trading strategies in a simulated environment before applying them in real-world trading scenarios on the Bitmex platform.

## Goal

The primary aim of the **Homo Novus** project is to effectively leverage historical market data to forecast price trends and execute trades that capitalize on these predictions, aiming to achieve substantial financial returns.

## Repository Files

### `percentages.csv`

This CSV file contains critical parameter values used by the trading algorithm implemented in `main.py`. It includes multiple columns like `g1`, `g2`, `g3`, `r1`, `r2`, and `r3`, each representing different trading parameters that adjust based on the hour of the day.

### `main.py`

This script contains the core trading algorithm for the **Homo Novus** bot. It handles:

- Dynamic order management based on real-time market data.
- Connections to the Bitmex trading platform using custom WebSocket integrations.
- Detailed error handling and data logging to ensure robust trading operations.
- SQLite database interactions for storing and managing trading states and orders.

### `bitmex_websocket_custom.py`

Manages the WebSocket connection to Bitmex, providing the bot with real-time market data, which is crucial for immediate response to market conditions. Features include:

- Streaming real-time data like quotes, trades, and order books.
- Sending, updating, and canceling orders based on algorithmic trading decisions.
- Handling authentication and connection stability to maintain a persistent and secure link to Bitmex.

### `subscriptions.py`

Defines subscription categories for the WebSocket connection, detailing which data streams are essential for the bot’s operation. It specifies:

- **NO_SYMBOL_SUBS**: General subscriptions that don't depend on a specific trading symbol.
- **DEFAULT_SUBS**: Essential market data for default operations, including execution data and order book details.
- **MY_SUBS**: Customized data streams primarily focused on execution details critical for the trading strategy.

## Setup and Operation

To run the **Homo Novus** trading bot, ensure that all dependencies are installed and that you have valid credentials for the Bitmex API. Start the bot by running the `main.py` file, which will initiate the trading operations based on predefined parameters and real-time market analysis.

