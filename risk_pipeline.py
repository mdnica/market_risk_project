import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine

# 1.Configuration
tickers = ["AAPL", "GOOGL", "MSFT", "TSLA"]
start_date = "2020-01-01"

# SQLite Database
engine = create_engine("sqlite:///market_data.db")

# 2.Download & clean data
df = yf.download(tickers, start=start_date, auto_adjust=True)

# Flatten MultiIndex columns 
df.columns = [f"{col[1]}_{col[0]}" for col in df.columns]

# Show preview
print("\nClean price data")
print(df.head())

# 3.Calculate daily returns
returns = df.pct_change().dropna()
returns.columns = [f"{col}_ret" for col in returns.columns]

print("\nDaily returns:")
print(returns.head())

# 4.Calculate rolling volatility (30-day)
volatility = returns.rolling(window=30).std() * (252 ** 0.5)
volatility = volatility.dropna()
volatility.columns = [f"{col}_vol" for col in volatility.columns]

print("\nVolatility:")
print(volatility.head())

# 5.Save to SQL 
df.to_sql("prices", engine, if_exists="replace")
returns.to_sql("returns", engine, if_exists="replace")
volatility.to_sql("volatility", engine, if_exists="replace")

print("\nSaved all tables successfully!")