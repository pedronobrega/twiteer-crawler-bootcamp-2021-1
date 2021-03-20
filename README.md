# Tweeter Crawler and Data Base Ingestion

## Disclaimer

**This crawler was developed at IGTI Data Engineer Bootcamp and only serves an educational purpose**

## Requirements

- SQL Server
- Python
- pip
- [Tweeter Developer](https://developer.twitter.com) account, to get the required credentials

## Setup

- Create a `.env` file and add the following constants:

```
API_KEY=<YOUR-API-KEY>
API_SECRET_KEY=<YOUR-API_SECRET_KEY>
BEARER_TOKEN=<YOUR-BEARER_TOKEN>
ACCESS_TOKEN=<YOUR-ACCESS_TOKEN>
ACCESS_TOKEN_SECRET=<YOUR-ACCESS_TOKEN_SECRET>
DB_USER=<YOUR-DB_USER>
DB_PASSWORD=<YOUR-DB_PASSWORD>
```

- Run `pip install -r requirements.txt`

## Running

```bash
python get_tweets.py
```

Run this until you have a significant amount of data, then run:

```bash
python ingest_tweets.py
```

This will ingest all the valid tweets into a SQL Server table