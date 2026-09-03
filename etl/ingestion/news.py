import feedparser
import polars as pl
from datetime import datetime

from etl.ingestion.source import TableLike
from etl.logger import get_logger

logger= get_logger(__name__)

class YahooNewsIngester(TableLike):
    """
    Ingests news articles from Yahoo Finance RSS feeds for a given ticker symbol.
    """

    def __init__(self, tickers: list[str], dataplatform_root: str, **kwargs):
        self.tickers = tickers

        super().__init__(
            name="articles",
            key_columns=["link"],
            dataplatform_root=dataplatform_root,
            **kwargs
        )
    
    def load(self, **kwargs) -> pl.DataFrame:

        extracted_data = []

        for ticker in self.tickers:
            logger.info(f"Fetching news for ticker: {ticker}")

            feed_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}"
            feed = feedparser.parse(feed_url)
  
            for entry in feed.entries:

                news = {
                    "ticker": ticker,
                    "title": entry.get("title", ""),
                    "summary": entry.get("summary", ""),
                    "link": entry.get("link", ""),
                    "published_at": entry.get("published", ""),
                    "ingested_at": datetime.now().isoformat(),
                    "processed": False  
                    }
                extracted_data.append(news)

        if not extracted_data:
            logger.warning(f"No news articles found for ticker: {self.tickers}")
            return pl.DataFrame()
    
        df = pl.DataFrame(extracted_data)

        return df