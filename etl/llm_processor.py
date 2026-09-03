import polars as pl
import requests
from pathlib import Path

from etl.logger import get_logger

logger = get_logger(__name__)

class LLMProcessor:
    """
    Reads unprocessed articles from the dataplatform and uses a local LLM 
    (Ollama / llama.cpp) to perform sentiment analysis.
    """

    def __init__(
        self, 
        dataplatform_root: str, 
        model_name: str = "llama3", 
        ollama_url: str = "http://localhost:11434/api/generate"
    ):
        self.articles_dir = Path(dataplatform_root) / "raw" / "articles"
        self.model_name = model_name
        self.ollama_url = ollama_url

    def ask_ollama(self, prompt: str) -> str:
        """
        Sends the prompt to the local Ollama API and returns the response.
        """
        payload = {
            "model": self.model_name,
            "prompt": prompt,
            "stream": False  
        }
        
        try:
            response = requests.post(self.ollama_url, json=payload, timeout=60)
            response.raise_for_status() 
            
            return response.json().get("response", "").strip()
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Error communicating with Ollama: {e}")
            return "LLM_ERROR"

    def process_unprocessed_articles(self):
        """
        Reads Parquet files, filters unprocessed ones, and analyzes them.
        """
        logger.info("Looking for unprocessed articles...")
        
        parquet_pattern = str(self.articles_dir / "*.parquet")
        
        try:
     
            df = (
                pl.scan_parquet(parquet_pattern)
                .filter(pl.col("processed") == False)
                .collect()
            )
        except Exception as e:
            logger.warning(f"No data found or read error: {e}")
            return

        if df.is_empty():
            logger.info("No new articles to process. HODL!")
            return

        logger.info(f"Found {len(df)} articles. Starting sentiment analysis...")

        results = []

        for row in df.iter_rows(named=True):
            ticker = row["ticker"]
            title = row["title"]
            summary = row["summary"]
            
            prompt = f"""
            You are an expert financial analyst. Read the following news regarding the ticker {ticker}:
            
            Title: {title}
            Summary: {summary}
            
            Based on this information, do you think it is a good idea to invest in this company?
            Provide a very brief analysis and you MUST conclude with exactly one of the following: 
            "SENTIMENT: POSITIVE", "SENTIMENT: NEGATIVE", or "SENTIMENT: NEUTRAL".
            """
            
            print(f"\n📰 NEWS: [{ticker}] {title}")
            print("🤖 Analysis in progress (Ollama is thinking)...")
            
            answer = self.ask_ollama(prompt)
            print(f"📊 RESULT:\n{answer}\n")
            print("-" * 60)
            
            results.append({
                "ticker": ticker,
                "link": row["link"],
                "llm_analysis": answer
            })

        #TODO: salavre i risultati in un nuovo file parquet? Per ora stampa
        return results