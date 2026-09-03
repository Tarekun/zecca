from etl.ingestion.news import YahooNewsIngester
from etl.llm_processor import LLMProcessor
from etl.logger import get_logger

logger = get_logger(__name__)

# Configuration for the Proof of Concept
TICKERS = ["AAPL", "NVDA", "TSLA"]
DATAPLATFORM_ROOT = "./dataplatform"
LLM_MODEL_NAME = "llama3"

def run_poc():
    logger.info("=== STARTING SENTIMENT ANALYSIS POC ===")
    
    # 1. Ingestion Phase
    logger.info("--- Phase 1: Ingesting News ---")
    ingester = YahooNewsIngester(
        tickers=TICKERS, 
        dataplatform_root=DATAPLATFORM_ROOT
    )
    ingester.ingest()
    
    # 2. Processing Phase
    logger.info("--- Phase 2: LLM Processing ---")
    processor = LLMProcessor(
        dataplatform_root=DATAPLATFORM_ROOT,
        model_name=LLM_MODEL_NAME
    )
    processor.process_unprocessed_articles()
    
    logger.info("=== POC COMPLETED SUCCESSFULLY ===")

if __name__ == "__main__":
    run_poc()