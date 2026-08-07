import requests

from etl.ingestion.source import DEFAULT_DATAPLATFORM_ROOT, DictLike
from etl.ingestion.utils import download_and_unzip
from etl.logger import get_logger

logger = get_logger(__name__)


# TODO in caso di IP ban mettere qualcosa di legit qua
USER_AGENT = "Moe Lester plsdontba.nmeagain@gmail.com"


class SecTickers(DictLike):
    """The SEC's CIK/ticker/company-name mapping: a single JSON file
    (`company_tickers.json`) covering every registrant."""

    def __init__(self, **kwargs) -> None:
        super().__init__(name="company_tickers", **kwargs)

    def load(self, **kwargs):
        headers = {"User-Agent": USER_AGENT}  # scegliere una mail da sostituire con ***
        r = requests.get(
            "https://www.sec.gov/files/company_tickers.json", headers=headers
        )
        r.raise_for_status()
        return r.json()


class SecCompanyFacts(DictLike):
    """SEC XBRL "company facts" for every registrant, one JSON file per CIK,
    extracted from the SEC's bulk `companyfacts.zip` download -- a
    collection source, indexed by CIK (e.g. `sec["CIK0000320193"]`)."""

    _URL = "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"

    def __init__(self, **kwargs) -> None:
        super().__init__(name="company_facts", **kwargs)

    def load(self, **kwargs) -> None:
        """Downloads and extracts the zip directly into this source's
        directory -- unlike the generic DictLike case, the per-CIK files
        are written as a side effect of the download itself rather than
        held in memory as one big payload, so `_persist()` is a no-op."""
        download_and_unzip(self._URL, str(self.root_dir), USER_AGENT)

    def _persist(self, data) -> None:
        pass
