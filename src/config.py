"""Configuration settings and API endpoints."""
import os
from dotenv import load_dotenv

load_dotenv()

# DataSF API endpoints
DATASF_PERMITS_URL = "https://data.sfgov.org/resource/i98e-djp9.json"
DATASF_TAX_ROLLS_URL = "https://data.sfgov.org/resource/wv5m-vpq2.json"

# Skip Trace API
SKIP_TRACE_API_KEY = os.getenv("SKIP_TRACE_API_KEY", "")
SKIP_TRACE_API_URL = os.getenv("SKIP_TRACE_API_URL", "https://api.batchskiptracing.com/v1")

# DataSF App Token (optional but recommended for higher rate limits)
DATASF_APP_TOKEN = os.getenv("DATASF_APP_TOKEN", "")

# Query settings
DEFAULT_YEARS_LOOKBACK = 15
DEFAULT_BATCH_SIZE = 100
DEFAULT_PAGE_SIZE = 50000

# Rate limiting
DATASF_REQUESTS_PER_HOUR = 1000
