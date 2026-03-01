import logging
from datetime import datetime
import os

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Create a timestamp string (YearMonthDay_HourMinuteSecond)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
# Incorporate the timestamp into the filename
LOG_FILE = os.path.join(LOG_DIR, f"etl_{timestamp}.log")

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("ETL")
logger.setLevel(logging.INFO)
