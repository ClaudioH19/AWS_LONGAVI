from datetime import datetime

from ..config import CHILE_TZ


def get_chile_now_text():
    return datetime.now(CHILE_TZ).strftime("%Y-%m-%d %H:%M:%S")
