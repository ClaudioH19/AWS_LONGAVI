from backend.app.main import app
from backend.app.repositories.weather_readings import (
    build_query,
    get_conn,
    get_db_status,
    init_db,
    save_reading,
)
from backend.app.services.readings import flatten_rows, prepare_reading
from backend.app.services.time import get_chile_now_text
from backend.app.services.weather_normalizer import normalize_payload

__all__ = [
    "app",
    "build_query",
    "flatten_rows",
    "get_chile_now_text",
    "get_conn",
    "get_db_status",
    "init_db",
    "normalize_payload",
    "prepare_reading",
    "save_reading",
]


if __name__ == "__main__":
    from backend.app.config import PORT

    app.run(host="0.0.0.0", port=PORT, debug=False)
