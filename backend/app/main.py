import logging
from datetime import datetime

from flask import Flask

from .config import DB_PATH, LOG_FILE, PORT
from .repositories.weather_readings import init_db
from .realtime import init_realtime
from .routes.frontend import bp as frontend_bp
from .routes.weather import bp as weather_bp


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
    )


configure_logging()

app = Flask(__name__)
init_realtime(app)
app.register_blueprint(weather_bp)
app.register_blueprint(frontend_bp)

init_db()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.info("=" * 55)
    logger.info("Port: %s  DB: %s", PORT, DB_PATH)
    logger.info("Dashboard  -> GET  /")
    logger.info("Datalogger -> POST /weather")
    logger.info("API        -> GET  /weather/latest | /range | /count | /devices")
    logger.info("Download   -> GET  /weather/export/csv | /export/json")
    logger.info("Health     -> GET  /health")
    logger.info("Started at: %s", datetime.utcnow().isoformat())
    logger.info("=" * 55)
    app.run(host="0.0.0.0", port=PORT, debug=False)
