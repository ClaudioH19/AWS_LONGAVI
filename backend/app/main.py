import logging
import os
import sys
import uuid
from datetime import datetime
from logging.handlers import RotatingFileHandler

from flask import Flask, g, jsonify, request, send_from_directory
from werkzeug.middleware.proxy_fix import ProxyFix

from .config import (
    DB_PATH,
    FRONTEND_DIST_DIR,
    LOG_FILE,
    LOG_LEVEL,
    MAX_CONTENT_LENGTH,
    PORT,
    TRUST_PROXY_COUNT,
)
from .repositories.weather_readings import init_db
from .realtime import init_realtime
from .routes.frontend import bp as frontend_bp
from .routes.weather import bp as weather_bp


def configure_logging():
    level = getattr(logging, LOG_LEVEL, logging.INFO)
    handlers = [logging.StreamHandler(sys.stdout)]
    if LOG_FILE:
        handlers.append(RotatingFileHandler(LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=3))
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers,
        force=True,
    )


def create_app():
    configure_logging()
    app = Flask(__name__, static_folder=None)
    app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH

    if TRUST_PROXY_COUNT:
        app.wsgi_app = ProxyFix(
            app.wsgi_app,
            x_for=TRUST_PROXY_COUNT,
            x_proto=TRUST_PROXY_COUNT,
            x_host=TRUST_PROXY_COUNT,
        )

    @app.before_request
    def assign_request_id():
        incoming = request.headers.get("X-Request-ID", "").strip()
        g.request_id = incoming[:128] if incoming else uuid.uuid4().hex

    @app.after_request
    def apply_response_headers(response):
        response.headers["X-Request-ID"] = getattr(g, "request_id", uuid.uuid4().hex)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "same-origin"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; base-uri 'self'; object-src 'none'; frame-ancestors 'none'; "
            "img-src 'self' data:; style-src 'self'; script-src 'self'; "
            "connect-src 'self' ws: wss:"
        )
        if request.path.startswith(("/health", "/status")):
            response.headers["Cache-Control"] = "no-store"
        return response

    @app.errorhandler(404)
    def not_found(_error):
        api_prefixes = ("/weather", "/health", "/status", "/internal")
        if request.path.startswith(api_prefixes):
            return jsonify({"error": "not_found", "message": "La ruta solicitada no existe."}), 404
        not_found_path = os.path.join(FRONTEND_DIST_DIR, "404.html")
        if os.path.isfile(not_found_path):
            response = send_from_directory(FRONTEND_DIST_DIR, "404.html")
            response.status_code = 404
            return response
        return "Página no encontrada", 404

    @app.errorhandler(405)
    def method_not_allowed(_error):
        return jsonify({"error": "method_not_allowed", "message": "Método HTTP no permitido."}), 405

    @app.errorhandler(413)
    def payload_too_large(_error):
        return jsonify({"error": "payload_too_large", "message": "El cuerpo supera el tamaño permitido."}), 413

    @app.errorhandler(500)
    def internal_error(error):
        logging.getLogger(__name__).error("Unhandled error request_id=%s: %s", g.request_id, error)
        return jsonify({"error": "internal_error", "message": "Ocurrió un error interno."}), 500

    init_realtime(app)
    app.register_blueprint(weather_bp)
    app.register_blueprint(frontend_bp)
    init_db()
    return app


app = create_app()


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
