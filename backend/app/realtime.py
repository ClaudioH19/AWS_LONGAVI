from flask_socketio import SocketIO

from .config import DRAGONFLY_URL, SOCKETIO_CORS_ORIGINS

socketio = SocketIO()


def init_realtime(app):
    socketio.init_app(
        app,
        async_mode="threading",
        cors_allowed_origins=SOCKETIO_CORS_ORIGINS,
        message_queue=DRAGONFLY_URL or None,
        channel="weather-live",
    )
