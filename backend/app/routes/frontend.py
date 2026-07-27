import os

from flask import Blueprint, send_from_directory

from ..config import FRONTEND_DIST_DIR

bp = Blueprint("frontend", __name__)


@bp.route("/")
def dashboard():
    return send_from_directory(FRONTEND_DIST_DIR, "index.html")


@bp.route("/<path:path>")
def frontend_files(path):
    file_path = os.path.join(FRONTEND_DIST_DIR, path)
    if os.path.isfile(file_path):
        return send_from_directory(FRONTEND_DIST_DIR, path)
    return send_from_directory(FRONTEND_DIST_DIR, "index.html")
