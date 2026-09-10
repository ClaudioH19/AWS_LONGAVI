from flask import Blueprint, send_from_directory

from ..config import FRONTEND_DIST_DIR

bp = Blueprint("frontend", __name__)


@bp.route("/")
def dashboard():
    return send_from_directory(FRONTEND_DIST_DIR, "index.html")


@bp.route("/<path:path>")
def frontend_files(path):
    # send_from_directory usa safe_join y rechaza traversal fuera del dist.
    return send_from_directory(FRONTEND_DIST_DIR, path)
