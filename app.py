from pathlib import Path
import panel as pn

from apps.app_PSET_change_log.app_PSET_change_log import PSET_change_log_page

pn.extension()

ROUTES = {
    "PSET_change_log_page": PSET_change_log_page,
}

pn.serve(ROUTES,
         port=5006,
         allow_websocket_origin=["*"],
         show=False,
         admin=False,
         log_level="info",
         num_threads=4,
         ico_path=Path(__file__).parent / "assets" / "img" / "favicon.ico",
         static_dirs={'assets': Path(__file__).parent / "assets"},
         reuse_sessions=True,
         global_loading_spinner=False)
