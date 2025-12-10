from pathlib import Path

_NAV_CSS_FILE = Path(__file__).parent / "nav.css"
_NAV_CSS = _NAV_CSS_FILE.read_text()

_BK_CSS_FILE = Path(__file__).parent / "bk.css"
_BK_CSS = _BK_CSS_FILE.read_text()

_RAW_CSS_FILE = Path(__file__).parent / "raw.css"
_RAW_CSS = _RAW_CSS_FILE.read_text()
