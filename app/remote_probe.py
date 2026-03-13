import base64
import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

from .config import NodeConfig


BASE_DIR = Path(__file__).resolve().parent
REMOTE_PROBE_TEMPLATE_PATH = BASE_DIR / "assets" / "remote_probe.py.tmpl"
REMOTE_COMMAND_TEMPLATE = """PYTHON_BIN=$(command -v python3 || command -v python)
if [ -z "$PYTHON_BIN" ]; then
  echo "python not found" >&2
  exit 127
fi
"$PYTHON_BIN" - <<'PY'
%s
PY"""


def build_remote_probe_payload(node: NodeConfig) -> Dict[str, Any]:
    return {
        "runs": [
            {
                "id": run.id,
                "label": run.label,
                "log_path": run.log_path,
                "log_glob": run.log_glob,
                "process_match": run.process_match,
            }
            for run in node.runs
        ],
        "queue_probe_command": node.queue_probe_command,
    }


@lru_cache(maxsize=1)
def _remote_probe_template() -> str:
    return REMOTE_PROBE_TEMPLATE_PATH.read_text(encoding="utf-8")


def render_remote_probe_script(payload: Dict[str, Any]) -> str:
    encoded = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
    return _remote_probe_template().replace("__PAYLOAD__", encoded)


def build_remote_probe_command(node: NodeConfig) -> str:
    return REMOTE_COMMAND_TEMPLATE % render_remote_probe_script(build_remote_probe_payload(node))
