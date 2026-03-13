import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.config import load_config


class ConfigTests(unittest.TestCase):
    def test_empty_shared_token_remains_optional(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "config.yaml"
            config_path.write_text(
                "server:\n  host: 127.0.0.1\n  shared_token: \"\"\n  sqlite_path: data/test.sqlite3\nnodes: []\n",
                encoding="utf-8",
            )

            config = load_config(str(config_path))

        self.assertEqual(config.server.shared_token, "")

    def test_env_token_override_takes_precedence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "config.yaml"
            config_path.write_text(
                "server:\n  host: 127.0.0.1\n  shared_token: file-token\n  sqlite_path: data/test.sqlite3\nnodes: []\n",
                encoding="utf-8",
            )

            with patch.dict(os.environ, {"TRAIN_WATCH_SHARED_TOKEN": "env-token"}, clear=False):
                config = load_config(str(config_path))

        self.assertEqual(config.server.shared_token, "env-token")
