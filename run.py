import argparse
import logging
import os

import uvicorn
from app.config import load_config
from app.logging_utils import configure_logging

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Train Watch server")
    parser.add_argument("--config", default="config.empty.yaml", help="Path to YAML config file")
    parser.add_argument("--host", default=None, help="Override listen host")
    parser.add_argument("--port", type=int, default=None, help="Override listen port")
    parser.add_argument("--reload", action="store_true", help="Enable auto reload")
    args = parser.parse_args()

    config = load_config(args.config)
    configure_logging(config.server.log_level)
    os.environ["TRAIN_WATCH_CONFIG"] = str(config.config_path)
    if config.server.shared_token:
        os.environ["TRAIN_WATCH_SHARED_TOKEN"] = config.server.shared_token
    logger.info(
        "Starting Train Watch on %s:%s with log level %s",
        args.host or config.server.host,
        args.port or config.server.port,
        config.server.log_level,
    )

    uvicorn.run(
        "app.main:build_app",
        factory=True,
        host=args.host or config.server.host,
        port=args.port or config.server.port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
