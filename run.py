import argparse
import os

import uvicorn

from app.config import load_config


def main() -> None:
    parser = argparse.ArgumentParser(description="Train Watch server")
    parser.add_argument("--config", default="config.yaml", help="Path to YAML config file")
    parser.add_argument("--host", default=None, help="Override listen host")
    parser.add_argument("--port", type=int, default=None, help="Override listen port")
    parser.add_argument("--reload", action="store_true", help="Enable auto reload")
    args = parser.parse_args()

    config = load_config(args.config)
    os.environ["TRAIN_WATCH_CONFIG"] = str(config.config_path)

    uvicorn.run(
        "app.main:build_app",
        factory=True,
        host=args.host or config.server.host,
        port=args.port or config.server.port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
