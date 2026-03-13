import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import NodeConfig


DEFAULT_SSH_CONFIG_PATH = Path("~/.ssh/config").expanduser()


def _is_exact_alias(pattern: str) -> bool:
    return pattern and not any(token in pattern for token in ("*", "?", "!"))


def ssh_config_alias_records(config_path: Optional[Path] = None) -> List[Dict[str, Any]]:
    path = Path(config_path or DEFAULT_SSH_CONFIG_PATH).expanduser()
    if not path.exists():
        return []

    records: List[Dict[str, Any]] = []
    current_aliases: List[str] = []
    current_options: Dict[str, str] = {}

    def flush() -> None:
        if not current_aliases:
            return
        for alias in current_aliases:
            records.append(
                {
                    "alias": alias,
                    "hostname": current_options.get("hostname", ""),
                    "user": current_options.get("user", ""),
                    "port": int(current_options.get("port", "22") or 22),
                    "proxyjump": current_options.get("proxyjump", ""),
                    "identityfile": current_options.get("identityfile", ""),
                }
            )

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        lower = line.lower()
        if lower.startswith("host "):
            flush()
            patterns = line[5:].split()
            current_aliases = [pattern for pattern in patterns if _is_exact_alias(pattern)]
            current_options = {}
            continue
        if not current_aliases:
            continue
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        key, value = parts[0].lower(), parts[1].strip()
        if key in {"hostname", "user", "port", "proxyjump", "identityfile"} and key not in current_options:
            current_options[key] = value

    flush()
    records.sort(key=lambda item: item["alias"].lower())
    return records


def ssh_config_aliases(config_path: Optional[Path] = None) -> List[str]:
    return [item["alias"] for item in ssh_config_alias_records(config_path)]


def ssh_config_alias_exists(host: str, config_path: Optional[Path] = None) -> bool:
    candidate = (host or "").strip()
    if not candidate:
        return False
    return candidate in set(ssh_config_aliases(config_path))


def build_system_ssh_command(
    node: NodeConfig,
    remote_command: str,
    ssh_binary: Optional[str] = None,
    config_path: Optional[Path] = None,
    host_key_policy: str = "accept-new",
    known_hosts_path: Optional[Path] = None,
    control_path: Optional[Path] = None,
    control_persist_seconds: int = 300,
) -> List[str]:
    binary = ssh_binary or shutil.which("ssh") or "ssh"
    path = Path(config_path or DEFAULT_SSH_CONFIG_PATH).expanduser()
    host_is_alias = ssh_config_alias_exists(node.host, path)
    strict_host_key_checking = "accept-new" if host_key_policy == "accept-new" else "yes"

    command = [
        binary,
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=15",
        "-o",
        f"StrictHostKeyChecking={strict_host_key_checking}",
        "-T",
    ]
    if known_hosts_path:
        command.extend(["-o", f"UserKnownHostsFile={Path(known_hosts_path).expanduser()}"])
    if control_path:
        command.extend(
            [
                "-o",
                "ControlMaster=auto",
                "-o",
                f"ControlPersist={max(1, int(control_persist_seconds or 300))}",
                "-o",
                f"ControlPath={Path(control_path).expanduser()}",
            ]
        )
    if path.exists():
        command.extend(["-F", str(path)])
    if node.key_path:
        command.extend(["-i", node.key_path])
    if node.user:
        command.extend(["-l", node.user])
    if (not host_is_alias) or int(node.port or 22) != 22:
        command.extend(["-p", str(max(1, int(node.port or 22)))])
    command.append(node.host)
    if remote_command:
        command.append(remote_command)
    return command
