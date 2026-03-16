import hashlib
import logging
import os
import shutil
import subprocess
import tempfile
import threading
from pathlib import Path
from typing import Dict, Optional, Tuple

import paramiko

from .config import NodeConfig, ServerConfig
from .ssh_support import build_system_ssh_command, ssh_config_alias_exists

logger = logging.getLogger(__name__)


class AcceptNewHostKeyPolicy(paramiko.MissingHostKeyPolicy):
    def __init__(self, known_hosts_path: str) -> None:
        self.known_hosts_path = Path(known_hosts_path).expanduser()

    def missing_host_key(self, client: paramiko.SSHClient, hostname: str, key: paramiko.PKey) -> None:
        self.known_hosts_path.parent.mkdir(parents=True, exist_ok=True)
        client.get_host_keys().add(hostname, key.get_name(), key)
        client.save_host_keys(str(self.known_hosts_path))
        logger.info("Accepted new SSH host key for %s and saved it to %s", hostname, self.known_hosts_path)


class ParamikoConnectionPool:
    def __init__(self, server_config: Optional[ServerConfig] = None) -> None:
        effective_server_config = server_config or ServerConfig()
        self._clients: Dict[str, paramiko.SSHClient] = {}
        self._system_ssh_nodes: Dict[str, NodeConfig] = {}
        self._system_ssh_control_paths: Dict[str, Path] = {}
        self._lock = threading.Lock()
        self._ssh_binary = shutil.which("ssh") or ""
        self._host_key_policy = effective_server_config.ssh_host_key_policy
        self._known_hosts_path = Path(effective_server_config.ssh_known_hosts_path).expanduser()
        if os.name == "nt":
            self._control_dir = Path(tempfile.gettempdir()) / "train-watch-ssh"
        else:
            self._control_dir = Path("/tmp/train-watch-ssh")

    def _cache_key(self, node: NodeConfig) -> str:
        return node.id

    def _connect(self, node: NodeConfig) -> paramiko.SSHClient:
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        if self._known_hosts_path.exists():
            client.load_host_keys(str(self._known_hosts_path))
        if self._host_key_policy == "accept-new":
            client.set_missing_host_key_policy(AcceptNewHostKeyPolicy(str(self._known_hosts_path)))
        else:
            client.set_missing_host_key_policy(paramiko.RejectPolicy())

        connect_kwargs = {
            "hostname": node.host,
            "port": node.port,
            "username": node.user,
            "timeout": 15,
            "banner_timeout": 15,
            "auth_timeout": 15,
            "allow_agent": False,
            "look_for_keys": False,
        }
        if node.key_path:
            connect_kwargs["key_filename"] = node.key_path
        if node.password:
            connect_kwargs["password"] = node.password
            if node.key_path:
                connect_kwargs["passphrase"] = node.password

        client.connect(**connect_kwargs)
        return client

    def _get_client(self, node: NodeConfig) -> paramiko.SSHClient:
        key = self._cache_key(node)
        with self._lock:
            client = self._clients.get(key)
            if client and client.get_transport() and client.get_transport().is_active():
                return client
            client = self._connect(node)
            self._clients[key] = client
            return client

    def _system_control_path(self, node: NodeConfig) -> Path:
        key = self._cache_key(node)
        digest = hashlib.sha1(("%s:%s:%s:%s" % (node.id, node.host, node.port, node.user)).encode("utf-8")).hexdigest()[
            :16
        ]
        path = self._control_dir / ("%s.sock" % digest)
        self._control_dir.mkdir(parents=True, exist_ok=True)
        with self._lock:
            self._system_ssh_control_paths[key] = path
            self._system_ssh_nodes[key] = node
        return path

    def _close_system_ssh(self, node: NodeConfig) -> None:
        key = self._cache_key(node)
        with self._lock:
            control_path = self._system_ssh_control_paths.pop(key, None)
            self._system_ssh_nodes.pop(key, None)
        if not self._ssh_binary or control_path is None:
            return
        if control_path.exists():
            exit_command = build_system_ssh_command(
                node,
                "",
                ssh_binary=self._ssh_binary,
                host_key_policy=self._host_key_policy,
                known_hosts_path=self._known_hosts_path,
                control_path=control_path,
            )
            exit_command[-1:-1] = ["-O", "exit"]
            try:
                subprocess.run(
                    exit_command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=10,
                )
            except (subprocess.SubprocessError, OSError):
                pass
        try:
            control_path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            logger.debug("Failed to remove SSH control socket %s", control_path, exc_info=True)

    def _execute_system_ssh(self, node: NodeConfig, command: str, timeout: int) -> Tuple[str, str, int]:
        if not self._ssh_binary:
            raise RuntimeError("ssh binary not found for key/config-based connection")
        control_path = self._system_control_path(node)
        ssh_command = build_system_ssh_command(
            node,
            command,
            ssh_binary=self._ssh_binary,
            host_key_policy=self._host_key_policy,
            known_hosts_path=self._known_hosts_path,
            control_path=control_path,
        )
        try:
            proc = subprocess.run(
                ssh_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("SSH command timed out") from exc
        return proc.stdout, proc.stderr, proc.returncode

    def execute(self, node: NodeConfig, command: str, timeout: int) -> Tuple[str, str, int]:
        host_is_alias = ssh_config_alias_exists(node.host)
        if host_is_alias and node.password:
            raise RuntimeError(
                "Password auth is not supported for local SSH config aliases; "
                "use key-based auth via ~/.ssh/config or enter host/user directly"
            )
        if host_is_alias or not node.password:
            return self._execute_system_ssh(node, command, timeout)

        attempts = 0
        last_error = None
        while attempts < 2:
            attempts += 1
            try:
                client = self._get_client(node)
                _stdin, stdout, stderr = client.exec_command(command, timeout=timeout)
                output = stdout.read().decode("utf-8", "replace")
                error = stderr.read().decode("utf-8", "replace")
                code = stdout.channel.recv_exit_status()
                return output, error, code
            except (paramiko.SSHException, OSError, EOFError) as exc:
                last_error = exc
                self.close_node(node)
        raise RuntimeError(str(last_error) if last_error else "SSH execution failed")

    def close_node(self, node: NodeConfig) -> None:
        key = self._cache_key(node)
        with self._lock:
            client = self._clients.pop(key, None)
        if client:
            client.close()
        self._close_system_ssh(node)

    def close_all(self) -> None:
        with self._lock:
            keys = list(self._clients.keys())
            system_nodes = list(self._system_ssh_nodes.values())
        for key in keys:
            client = self._clients.pop(key, None)
            if client:
                client.close()
        for node in system_nodes:
            self._close_system_ssh(node)
