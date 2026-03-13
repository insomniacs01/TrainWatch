import shutil
import subprocess
import threading
from typing import Dict, Tuple

import paramiko

from .config import NodeConfig
from .ssh_support import build_system_ssh_command


class ParamikoConnectionPool:
    def __init__(self) -> None:
        self._clients: Dict[str, paramiko.SSHClient] = {}
        self._lock = threading.Lock()
        self._ssh_binary = shutil.which("ssh") or ""

    def _cache_key(self, node: NodeConfig) -> str:
        return node.id

    def _connect(self, node: NodeConfig) -> paramiko.SSHClient:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

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

    def _execute_system_ssh(self, node: NodeConfig, command: str, timeout: int) -> Tuple[str, str, int]:
        if not self._ssh_binary:
            raise RuntimeError("ssh binary not found for key/config-based connection")
        ssh_command = build_system_ssh_command(node, command, ssh_binary=self._ssh_binary)
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
        if not node.password:
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
            except Exception as exc:
                last_error = exc
                self.close_node(node)
        raise RuntimeError(str(last_error) if last_error else "SSH execution failed")

    def close_node(self, node: NodeConfig) -> None:
        key = self._cache_key(node)
        with self._lock:
            client = self._clients.pop(key, None)
        if client:
            client.close()

    def close_all(self) -> None:
        with self._lock:
            keys = list(self._clients.keys())
        for key in keys:
            client = self._clients.pop(key, None)
            if client:
                client.close()

