import tempfile
import unittest
from pathlib import Path

from app.config import NodeConfig
from app.ssh_support import build_system_ssh_command, ssh_config_alias_exists, ssh_config_alias_records


class SSHSupportTests(unittest.TestCase):
    def test_alias_detection_reads_exact_hosts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "config"
            config_path.write_text(
                "Host jump-box\n  HostName 1.2.3.4\n\nHost *.wild\n  HostName 5.6.7.8\n",
                encoding="utf-8",
            )
            self.assertTrue(ssh_config_alias_exists("jump-box", config_path))
            self.assertFalse(ssh_config_alias_exists("abc.wild", config_path))
            self.assertFalse(ssh_config_alias_exists("missing", config_path))

    def test_alias_records_include_core_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "config"
            config_path.write_text(
                "Host gpu-lab-a\n  HostName gpu.example.com\n  User ubuntu\n  Port 10800\n  ProxyJump bastion-a\n  IdentityFile ~/.ssh/id_ed25519\n",
                encoding="utf-8",
            )
            items = ssh_config_alias_records(config_path)
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["alias"], "gpu-lab-a")
            self.assertEqual(items[0]["hostname"], "gpu.example.com")
            self.assertEqual(items[0]["user"], "ubuntu")
            self.assertEqual(items[0]["port"], 10800)
            self.assertEqual(items[0]["proxyjump"], "bastion-a")
            self.assertEqual(items[0]["identityfile"], "~/.ssh/id_ed25519")

    def test_build_system_ssh_command_uses_alias_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "config"
            config_path.write_text(
                "Host gpu-lab-a\n  HostName gpu.example.com\n  User ubuntu\n  Port 10800\n",
                encoding="utf-8",
            )
            node = NodeConfig(
                id="node-alias",
                label="Alias Node",
                host="gpu-lab-a",
                port=22,
                user="",
                key_path="",
                password="",
                runs=[],
            )
            command = build_system_ssh_command(node, "hostname", ssh_binary="ssh", config_path=config_path)
            self.assertIn("gpu-lab-a", command)
            self.assertNotIn("-p", command)
            self.assertNotIn("-l", command)

    def test_build_system_ssh_command_for_direct_host_includes_overrides(self) -> None:
        node = NodeConfig(
            id="node-direct",
            label="Direct Node",
            host="gpu.example.com",
            port=10800,
            user="ubuntu",
            key_path="/tmp/id_ed25519",
            password="",
            runs=[],
        )
        command = build_system_ssh_command(node, "hostname", ssh_binary="ssh")
        self.assertIn("-p", command)
        self.assertIn("10800", command)
        self.assertIn("-l", command)
        self.assertIn("ubuntu", command)
        self.assertIn("-i", command)
        self.assertIn("/tmp/id_ed25519", command)


if __name__ == "__main__":
    unittest.main()
