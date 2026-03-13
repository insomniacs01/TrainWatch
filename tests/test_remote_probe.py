import unittest

from app.remote_probe import render_remote_probe_script


def _remote_probe_namespace():
    script = render_remote_probe_script({"runs": [], "queue_probe_command": ""})
    definitions = script.split("\npid_lookup = build_pid_lookup()", 1)[0]
    namespace = {}
    exec(definitions, namespace, namespace)
    return namespace


class RemoteProbeTests(unittest.TestCase):
    def test_gpu_python_module_is_discoverable(self) -> None:
        namespace = _remote_probe_namespace()
        is_training_candidate = namespace["is_training_candidate"]

        self.assertTrue(is_training_candidate("python -m uav.online_eval.model_runner_navgpt2", has_gpu=True))
        self.assertFalse(is_training_candidate("python -m uav.online_eval.model_runner_navgpt2", has_gpu=False))

    def test_identical_gpu_module_commands_split_by_elapsed_cluster(self) -> None:
        namespace = _remote_probe_namespace()
        discover_runs = namespace["discover_runs"]

        pid_lookup = {
            1001: {
                "elapsed_seconds": 22000,
                "command": "python -m uav.online_eval.model_runner_navgpt2",
                "cwd": "",
                "stdout_path": "",
                "stderr_path": "",
            },
            1002: {
                "elapsed_seconds": 21950,
                "command": "python -m uav.online_eval.model_runner_navgpt2",
                "cwd": "",
                "stdout_path": "",
                "stderr_path": "",
            },
            2001: {
                "elapsed_seconds": 900,
                "command": "python -m uav.online_eval.model_runner_navgpt2",
                "cwd": "",
                "stdout_path": "",
                "stderr_path": "",
            },
            2002: {
                "elapsed_seconds": 860,
                "command": "python -m uav.online_eval.model_runner_navgpt2",
                "cwd": "",
                "stdout_path": "",
                "stderr_path": "",
            },
        }
        gpu_processes = [
            {"pid": 1001, "gpu_uuid": "GPU-0", "used_gpu_memory_mb": 4096.0},
            {"pid": 1002, "gpu_uuid": "GPU-3", "used_gpu_memory_mb": 4096.0},
            {"pid": 2001, "gpu_uuid": "GPU-4", "used_gpu_memory_mb": 4096.0},
            {"pid": 2002, "gpu_uuid": "GPU-6", "used_gpu_memory_mb": 4096.0},
        ]

        results = discover_runs(pid_lookup, gpu_processes)

        self.assertEqual(len(results), 2)
        pid_sets = {tuple(item["pid"] for item in result["matched_processes"]) for result in results}
        self.assertEqual(pid_sets, {(1001, 1002), (2001, 2002)})
        self.assertTrue(all(result["label"] == "python uav.online_eval.model_runner_navgpt2" for result in results))


if __name__ == "__main__":
    unittest.main()
