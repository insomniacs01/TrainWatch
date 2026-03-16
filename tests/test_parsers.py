import unittest

from app.parsers import parse_training_output

MAPANYTHING_LOG = (
    "Epoch: [4]  [123/500]  eta: 1:22:33  lr: 0.000050  loss: 0.4821 (0.6231) "
    "grad_norm: 1.2300  time: 1.2312  data: 0.0111  max mem: 42321\n"
    "Test Epoch: [4]  [203/204]  loss: 0.3910 (0.4011)  scale_err_mean: 0.0211\n"
)


DEEPSPEED_LOG = (
    "[2026-03-11 10:00:00,000] [INFO] [engine.py:123:train_batch] step=812 loss=1.9321 "
    "lr=1.20e-05 grad_norm=0.83 tokens/s=4321.8 samples/s=12.7 eta=0:12:10\n"
)


class ParserTests(unittest.TestCase):
    def test_mapanything_parser_extracts_key_metrics(self) -> None:
        parsed = parse_training_output(
            "mapanything",
            MAPANYTHING_LOG,
            r"Training complete",
            r"Traceback",
        )
        self.assertEqual(parsed.parser, "mapanything")
        self.assertEqual(parsed.epoch, 4)
        self.assertEqual(parsed.step, 123)
        self.assertEqual(parsed.step_total, 500)
        self.assertAlmostEqual(parsed.loss, 0.4821, places=4)
        self.assertAlmostEqual(parsed.eval_loss, 0.3910, places=4)
        self.assertEqual(parsed.eta, "1:22:33")

    def test_deepspeed_parser_extracts_throughput(self) -> None:
        parsed = parse_training_output(
            "deepspeed",
            DEEPSPEED_LOG,
            r"Training complete",
            r"Traceback",
        )
        self.assertEqual(parsed.parser, "deepspeed")
        self.assertEqual(parsed.step, 812)
        self.assertAlmostEqual(parsed.loss, 1.9321, places=4)
        self.assertAlmostEqual(parsed.tokens_per_sec, 4321.8, places=1)
        self.assertAlmostEqual(parsed.samples_per_sec, 12.7, places=1)

    def test_completion_and_error_flags(self) -> None:
        parsed = parse_training_output(
            "generic_torch",
            "step=1 loss=2.3\nTraining complete\n",
            r"Training complete",
            r"Traceback",
        )
        self.assertTrue(parsed.completion_matched)
        self.assertFalse(parsed.error_matched)


if __name__ == "__main__":
    unittest.main()
