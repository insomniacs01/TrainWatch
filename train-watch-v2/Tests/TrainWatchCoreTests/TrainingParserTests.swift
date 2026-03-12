import XCTest
@testable import TrainWatchCore

final class TrainingParserTests: XCTestCase {
    private let mapAnythingLog = """
    Epoch: [4]  [123/500]  eta: 1:22:33  lr: 0.000050  loss: 0.4821 (0.6231)  grad_norm: 1.2300  time: 1.2312  data: 0.0111  max mem: 42321
    Test Epoch: [4]  [203/204]  loss: 0.3910 (0.4011)  scale_err_mean: 0.0211
    """

    private let deepSpeedLog = """
    [2026-03-11 10:00:00,000] [INFO] [engine.py:123:train_batch] step=812 loss=1.9321 lr=1.20e-05 grad_norm=0.83 tokens/s=4321.8 samples/s=12.7 eta=0:12:10
    """

    func testMapAnythingParserExtractsKeyMetrics() {
        let parsed = TrainingParser.parse(
            parserName: .mapAnything,
            text: mapAnythingLog,
            completionRegex: "Training complete",
            errorRegex: "Traceback"
        )

        XCTAssertEqual(parsed.parser, "mapanything")
        XCTAssertEqual(parsed.epoch, 4)
        XCTAssertEqual(parsed.step, 123)
        XCTAssertEqual(parsed.stepTotal, 500)
        XCTAssertEqual(parsed.loss, 0.4821, accuracy: 0.0001)
        XCTAssertEqual(parsed.evalLoss, 0.3910, accuracy: 0.0001)
        XCTAssertEqual(parsed.eta, "1:22:33")
        XCTAssertEqual(parsed.etaSeconds, 4953)
    }

    func testDeepSpeedParserExtractsThroughput() {
        let parsed = TrainingParser.parse(
            parserName: .deepSpeed,
            text: deepSpeedLog,
            completionRegex: "Training complete",
            errorRegex: "Traceback"
        )

        XCTAssertEqual(parsed.parser, "deepspeed")
        XCTAssertEqual(parsed.step, 812)
        XCTAssertEqual(parsed.loss, 1.9321, accuracy: 0.0001)
        XCTAssertEqual(parsed.tokensPerSec, 4321.8, accuracy: 0.1)
        XCTAssertEqual(parsed.samplesPerSec, 12.7, accuracy: 0.1)
    }

    func testCompletionAndErrorFlags() {
        let parsed = TrainingParser.parse(
            parserName: .genericTorch,
            text: "step=1 loss=2.3\nTraining complete\n",
            completionRegex: "Training complete",
            errorRegex: "Traceback"
        )

        XCTAssertTrue(parsed.completionMatched)
        XCTAssertFalse(parsed.errorMatched)
    }
}
