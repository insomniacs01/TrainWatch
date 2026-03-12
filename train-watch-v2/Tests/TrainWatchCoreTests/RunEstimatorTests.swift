import XCTest
@testable import TrainWatchCore

final class RunEstimatorTests: XCTestCase {
    func testRemainingSecondsCanBeEstimatedFromProgress() {
        let parsed = TrainingParser.parse(
            parserName: .genericTorch,
            text: "Epoch: [2]  [50/100]  lr: 0.000020  loss: 1.0000  grad_norm: 0.50",
            completionRegex: "Training complete",
            errorRegex: "Traceback"
        )

        let activity = RunEstimator.deriveActivity(
            parsed: parsed,
            matchedProcesses: [
                MatchedProcess(pid: 9999, elapsedSeconds: 600, command: "python /workspace/train.py --config conf.yaml")
            ],
            collectedAt: "2026-03-11T10:00:00Z",
            status: .running
        )

        XCTAssertEqual(activity.taskName, "python train.py")
        XCTAssertEqual(activity.taskPID, 9999)
        XCTAssertEqual(activity.elapsedSeconds, 600)
        XCTAssertEqual(activity.remainingSeconds, 600)
        XCTAssertEqual(activity.startedAt, "2026-03-11T09:50:00Z")
        XCTAssertEqual(activity.estimatedEndAt, "2026-03-11T10:10:00Z")
        XCTAssertEqual(activity.progressPercent, 50.0, accuracy: 0.1)
    }

    func testPrimaryProcessPrefersTorchrun() {
        let parsed = TrainingParser.parse(
            parserName: .auto,
            text: "Epoch: [1]  [25/100]  eta: 0:30:00  lr: 0.000100  loss: 2.5000  grad_norm: 1.00",
            completionRegex: "Training complete",
            errorRegex: "Traceback"
        )

        let activity = RunEstimator.deriveActivity(
            parsed: parsed,
            matchedProcesses: [
                MatchedProcess(pid: 4322, elapsedSeconds: 890, command: "python train.py --local_rank=1"),
                MatchedProcess(pid: 4321, elapsedSeconds: 900, command: "torchrun train.py --config demo.yaml")
            ],
            collectedAt: "2026-03-11T10:00:00Z",
            status: .running
        )

        XCTAssertEqual(activity.taskName, "torchrun train.py")
        XCTAssertEqual(activity.taskPID, 4321)
        XCTAssertEqual(activity.elapsedSeconds, 900)
        XCTAssertEqual(activity.remainingSeconds, 1800)
        XCTAssertEqual(activity.startedAt, "2026-03-11T09:45:00Z")
        XCTAssertEqual(activity.estimatedEndAt, "2026-03-11T10:30:00Z")
        XCTAssertEqual(activity.progressPercent, 25.0, accuracy: 0.1)
    }
}
