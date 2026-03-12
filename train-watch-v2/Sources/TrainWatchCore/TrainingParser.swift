import Foundation

public enum TrainingParser {
    private static let numberPattern = #"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?"#

    public static func parse(
        parserName: TrainingParserKind,
        text: String,
        completionRegex: String = #"(Training complete|Finished training|saving final checkpoint)"#,
        errorRegex: String = #"(Traceback|RuntimeError|CUDA out of memory|NCCL error|AssertionError)"#
    ) -> ParsedTrainingState {
        var state: ParsedTrainingState

        switch parserName {
        case .auto, .mapAnything:
            state = parseMapAnything(text)
            if state.loss == nil && parserName == .auto {
                let generic = parseGenericTorch(text)
                if generic.loss != nil || generic.step != nil {
                    state = generic
                }
            }
            if state.loss == nil && parserName == .auto {
                let deepSpeed = parseDeepSpeed(text)
                if deepSpeed.loss != nil || deepSpeed.step != nil {
                    state = deepSpeed
                }
            }
        case .genericTorch:
            state = parseGenericTorch(text)
        case .deepSpeed:
            state = parseDeepSpeed(text)
        }

        state.completionMatched = matches(pattern: completionRegex, in: text)
        state.errorMatched = matches(pattern: errorRegex, in: text)
        return state
    }

    public static func parseMapAnything(_ text: String) -> ParsedTrainingState {
        let signals = collectSignalLines(text)
        let trainLine = signals.lastTrain
        let evalLine = signals.lastEval
        var state = ParsedTrainingState(parser: TrainingParserKind.mapAnything.rawValue, lastLogLine: signals.lastAny)

        if let eta = firstCapture(pattern: #"eta:\s*([0-9:]+)"#, in: trainLine) {
            state.eta = eta
            state.etaSeconds = parseEtaSeconds(eta)
        }

        let epochStep = extractEpochStep(trainLine)
        state.epoch = epochStep.epoch
        state.step = epochStep.step
        state.stepTotal = epochStep.stepTotal

        let metrics = parseCommonMetrics(trainLine)
        state.loss = metrics.loss
        state.lr = metrics.lr
        state.gradNorm = metrics.gradNorm

        if !evalLine.isEmpty {
            let evalMetrics = parseCommonMetrics(evalLine)
            state.evalLoss = evalMetrics.loss ?? evalMetrics.evalLoss
            if state.loss == nil, let evalLoss = state.evalLoss {
                state.loss = evalLoss
            }
        }

        return state
    }

    public static func parseGenericTorch(_ text: String) -> ParsedTrainingState {
        let signals = collectSignalLines(text)
        let line = signals.lastTrain
        var state = ParsedTrainingState(parser: TrainingParserKind.genericTorch.rawValue, lastLogLine: signals.lastAny)

        if let eta = firstCapture(pattern: #"eta[:=\s]+([0-9:]+)"#, in: line) {
            state.eta = eta
            state.etaSeconds = parseEtaSeconds(eta)
        }

        state.epoch = extractInt(pattern: #"epoch[:=\s\[]+(\d+)"#, in: line)
        state.step = extractInt(pattern: #"(?:step|iter|global_step)[:=\s]+(\d+)"#, in: line) ?? extractEpochStep(line).step
        state.stepTotal = extractInt(pattern: #"(?:step_total|total_steps|iters?)[:=\s]+(\d+)"#, in: line) ?? extractEpochStep(line).stepTotal

        let metrics = parseCommonMetrics(line)
        state.loss = metrics.loss
        state.evalLoss = metrics.evalLoss
        state.lr = metrics.lr
        state.gradNorm = metrics.gradNorm
        state.tokensPerSec = metrics.tokensPerSec
        state.samplesPerSec = metrics.samplesPerSec
        return state
    }

    public static func parseDeepSpeed(_ text: String) -> ParsedTrainingState {
        let signals = collectSignalLines(text)
        let line = signals.lastTrain
        var state = ParsedTrainingState(parser: TrainingParserKind.deepSpeed.rawValue, lastLogLine: signals.lastAny)

        state.epoch = extractInt(pattern: #"epoch[:=\s\[]+(\d+)"#, in: line)
        state.step = extractInt(pattern: #"(?:step|global_step)[:=\s]+(\d+)"#, in: line)
        state.stepTotal = extractInt(pattern: #"(?:total_steps|steps_total)[:=\s]+(\d+)"#, in: line)

        let metrics = parseCommonMetrics(line)
        state.loss = metrics.loss
        state.evalLoss = metrics.evalLoss
        state.lr = metrics.lr
        state.gradNorm = metrics.gradNorm
        state.tokensPerSec = metrics.tokensPerSec
        state.samplesPerSec = metrics.samplesPerSec

        if let eta = firstCapture(pattern: #"eta[:=\s]+([0-9:]+)"#, in: line) {
            state.eta = eta
            state.etaSeconds = parseEtaSeconds(eta)
        }

        return state
    }

    private static func parseEtaSeconds(_ eta: String) -> Int? {
        guard eta.contains(":") else { return nil }
        let parts = eta.split(separator: ":").map { String($0).trimmingCharacters(in: .whitespacesAndNewlines) }.filter { !$0.isEmpty }
        let integers = parts.compactMap(Int.init)
        guard integers.count == parts.count else { return nil }
        switch integers.count {
        case 3:
            return integers[0] * 3600 + integers[1] * 60 + integers[2]
        case 2:
            return integers[0] * 60 + integers[1]
        default:
            return nil
        }
    }

    private static func collectSignalLines(_ text: String) -> (lastAny: String, lastTrain: String, lastEval: String) {
        let lines = text
            .split(whereSeparator: \ .isNewline)
            .map { String($0).trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }

        let lastAny = lines.last ?? ""
        var lastTrain = ""
        var lastEval = ""

        for line in lines.reversed() {
            if lastEval.isEmpty && matches(pattern: #"Test\s+Epoch|eval|validation"#, in: line) {
                lastEval = line
            }
            let isEvalLine = matches(pattern: #"Test\s+Epoch|eval|validation"#, in: line)
            if lastTrain.isEmpty && !isEvalLine && matches(pattern: #"Epoch:|step=|global_step|loss"#, in: line) {
                lastTrain = line
            }
            if !lastTrain.isEmpty && !lastEval.isEmpty {
                break
            }
        }

        return (
            lastAny: lastAny,
            lastTrain: lastTrain.isEmpty ? lastAny : lastTrain,
            lastEval: lastEval
        )
    }

    private static func extractEpochStep(_ line: String) -> (epoch: Int?, step: Int?, stepTotal: Int?) {
        (
            epoch: extractInt(pattern: #"(?:Train\s+)?Epoch:\s*\[(\d+)\]"#, in: line),
            step: extractInt(pattern: #"\[(\d+)\s*/\s*\d+\]"#, in: line),
            stepTotal: extractInt(pattern: #"\[\d+\s*/\s*(\d+)\]"#, in: line)
        )
    }

    private static func parseCommonMetrics(_ line: String) -> (
        loss: Double?,
        evalLoss: Double?,
        lr: Double?,
        gradNorm: Double?,
        tokensPerSec: Double?,
        samplesPerSec: Double?
    ) {
        (
            loss: extractFloat(pattern: #"(?:^|\s)loss[:=\s]+("# + numberPattern + #")"#, in: line),
            evalLoss: extractFloat(pattern: #"(?:eval[_\s-]*loss|val[_\s-]*loss)[:=\s]+("# + numberPattern + #")"#, in: line),
            lr: extractFloat(pattern: #"(?:^|\s)lr[:=\s]+("# + numberPattern + #")"#, in: line),
            gradNorm: extractFloat(pattern: #"grad[_\s-]*norm[:=\s]+("# + numberPattern + #")"#, in: line),
            tokensPerSec: extractFloat(pattern: #"(?:tokens?/sec|tokens?/s|tok/sec|tok/s|toks?/s)[:=\s]+("# + numberPattern + #")"#, in: line),
            samplesPerSec: extractFloat(pattern: #"(?:samples?/sec|samples?/s|imgs?/sec|imgs?/s)[:=\s]+("# + numberPattern + #")"#, in: line)
        )
    }

    private static func extractFloat(pattern: String, in text: String) -> Double? {
        guard let capture = firstCapture(pattern: pattern, in: text) else { return nil }
        return Double(capture)
    }

    private static func extractInt(pattern: String, in text: String) -> Int? {
        guard let capture = firstCapture(pattern: pattern, in: text) else { return nil }
        return Int(capture)
    }

    private static func matches(pattern: String, in text: String) -> Bool {
        guard !pattern.isEmpty else { return false }
        do {
            let regex = try NSRegularExpression(pattern: pattern, options: [.caseInsensitive])
            let range = NSRange(text.startIndex..., in: text)
            return regex.firstMatch(in: text, options: [], range: range) != nil
        } catch {
            return false
        }
    }

    private static func firstCapture(pattern: String, in text: String) -> String? {
        do {
            let regex = try NSRegularExpression(pattern: pattern, options: [.caseInsensitive])
            let range = NSRange(text.startIndex..., in: text)
            guard let match = regex.firstMatch(in: text, options: [], range: range), match.numberOfRanges > 1 else {
                return nil
            }
            guard let captureRange = Range(match.range(at: 1), in: text) else { return nil }
            return String(text[captureRange])
        } catch {
            return nil
        }
    }
}
