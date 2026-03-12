import Foundation

public enum RunEstimator {
    public static func deriveActivity(
        parsed: ParsedTrainingState,
        matchedProcesses: [MatchedProcess],
        collectedAt: String,
        status: RunStatus
    ) -> RunActivity {
        let primary = selectPrimaryProcess(matchedProcesses)
        let command = primary?.command.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        let elapsedSeconds = primary?.elapsedSeconds
        let collectedAtDate = parseISO8601(collectedAt)

        var startedAt = ""
        if let collectedAtDate, let elapsedSeconds {
            startedAt = iso8601(collectedAtDate.addingTimeInterval(TimeInterval(-elapsedSeconds)))
        }

        let remainingSeconds = deriveRemainingSeconds(parsed: parsed, elapsedSeconds: elapsedSeconds, status: status)
        var estimatedEndAt = ""
        if let collectedAtDate, let remainingSeconds {
            estimatedEndAt = iso8601(collectedAtDate.addingTimeInterval(TimeInterval(remainingSeconds)))
        }

        return RunActivity(
            taskName: summarizeCommand(command),
            taskCommand: command,
            taskPID: primary?.pid,
            startedAt: startedAt,
            elapsedSeconds: elapsedSeconds,
            remainingSeconds: remainingSeconds,
            estimatedEndAt: estimatedEndAt,
            progressPercent: deriveProgressPercent(parsed: parsed)
        )
    }

    public static func summarizeCommand(_ command: String) -> String {
        guard !command.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return "" }
        let parts = safeSplit(command)
        guard !parts.isEmpty else { return command.trimmingCharacters(in: .whitespacesAndNewlines) }

        var launcher = basename(parts[0])
        var searchParts = Array(parts.dropFirst())

        if launcher == "accelerate", parts.count > 1, parts[1] == "launch" {
            launcher = "accelerate launch"
            searchParts = Array(parts.dropFirst(2))
        }

        if launcher.hasPrefix("python") || ["torchrun", "deepspeed", "accelerate launch", "bash", "sh"].contains(launcher) {
            if let script = searchParts.first(where: { $0.hasSuffix(".py") || $0.hasSuffix(".sh") }) {
                return "\(launcher) \(basename(script))"
            }
            if let token = searchParts.first(where: { !$0.hasPrefix("-") && !$0.contains("=") }) {
                return "\(launcher) \(basename(token))"
            }
        }

        return basename(parts[0])
    }

    public static func deriveRemainingSeconds(
        parsed: ParsedTrainingState,
        elapsedSeconds: Int?,
        status: RunStatus
    ) -> Int? {
        if status == .completed {
            return 0
        }
        if let etaSeconds = parsed.etaSeconds {
            return max(0, etaSeconds)
        }
        guard let elapsedSeconds, let step = parsed.step, let stepTotal = parsed.stepTotal else {
            return nil
        }
        if stepTotal <= 0 || step <= 0 {
            return nil
        }
        if step >= stepTotal {
            return 0
        }

        let progress = min(max(Double(step) / Double(stepTotal), 0.0), 0.999_999)
        let estimatedTotal = Int((Double(elapsedSeconds) / progress).rounded())
        return max(0, estimatedTotal - elapsedSeconds)
    }

    public static func deriveProgressPercent(parsed: ParsedTrainingState) -> Double? {
        guard let step = parsed.step, let stepTotal = parsed.stepTotal, stepTotal > 0 else { return nil }
        let percent = max(0.0, min(100.0, (Double(step) / Double(stepTotal)) * 100.0))
        return (percent * 10).rounded() / 10
    }

    public static func selectPrimaryProcess(_ processes: [MatchedProcess]) -> MatchedProcess? {
        guard !processes.isEmpty else { return nil }
        return processes.sorted {
            let lhsPriority = commandPriority($0.command)
            let rhsPriority = commandPriority($1.command)
            if lhsPriority != rhsPriority { return lhsPriority < rhsPriority }

            let lhsElapsed = $0.elapsedSeconds ?? 0
            let rhsElapsed = $1.elapsedSeconds ?? 0
            if lhsElapsed != rhsElapsed { return lhsElapsed > rhsElapsed }

            return ($0.pid ?? 0) < ($1.pid ?? 0)
        }.first
    }

    private static func commandPriority(_ command: String) -> Int {
        if regexMatches(#"\b(torchrun|deepspeed)\b"#, in: command) {
            return 0
        }
        if regexMatches(#"\baccelerate\s+launch\b"#, in: command) {
            return 1
        }
        if regexMatches(#"\bpython(?:\d+(?:\.\d+)*)?\b"#, in: command) {
            return 2
        }
        return 3
    }

    private static func basename(_ token: String) -> String {
        token.trimmingCharacters(in: CharacterSet(charactersIn: "/")).split(separator: "/").last.map(String.init) ?? token
    }

    private static func safeSplit(_ command: String) -> [String] {
        var tokens: [String] = []
        var current = ""
        var quote: Character?

        for char in command {
            if let activeQuote = quote {
                if char == activeQuote {
                    quote = nil
                } else {
                    current.append(char)
                }
                continue
            }

            if char == "\"" || char == "'" {
                quote = char
                continue
            }

            if char.isWhitespace {
                if !current.isEmpty {
                    tokens.append(current)
                    current = ""
                }
            } else {
                current.append(char)
            }
        }

        if !current.isEmpty {
            tokens.append(current)
        }
        return tokens
    }

    private static func parseISO8601(_ value: String) -> Date? {
        guard !value.isEmpty else { return nil }
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let date = formatter.date(from: value) {
            return date
        }
        formatter.formatOptions = [.withInternetDateTime]
        return formatter.date(from: value)
    }

    private static func iso8601(_ date: Date) -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        return formatter.string(from: date)
    }

    private static func regexMatches(_ pattern: String, in text: String) -> Bool {
        do {
            let regex = try NSRegularExpression(pattern: pattern, options: [.caseInsensitive])
            let range = NSRange(text.startIndex..., in: text)
            return regex.firstMatch(in: text, options: [], range: range) != nil
        } catch {
            return false
        }
    }
}
