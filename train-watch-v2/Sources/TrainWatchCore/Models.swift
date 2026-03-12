import Foundation

public enum TrainingParserKind: String, Codable, CaseIterable {
    case auto
    case mapAnything = "mapanything"
    case genericTorch = "generic_torch"
    case deepSpeed = "deepspeed"
}

public enum RunStatus: String, Codable, CaseIterable {
    case idle
    case running
    case stalled
    case completed
    case failed
    case unknown
    case connecting
}

public enum SSHCredentialKind: String, Codable, CaseIterable {
    case password
    case keychainReference
    case importedPrivateKey
}

public struct SSHJumpHost: Codable, Equatable {
    public var host: String
    public var port: Int
    public var user: String

    public init(host: String, port: Int = 22, user: String) {
        self.host = host
        self.port = port
        self.user = user
    }
}

public struct SSHCredentialReference: Codable, Equatable {
    public var kind: SSHCredentialKind
    public var referenceID: String

    public init(kind: SSHCredentialKind, referenceID: String) {
        self.kind = kind
        self.referenceID = referenceID
    }
}

public struct SSHConnectionProfile: Codable, Equatable {
    public var label: String
    public var host: String
    public var port: Int
    public var user: String
    public var credential: SSHCredentialReference?
    public var jumpHosts: [SSHJumpHost]

    public init(
        label: String,
        host: String,
        port: Int = 22,
        user: String,
        credential: SSHCredentialReference? = nil,
        jumpHosts: [SSHJumpHost] = []
    ) {
        self.label = label
        self.host = host
        self.port = port
        self.user = user
        self.credential = credential
        self.jumpHosts = jumpHosts
    }
}

public struct MonitoredRunPlan: Codable, Equatable {
    public var id: String
    public var label: String
    public var logPath: String?
    public var logGlob: String?
    public var processMatch: String
    public var parser: TrainingParserKind
    public var stallAfterSeconds: Int

    public init(
        id: String,
        label: String,
        logPath: String? = nil,
        logGlob: String? = nil,
        processMatch: String = "",
        parser: TrainingParserKind = .auto,
        stallAfterSeconds: Int = 900
    ) {
        self.id = id
        self.label = label
        self.logPath = logPath
        self.logGlob = logGlob
        self.processMatch = processMatch
        self.parser = parser
        self.stallAfterSeconds = stallAfterSeconds
    }
}

public struct MatchedProcess: Codable, Equatable {
    public var pid: Int?
    public var elapsedSeconds: Int?
    public var command: String

    public init(pid: Int? = nil, elapsedSeconds: Int? = nil, command: String = "") {
        self.pid = pid
        self.elapsedSeconds = elapsedSeconds
        self.command = command
    }
}

public struct ParsedTrainingState: Codable, Equatable {
    public var parser: String
    public var epoch: Int?
    public var step: Int?
    public var stepTotal: Int?
    public var loss: Double?
    public var evalLoss: Double?
    public var lr: Double?
    public var gradNorm: Double?
    public var tokensPerSec: Double?
    public var samplesPerSec: Double?
    public var eta: String
    public var etaSeconds: Int?
    public var lastLogLine: String
    public var completionMatched: Bool
    public var errorMatched: Bool

    public init(
        parser: String,
        epoch: Int? = nil,
        step: Int? = nil,
        stepTotal: Int? = nil,
        loss: Double? = nil,
        evalLoss: Double? = nil,
        lr: Double? = nil,
        gradNorm: Double? = nil,
        tokensPerSec: Double? = nil,
        samplesPerSec: Double? = nil,
        eta: String = "",
        etaSeconds: Int? = nil,
        lastLogLine: String = "",
        completionMatched: Bool = false,
        errorMatched: Bool = false
    ) {
        self.parser = parser
        self.epoch = epoch
        self.step = step
        self.stepTotal = stepTotal
        self.loss = loss
        self.evalLoss = evalLoss
        self.lr = lr
        self.gradNorm = gradNorm
        self.tokensPerSec = tokensPerSec
        self.samplesPerSec = samplesPerSec
        self.eta = eta
        self.etaSeconds = etaSeconds
        self.lastLogLine = lastLogLine
        self.completionMatched = completionMatched
        self.errorMatched = errorMatched
    }
}

public struct RunActivity: Codable, Equatable {
    public var taskName: String
    public var taskCommand: String
    public var taskPID: Int?
    public var startedAt: String
    public var elapsedSeconds: Int?
    public var remainingSeconds: Int?
    public var estimatedEndAt: String
    public var progressPercent: Double?

    public init(
        taskName: String = "",
        taskCommand: String = "",
        taskPID: Int? = nil,
        startedAt: String = "",
        elapsedSeconds: Int? = nil,
        remainingSeconds: Int? = nil,
        estimatedEndAt: String = "",
        progressPercent: Double? = nil
    ) {
        self.taskName = taskName
        self.taskCommand = taskCommand
        self.taskPID = taskPID
        self.startedAt = startedAt
        self.elapsedSeconds = elapsedSeconds
        self.remainingSeconds = remainingSeconds
        self.estimatedEndAt = estimatedEndAt
        self.progressPercent = progressPercent
    }
}
