# Train Watch v2

Train Watch v2 是新的 iPhone 原生方向骨架。

这次不再沿着仓库根目录下 v1 的“常在线后端 + PWA”继续硬改，而是把真正有价值的训练监控知识抽出来，给未来的原生客户端复用：

- 训练日志解析规则
- 任务状态与 ETA 估算逻辑
- SSH 连接与跳板链的数据模型
- 手机直连为主、可选 relay 为辅的架构设计

## 这次已经落地的内容

- `Package.swift`：新建可测试的 Swift package
- `Sources/TrainWatchCore`：v2 共享核心层
  - `Models.swift`：领域模型与 SSH 连接规划模型
  - `TrainingParser.swift`：从 v1 迁移的日志解析规则
  - `RunEstimator.swift`：从 v1 迁移的任务时长 / ETA / 命令摘要逻辑
- `Tests/TrainWatchCoreTests`：对齐 v1 行为的核心单测
- `docs/architecture.md`：v2 架构与迁移设计
- `docs/external-research.md`：GitHub 外部项目调研与选型

## 为什么先做 Swift Core，而不是先搭 UI

因为真正可复用、也最容易跑偏的不是 UI，而是：

- 日志里到底怎么识别 `loss / eval_loss / lr / grad_norm / ETA`
- 没有显式 ETA 时，怎么从 `step / step_total + elapsed` 估算结束时间
- 多进程训练时，哪个进程才是主任务
- 将来支持跳板机 / 导入 SSH config 时，连接模型怎么定义

这些规则一旦稳定，SwiftUI 壳、Keychain、Charts、Citadel 集成都能往上接。

## 本地验证

```bash
cd train-watch-v2
swift test
```

如果你当前机器上的 Command Line Tools 缺少完整的 SwiftPM/XCTest 支持，也可以先用我这次实际跑通的两步：

```bash
cd train-watch-v2
swiftc -typecheck Sources/TrainWatchCore/*.swift
```

然后再用一个临时 `main.swift` 做 smoke test，把 `TrainingParser` 和 `RunEstimator` 直接编进可执行文件验证。

## 下一步建议

1. 在这个 package 上再加一个 `TrainWatchApp` iOS target
2. 用 GitHub 上的 SSH 库接入真正的远程执行
3. 把 v1 的远端 probe 脚本提取成共享资源，让 v1/v2 共用同一套采集规则
4. 再把 SwiftUI 首页、节点详情、连接表单、Keychain 存储接上
