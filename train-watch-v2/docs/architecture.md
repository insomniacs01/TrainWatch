# Train Watch v2 架构设计

## 1. 现状审计结论

结合当前仓库根目录下的 v1 实现，可以把现阶段能力拆成两部分：

### 1.1 应该保留的资产

- `app/parsers.py`：训练日志解析规则已经覆盖 `mapanything / generic_torch / deepspeed`
- `app/collector.py`：任务自动发现、主进程选择、时长与 ETA 推导逻辑已经很成熟
- `app/models.py`：节点 / GPU / 训练任务的数据模型已经清晰
- `tests/test_parsers.py` 与 `tests/test_collector.py`：给 v2 提供了行为基线

### 1.2 必须替换的部分

- `app/main.py`：这是典型中心服务 API 壳，不适合“手机自己是入口”的目标
- `app/runtime.py`：依赖服务端常驻轮询和 WebSocket 广播
- `static/*`：PWA 适合作为 v1，但不适合作为 v2 主形态

## 2. v2 的北极星目标

v2 默认假设是：

- iPhone 是主入口
- 手机前台直连 SSH 是主路径
- 不要求一台笔记本长期在线
- relay 只作为可选增强层，而不是默认依赖

## 3. 推荐的 v2 分层

### 3.1 `TrainWatchCore`

纯 Swift 共享核心，不依赖 UI。

职责：

- 定义节点、训练任务、GPU、SSH 路由等领域模型
- 迁移 v1 解析规则
- 迁移 ETA / 进度 / 主任务摘要逻辑
- 固化 v2 的数据契约，避免未来 UI 和 SSH 层耦合

本次提交已经落地这一层。

### 3.2 `TrainWatchTransportSSH`（下一阶段）

职责：

- 管理 SSH 会话
- 执行远端 probe 命令
- 支持密码 / 私钥 / Keychain 引用
- 为未来的 `ProxyJump` / 多跳链路预留接口

### 3.3 `TrainWatchApp`（下一阶段）

SwiftUI App 壳。

职责：

- 连接表单
- 节点列表页
- 节点详情页
- 任务详情与曲线
- 本地缓存与应用内提醒

### 3.4 Optional Relay Bridge（后续增强）

职责：

- 在你有常在线机器时提供后台采集、长时间历史和结束提醒
- 兼容 v1 的 FastAPI/SQLite 形态
- 对 v2 来说是增强件，不是必须件

## 4. 数据流设计

### 模式 A：手机直连（主路径）

1. 用户在 iPhone 输入 SSH 连接信息
2. App 构建 SSH route / credential plan
3. SSH 层执行远端 probe 命令
4. 返回原始 JSON 与日志 tail
5. `TrainWatchCore` 本地解析训练状态并估算 ETA
6. SwiftUI 渲染节点、GPU、任务、图表与提醒

### 模式 B：Relay（增强路径）

1. App 连接 relay API
2. Relay 使用现有 v1 collector/SQLite 持续采集
3. App 只负责展示与筛选

## 5. 与 v1 的融合方式

### 直接复用

- 解析规则：已迁移到 `Sources/TrainWatchCore/TrainingParser.swift`
- ETA / 进度估算：已迁移到 `Sources/TrainWatchCore/RunEstimator.swift`
- 连接模型：已在 `Sources/TrainWatchCore/Models.swift` 中为密码、私钥、跳板链预留

### 下一步复用

- 抽取 `app/collector.py` 里的远端 probe 脚本为共享资源
- 让 v1 和 v2 都调用同一份 probe 模板，避免自动发现逻辑分叉

## 6. v2 第一阶段范围

### 目标能力

- 直连一台 SSH 主机
- 输入 `host / port / user / password` 或私钥引用
- 获取真实节点状态
- 展示 GPU / CPU / RAM / Disk / 正在跑的训练任务
- 展示任务已运行多久、还剩多久、预计何时完成

### 暂不做

- iOS 后台长连
- 系统级推送
- Slurm / Kubernetes / W&B / MLflow
- 完整的 OpenSSH 配置导入器
- 多用户权限体系

## 7. 屏幕规划

### 首页

- 节点卡片列表
- 在线状态 / GPU busy / 最近 loss / ETA / 最后刷新时间

### 连接页

- 手填 SSH
- 选择密码或私钥
- 预留“导入 SSH config / alias”入口

### 节点详情页

- 节点基础指标
- GPU 卡片
- 任务卡片
- 曲线区域
- 最近日志 / 最近进程

## 8. 风险与应对

### SSH 实现风险

原生 iOS 下不能直接照搬 Paramiko。

应对：

- SSH 层独立成单独模块
- 先把 `TrainWatchCore` 稳定下来，再替换 transport 实现
- 优先选择维护活跃、能跑在 Apple 平台的 GitHub SSH 库

### 规则分叉风险

如果 v1/v2 各自维护一套自动发现脚本，后续会越来越难统一。

应对：

- 下一步尽快抽共享 probe 模板
- Core 层只负责解析和展示，不重复发明采集启发式

## 9. 迁移路线

### M0（本次）

- 完成代码审计
- 完成 GitHub 方案调研
- 新建 `train-watch-v2`
- 落地可测试的 Swift Core

### M1

- 接入真实 SSH 执行层
- 打通单主机真实采集
- 用真实返回值驱动 SwiftUI 首页

### M2

- 加入本地历史缓存
- 节点详情图表
- 应用内提醒

### M3

- 支持导入 SSH config / jump host
- 支持多节点连接编排
- 视情况接回可选 relay
