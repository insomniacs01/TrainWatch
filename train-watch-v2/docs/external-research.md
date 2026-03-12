# 外部 GitHub 调研

这次 v2 不是闭门重写，而是先看现成生态里哪些仓库值得借力。

## 1. 重点候选

### `orlandos-nl/Citadel`

GitHub：<https://github.com/orlandos-nl/Citadel>

为什么关注：

- 面向 Swift 的 SSH2 库
- 适合承担 v2 的 SSH transport 层
- 更接近原生 Apple 平台开发习惯

结论：

- 作为 v2 的首选 SSH transport 候选
- 适合放到 `TrainWatchTransportSSH` 层，不直接污染 Core

### `migueldeicaza/SwiftTerm`

GitHub：<https://github.com/migueldeicaza/SwiftTerm>

为什么关注：

- 原生终端视图组件成熟
- 如果未来需要“调试视图 / 应急终端 / 查看 stdout”会很有用

结论：

- 不是 v2 第一阶段必需
- 适合作为后续增强项，用于节点详情页里的诊断模式

## 2. 为什么这次没有直接整仓 vendoring

原因不是“不能用外部仓库”，而是当前更需要先稳定这三件事：

- 数据契约
- 训练解析规则
- SSH 连接抽象边界

如果现在直接把外部 SSH 项目大段塞进来，反而会把 v2 的边界变模糊。

所以本次采取的是：

- 先做 GitHub 调研
- 明确将来接哪个 SSH 库
- 先把你现有代码里最值钱的逻辑抽成可测试核心层

这样下一步接 Citadel 时，只需要实现 transport，不需要重写上层状态逻辑。

## 3. 选型原则

后续引入外部库时，按下面顺序取舍：

1. Apple 平台可用性
2. SSH 稳定性与维护活跃度
3. 是否容易封装为独立 transport 层
4. 是否能支持后续 jump host / route chain
5. 是否不破坏当前 `TrainWatchCore` 的纯逻辑层定位
