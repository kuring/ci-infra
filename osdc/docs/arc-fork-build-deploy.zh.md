# ARC Fork:构建、部署与配置

## ARC Fork

**仓库**: https://github.com/jeanschmidt/actions-runner-controller.git
**分支**: `master`(基于上游 `actions/actions-runner-controller` 的 master)。所有 fork 工作已通过特性分支 PR(`jeanschmidt/placeholder_run_poc`、`jeanschmidt/proactive_capacity_metrics`、`jeanschmidt/proactive_capacity_max_runners`、`jeanschmidt/release_enforcement` 等)合并进 `master`。最初的 PoC 分支 `jeanschmidt/placeholder_run_poc` 仍然存在但已过时——**不要**基于它开展新工作。

**动机**: 为 `ghalistener` 二进制文件添加容量感知自动伸缩(主动容量,proactive capacity)。原版 ARC 基于计数且不感知容量——它伸缩 runner 时不会检查集群是否真的装得下 runner + 工作流 pod 这一对。该 fork 新增了一个 CapacityMonitor goroutine,通过 `X-ScaleSetMaxCapacity` 请求头动态调整报告给 GitHub 的 `maxRunners`,并以占位 pod 预留(placeholder pod reservations)作为支撑。

**变更内容**:
- 新增包: `cmd/ghalistener/capacity/` — 8 个生产文件(`config.go`、`counting.go`、`hud_client.go`、`monitor.go`、`placeholder.go`、`placeholder_cleanup.go`、`placeholder_lifetime.go`、`schedulability.go`)外加 8 个测试文件(`config_test.go`、`counting_test.go`、`hud_client_test.go`、`monitor_test.go`、`placeholder_test.go`、`placeholder_cleanup_test.go`、`pod_spec_test.go`、`schedulability_test.go`);约 7.3k 行代码(2,322 行生产代码 + 4,977 行测试)
- 修改: `cmd/ghalistener/main.go` — 当 `CAPACITY_AWARE_ENABLED=true` 时,将 CapacityMonitor 挂入 listener 的 errgroup
- 修改: `cmd/ghalistener/metrics/` — 容量监控器的 Prometheus 指标,自 `.6` 起(`metrics.go`、`metrics_test.go`、重新生成的 `mocks_test.go`、新增的 `testdata/listener_metrics.yaml`)
- 修改: `gha-runner-scale-set` chart — 可选的 `resourceName` 值,自 `.19` 起,用于独立于 `runnerScaleSetName`(在 GitHub 上注册的名称)命名 Kubernetes 资源:`templates/_helpers.tpl` 中的 scale-set-name helper 为 `.Values.resourceName | default .Values.runnerScaleSetName | default .Release.Name`(另加 `values.yaml` 和 `tests/template_test.go`)
- 修改: `gha-runner-scale-set-controller` chart — `values.yaml` 将 `image.repository` 默认值设为 `ghcr.io/jeanschmidt/gha-runner-scale-set-controller`
- Controller 与 CRD 保持原版(`controllers/`、`apis/`、`config/` 及各 chart 的 `crds/` 下没有任何改动)

## Helm Chart

通过 `gha-publish-chart.yaml` 工作流从 fork 发布(手动 `workflow_dispatch` 触发)。该工作流构建 controller 镜像(多架构 `linux/amd64` + `linux/arm64`)并将 chart 发布到 GHCR。

- **OCI registry**: `oci://ghcr.io/jeanschmidt/actions-runner-controller-charts/gha-runner-scale-set-controller`
- **Chart 版本**: 在 `clusters.yaml` 的 `arc.chart_version` 中配置(当前为 `0.14.1-jeanschmidt.20`)。格式为 `<upstream-base>-jeanschmidt.<N>`;每次 fork 发布时递增 `<N>`。该值同时作为 Helm chart 版本和 OCI 镜像 tag 生效。
- **镜像 tag**: 该工作流每次构建会发布两个 tag:
  - `ghcr.io/jeanschmidt/gha-runner-scale-set-controller:<release_tag_name>`(滚动 release tag — 传 `release_tag_name=0.14.1-jeanschmidt.20` 以与 chart 匹配)
  - `ghcr.io/jeanschmidt/gha-runner-scale-set-controller:<release_tag_name>-<short_sha>`(内置源 commit 的不可变 tag,便于锁定版本和调试)

发布新 chart 版本:从 fork 的 GitHub Actions 界面触发 `gha-publish-chart.yaml`,并设置 `publish_gha_runner_scale_set_controller_chart: true`。

**`release_tag_name` 精确匹配强制**: 工作流的第一步会运行 `hack/check-gh-chart-versions.sh ${{ inputs.release_tag_name }}`,除非 `release_tag_name` 与**全部四个** chart 字段完全一致——`gha-runner-scale-set-controller` 和 `gha-runner-scale-set` 各自的 `version` 与 `appVersion`——否则拒绝构建。部分匹配(如仅 major.minor 一致)会在构建时失败。下文"Controller 版本检查"中描述的 major.minor 宽限是 `IsVersionAllowed` 的仅运行时行为,并非构建时的放宽。

## 使用本地 Chart(开发工作流)

本地开发时,可以跳过把 chart 发布到 GHCR,让 `deploy.sh` 直接指向本地 chart 路径:

在 `modules/arc/deploy.sh` 中,把 OCI chart 引用:

```bash
# 替换这一段:
  oci://ghcr.io/jeanschmidt/actions-runner-controller-charts/gha-runner-scale-set-controller \
  --version "${ARC_CHART_VERSION}" \

# 换成本地路径(不再需要 --version):
  /Users/jschmidt/meta/actions-runner-controller/charts/gha-runner-scale-set-controller \
```

把原始行以 `TODO: restore before committing` 注释的形式保留,以免意外合并进仓库。

这样完全跳过了 chart 发布步骤——Helm 直接从磁盘读取 chart。但仍需构建并推送 controller 镜像到 Harbor(见下文)。

## 构建 Fork 镜像

`Dockerfile` 会把所有二进制文件(manager、ghalistener、webhook server、metrics server、sleep)从 Go 源码构建进 distroless 镜像。

### 版本管理

镜像有两个版本组件,用途不同:

- **`VERSION` 构建参数**(兼容性): 必须匹配 chart `appVersion` 的 **major.minor**(例如 `0.14.1` 可以配合 `appVersion=0.14.1-jeanschmidt.9` 使用,因为 `IsVersionAllowed` 只比较 major.minor)。为什么这一点至关重要,见下文版本检查部分。
- **Docker tag**(标识): 用 `<chart_version>-capacity.<N>` 格式追踪 fork 迭代(例如 `0.14.1-capacity.1`)。每次新构建递增 `<N>`。这正是写入 `deploy.sh` 的 `image.tag` 以及 Harbor 中存储的值。

将 fork rebase 到新的上游 release(如 `0.15.0`)时,同时递增 `VERSION` 构建参数并重置 capacity 后缀(如 `0.15.0-capacity.1`)。

### Controller 版本检查(关键——构建前必读)

ARC controller 内置一个**硬编码的版本对账循环**,会对每个 AutoscalingRunnerSet 运行。每次 reconcile 时,它会比较:

- `buildVersion`: 通过 `-ldflags -X build.Version=<VERSION>` 编译进 controller 二进制(来自 `VERSION` 构建参数)
- `autoscalingRunnerSetVersion`: CR 上的 `app.kubernetes.io/version` 标签,由 runner scale set Helm chart 用 `Chart.AppVersion` 盖章写入

如果两者不匹配,**controller 会删除 AutoscalingRunnerSet CR**。这一切静默发生——Helm 显示部署成功,但 controller 立刻清空所有 runner scale set。所有 listener、runner 和占位 pod 全部消失。

**源码位置**: `controllers/actions.github.com/autoscalingrunnerset_controller.go:139-153`

比较逻辑(`apis/actions.github.com/v1alpha1/version.go:IsVersionAllowed()`)允许以下情况:

| 条件 | 结果 | 示例 |
|-----------|--------|---------|
| `buildVersion == "dev"` | 总是允许 | 未设置 `VERSION` 构建参数时的默认值 |
| `buildVersion` 以 `"canary-"` 开头 | 总是允许 | `canary-test` |
| 字符串精确匹配 | 允许 | 双方均为 `0.14.1` |
| Semver major.minor 匹配 | 允许 | `0.14.0` controller + `0.14.1` chart |
| 其他任何不匹配 | **CR 被删除** | `v0.0.2` controller + `0.14.1` chart |

**常见陷阱**:

- 使用 `v` 前缀(如 `v0.14.1`)——semver 解析器不会剥离 `v`,解析失败,版本被拒绝
- 使用无关版本(如 `v0.0.2`)——无匹配,所有 runner scale set 被删除
- **没有任何 flag、环境变量或配置选项**可以禁用该检查

### 本地构建并推送到 Harbor

```bash
cd /Users/jschmidt/meta/actions-runner-controller

# 为 linux/amd64 构建(集群运行的架构)
# VERSION 必须与 chart 的 appVersion 匹配——不要使用无关版本
docker buildx build \
  --platform linux/amd64 \
  --build-arg VERSION=0.14.1 \
  --build-arg COMMIT_SHA=$(git rev-parse HEAD) \
  -t localhost:8081/osdc/gha-runner-scale-set-controller:0.14.1-capacity.1 \
  -f Dockerfile \
  --load .

# 保存为 tarball 供 crane push 使用(比走 port-forward 的 docker push 快得多)
docker save localhost:8081/osdc/gha-runner-scale-set-controller:0.14.1-capacity.1 -o /tmp/arc-controller.tar

# Port-forward 到 Harbor(如尚未运行)。
# 8081 端口与本仓库所有其他 deploy.sh 使用的约定一致
# (modules/arc、modules/harbor-cache-recovery、base/node-compactor 等)——
# 保持同一端口,避免 `just deploy-module` 的并发 port-forward 互相冲突。
kubectl port-forward -n harbor-system svc/harbor 8081:80 &

# 用 crane 向 Harbor 认证
HARBOR_PASS=$(kubectl get secret -n harbor-system harbor-admin-password -o jsonpath='{.data.password}' | base64 -d)
mise exec -- crane auth login localhost:8081 -u admin -p "$HARBOR_PASS" --insecure

# 通过 crane 推送(比 docker push 快得多——对已有 blob 去重)
mise exec -- crane push --insecure /tmp/arc-controller.tar localhost:8081/osdc/gha-runner-scale-set-controller:0.14.1-capacity.1
```

**Tag 规范**: 始终使用 `<chart_version>-capacity.<N>` 格式。每次新构建递增 `<N>`——`IfNotPresent` 拉取策略意味着相同 tag 不会被重新拉取。生产环境绝不要用 `latest` 或 `proactive-capacity` 这类静态可变 tag。`VERSION` 绝不要带 `v` 前缀——controller 的 semver 解析器不会剥离它,版本检查会失败。

**为什么用 crane 而不是 docker push**: 在 `kubectl port-forward` 之上跑 `docker push` 极慢且经常超时。`crane` 是轻量级 registry 客户端,会对 blob 去重,在 port-forward 之上工作可靠。项目内可通过 `mise` 使用。

**注意**: Harbor 的 `osdc` 项目若不存在,由 `modules/arc/deploy.sh` 自动创建。

## 部署

```bash
just deploy-module <cluster> arc
```

该命令运行 `modules/arc/deploy.sh`,它会:

1. **确保 Harbor 项目** `osdc` 存在(port-forward 到 Harbor,通过 API 创建,409 = 已存在)
2. **应用 PriorityClasses**,来自 `modules/arc/kubernetes/priority-classes.yaml`(placeholder-runner、arc-runner、placeholder-workflow、arc-workflow)
3. **应用 RBAC**,来自 `modules/arc/kubernetes/capacity-monitor-rbac.yaml`
4. **Helm upgrade** fork chart:
   - Chart: `oci://ghcr.io/jeanschmidt/actions-runner-controller-charts/gha-runner-scale-set-controller`
   - 版本: 来自 `clusters.yaml` 的 `arc.chart_version`(默认 `0.14.1-jeanschmidt.20`)
   - 镜像: 默认 `ghcr.io/jeanschmidt/gha-runner-scale-set-controller:<chart_version>`。本地 Harbor 构建时,用 `clusters.yaml` 中的 `arc.image_repository` / `arc.image_tag` 覆盖。

其他 deploy.sh 配置项(全部来自 `clusters.yaml`):

| 键 | 默认值 | 说明 |
|-----|---------|------|
| `arc.chart_version` | `0.14.1-jeanschmidt.20` | Helm chart 版本(fork) |
| `arc.image_repository` | `ghcr.io/jeanschmidt/gha-runner-scale-set-controller` | Controller 镜像仓库(本地 Harbor 构建时覆盖) |
| `arc.image_tag` | _(chart_version)_ | Controller 镜像 tag(本地 Harbor 构建时覆盖) |
| `arc.replica_count` | `2` | Controller 副本数 |
| `arc.log_level` | `info` | 日志级别 |
| `arc.controller_cpu_request` | `1` | CPU 请求 |
| `arc.controller_cpu_limit` | `4` | CPU 上限 |
| `arc.controller_memory_request` | `2Gi` | 内存请求 |
| `arc.controller_memory_limit` | `4Gi` | 内存上限 |

## 配置

容量监控器通过 listener pod 上的环境变量配置,在 `modules/arc-runners/templates/runner.yaml.tpl` 中设置:

下表的"代码默认值"是环境变量未设置时使用的值。"模板值"是 `runner.yaml.tpl` 实际设置的值——也就是今天生产环境实际运行的值。

| 环境变量 | 代码默认值 | 模板值 | 说明 |
|---------|--------------|----------------|-------------|
| `CAPACITY_AWARE_ENABLED` | `false` | `true` | 启用容量监控器 goroutine |
| `CAPACITY_AWARE_PROACTIVE_CAPACITY` | `0` | `{{PROACTIVE_CAPACITY}}`(来自 runner 定义;当 `clusters.yaml` 设置了 `proactive_capacity_max` 时以其封顶——staging 集群设 `0` 以禁用热池预配置) | 提前于需求维持的占位对数量。硬上限 `1000`(强制截断);超过 `100` 记录警告。 |
| `CAPACITY_AWARE_MAX_BURST_CAPACITY` | `0` | `{{MAX_BURST_CAPACITY}}`(来自 runner 定义) | 限制供给器每周期创建的占位对总数上限(running + pending)。`0` 表示无限制。用于防止突发性节点供给压垮下游服务(Harbor、pypi-cache) |
| `CAPACITY_AWARE_RECALCULATE_INTERVAL` | `30s` | `30s` | 兜底对账间隔(主路径为事件驱动) |
| `CAPACITY_AWARE_REPORT_INTERVAL` | `5s` | _(未设置——用代码默认值)_ | 监控器通过 `X-ScaleSetMaxCapacity` 报告状态的频率 |
| `CAPACITY_AWARE_PLACEHOLDER_TIMEOUT` | `5m` | `20m` | 承担两个角色。**Pending 超时**:创建后任一 pod 处于 Pending 超过此时长,则整对被删除。**生命周期**:每个占位 pod 运行 `sleep <lifetime>`,且 `RestartPolicy: Never`;生命周期按对固定(由 listener pod 和槽位派生,两个 pod 共享)为此值的 1.5 倍到略低于 2 倍(设 `20m` 时即 30m 到略低于 40m),自容器启动起计。到期后,下一个供给周期删除该终结对;若需求仍需要,则同一周期创建替代对。它不是禁用开关:`<= 0` 只会去掉生命周期(`sleep infinity`),此时 Pending 检查会在下一周期删除所有 Pending 对。 |
| `CAPACITY_AWARE_WORKFLOW_CPU` | _(空)_ | `{{VCPU}}`(来自 runner 定义) | 工作流占位的 CPU 请求 |
| `CAPACITY_AWARE_WORKFLOW_MEMORY` | _(空)_ | `{{MEMORY}}`(来自 runner 定义) | 工作流占位的内存请求 |
| `CAPACITY_AWARE_WORKFLOW_GPU` | `0` | `{{GPU_COUNT}}`(来自 runner 定义) | 工作流占位的 GPU 数 |
| `CAPACITY_AWARE_WORKFLOW_DISK` | _(空)_ | `{{DISK_SIZE}}`(来自 runner 定义) | 工作流占位的磁盘 |
| `CAPACITY_AWARE_RUNNER_CPU` | `750m` | `750m` | Runner 占位的 CPU 请求——**必须与 runner 容器实际的 CPU 请求/上限一致** |
| `CAPACITY_AWARE_RUNNER_MEMORY` | `512Mi` | `1Gi` | Runner 占位的内存请求——**必须与 runner 容器实际的内存请求/上限一致**。模板值(`1Gi`)是生效值;代码默认值(`512Mi`)会预留不足,破坏拓扑保证。 |
| `CAPACITY_AWARE_NODE_FLEET` | _(空)_ | `{{NODE_FLEET}}`(来自 runner 定义) | 占位 workflow 调度使用的节点 fleet(workflow 池,按 scale set) |
| `CAPACITY_AWARE_RUNNER_NODE_FLEET` | _(空)_ | `c7i-runner` | **`CAPACITY_AWARE_ENABLED=true` 时必填**——占位 runner 调度使用的集群级 runner 池 fleet(当前为 `c7i-runner`)。缺失时 `Validate()` 返回错误。与上文的 `CAPACITY_AWARE_NODE_FLEET` 不同:runner 占位始终落在集群级 runner 池,而 workflow 占位落在按 scale set 的 workflow 池。 |
| `CAPACITY_AWARE_HUD_FAILURE_MULTIPLIER` | `3` | `1`(`runner.yaml.tpl` 中硬编码) | 当 HUD API 不可达时,容量监控器将占位超配到 `ProactiveCapacity * multiplier + HUDFailureBaseCapacity`。外层上限(`MaxRunners` 余量、`MaxBurstCapacity`)约束绝对爆炸半径。下限截断为 `1`。 |
| `CAPACITY_AWARE_HUD_FAILURE_BASE_CAPACITY` | `0` | `{{HUD_FAILURE_BASE_CAPACITY}}`(来自 runner 定义;默认 `0`) | 加到 HUD 失败兜底公式中的加性基线:`desiredPairs = ProactiveCapacity * HUDFailureMultiplier + HUDFailureBaseCapacity`。让运维能以 `proactive_capacity: 0` 运行,在 HUD API 不可达时仍激增固定底线的对数。截断到 `[0, 1000]`;超过 `100` 记录警告。 |
| `CAPACITY_AWARE_RUNNER_CLASS` | _(空)_ | `{{RUNNER_CLASS}}`(来自 runner 定义) | 占位节点选择器使用的 runner class |
| `CAPACITY_AWARE_HUD_API_URL` | _(内置默认 URL)_ | 硬编码的 PyTorch HUD `queued_jobs_aggregate` URL | 获取排队任务数的 HUD 端点 |
| `CAPACITY_AWARE_HUD_API_TOKEN` | _(空)_ | 来自 K8s secret `pytorch-hud-token`(可选挂载) | 获取排队任务数用的 PyTorch HUD API token |
| `CAPACITY_AWARE_CLUSTER_INDEX` | `0` | 由 `generate_runners.py` 从 `clusters.yaml` 自动推导 | 本集群在 peer 列表中的位置(从 0 起)。peer 指部署同一 `arc-runners` 族模块且声明相同 `runner_name_prefix` 的集群。 |
| `CAPACITY_AWARE_CLUSTER_COUNT` | `1` | 由 `generate_runners.py` 从 `clusters.yaml` 自动推导 | 服务相同 runner 标签的 peer 集群数量。`1` 表示禁用分片(单集群行为)。 |
| `CAPACITY_AWARE_AGE_THRESHOLD_SECONDS` | `900`(15 分钟) | 来自 `clusters.yaml` 的 `arc-runners.capacity_aware_age_threshold_seconds`(默认 `900`) | 分片切片的新鲜任务阈值。比该值新的任务在 peer 间切片;更老的任务由所有 peer 认领(故障转移信号)。`0` 完全禁用分片。低于 60 秒的值截断为 `60`。 |
| `CAPACITY_AWARE_FRESH_MULTIPLIER` | `1.0` | def `fresh_multiplier` → 集群 `capacity_aware_fresh_multiplier` → 1.0 | 调节每个 peer 的新鲜队列切片。`0.5` = 只取公平份额的一半(更省,队列消化更慢)。`2.0` = 加倍认领(启动更快,存在冗余)。 |

目前对所有 runner 启用(`CAPACITY_AWARE_ENABLED=true` 在模板中硬编码)。注意:对在 `clusters.yaml` 中设置 `proactive_capacity_max: N` 的集群,`generate_runners.py` 会把 `proactive_capacity` 封顶到 `N`——每个 scale set 渲染为 `min(def_proactive_capacity, N)`。staging 集群设置 `proactive_capacity_max: 0`,因此那里不预配置任何占位——只为在途任务创建按需对。其他集群不设置封顶,直接使用 def 值。

### 多集群 HUD 队列分片

当 N 个集群服务同一 runner 标签时,每个 listener 认领 `1/N` 的新鲜排队任务(按集群索引以稳定的余数分布取整),外加 100% 的早于 `AgeThresholdSeconds` 的任务。年龄层即故障转移信号:健康的 peer 会快速消化自己的份额,任务不会老化;挂掉的 peer 的份额会老化,由存活的 peer 吸收。不需要任何跨集群协调。`CAPACITY_AWARE_CLUSTER_INDEX` 和 `CAPACITY_AWARE_CLUSTER_COUNT` 由 `generate_runners.py` 从 `clusters.yaml` 中部署同一 `arc-runners` 族模块且声明相同 `runner_name_prefix` 的集群集合自动推导。每个 peer 的份额可以按 runner 定义调整(`fresh_multiplier`),也可按集群全局调整(`capacity_aware_fresh_multiplier`)——默认 1.0 保持原始分片行为。

## 创建 HUD API Secret

```bash
kubectl create secret generic pytorch-hud-token \
  --namespace arc-systems \
  --from-literal=token='<hud-internal-bot-secret>'
```

模板以可选方式挂载该 secret(`optional: true`),因此缺少 secret 不会阻止 pod 启动。

## 为 Runner 定义添加 maxRunners

在 runner 定义 YAML 中添加 `max_runners: <value>`。大多数现有定义还设置了 `proactive_capacity` 和 `max_burst_capacity`——除非你明确要退出主动容量,否则一并复制。示例(虚构的定义,仅作演示):

```yaml
runner:
  name: l-x86example-8-16
  instance_type: c7a.48xlarge
  vcpu: 8
  memory: 16Gi
  gpu: 0
  disk_size: 150
  max_runners: 100
  proactive_capacity: 30
  max_burst_capacity: 2000
```

该值经模板流转为生成的 Helm values 中的 `maxRunners:`(chart 的标准伸缩字段——`runner.yaml.tpl:73` 处的 `gha_max_runners` 是无关的 Prometheus 指标)。容量监控器读取 `config.MaxRunners`(由 listener 配置设置),并将其用作 `X-ScaleSetMaxCapacity` 的上限。不设 `max_runners` 时,该值为空/无限制。

H100(`modules/arc-runners-h100/defs/`)和 B200(`modules/arc-runners-b200/defs/`)runner 全部设置了 `max_runners`,按 `(预留 GPU 数 / 每 runner GPU 数)` 计算:

- H100 有 1 个预留的 8-GPU 节点(共 8 块 GPU)。各分片值:1× / 2× / 4× / 8× 分片分别为 `8 / 4 / 2 / 1`。
- B200 有 2 个预留的 8-GPU 节点(共 16 块 GPU)。各分片值:1× / 2× / 4× / 8× 分片分别为 `16 / 8 / 4 / 2`。

CPU/T4/A10G/L4/A100 的 runner 定义目前未设置 `max_runners`。

## 容量监控器压测

在 meta-staging-aws-uw1 上验证容量监控器与 HUD 集成:

```bash
just load-test meta-staging-aws-uw1 --label l-x86iamx-8-16:400
```

**为什么是 `--label l-x86iamx-8-16:400`**: meta-staging-aws-uw1 运行在 us-west-1,该区域没有 `c7a` 实例。默认分布会把大多数任务分配给 `l-x86iavx512-8-16`(node-fleet `c7a`),它永远不会被调度。`l-x86iamx-8-16` 使用 node-fleet `c7i`,在 us-west-1 可用。

要并行测试多种 runner 类型(如 CPU + GPU),重复 `--label`:

```bash
just load-test meta-staging-aws-uw1 --label l-x86iamx-8-16:400 --label l-x86iavx512-29-115-t4:200
```

**us-west-1 的 GPU 标签**: g5 (A10G) 和 g6 (L4) fleet 配置了 `exclude_regions: [us-west-1]`。只有 g4dn (T4) 可用——从 `l-x86iavx512-29-115-t4`(1×T4)、`l-x86iavx512-45-172-t4-4`(4×T4)或 `l-bx86iavx512-94-344-t4-8`(8×T4,裸金属)中选择。被排除实例类型的 scale set 仍会部署,但渲染为 `maxRunners: 0`,所以在 `--label` 里选它们等于空操作。

`--label` 和 `--jobs` 互斥。不指定 `--label` 时,`--jobs N` 会按比例分配到所有可用 runner 类型。

**验证容量监控器是否正常工作**:

```bash
# 查看 listener 启动日志中的 "Capacity monitor enabled" 和 "Starting capacity monitor"
kubectl logs -n arc-systems <listener-pod> | head -20

# 查看对账日志(每 30 秒一次)
kubectl logs -n arc-systems <listener-pod> | grep "capacity reconciled"

# 直接测试 HUD API
curl -s 'https://hud.pytorch.org/api/clickhouse/queued_jobs_aggregate?parameters=%7B%22queuedThresholdMinutes%22%3A0%2C%22maxAgeDays%22%3A3%2C%22orgs%22%3A%5B%22pytorch%22%5D%2C%22repo%22%3A%22%22%7D' | python3 -m json.tool | head -20
```

如果尽管设置了 `CAPACITY_AWARE_ENABLED=true` 容量监控器仍未启动,controller 镜像很可能需要重新构建——正在运行的二进制可能早于容量监控器代码。

## 维护

进行 ARC 升级时:
1. 检查 `cmd/ghalistener/main.go` 是否有变更(入口接线——我们的改动在容量监控器 errgroup 块中)
2. 检查 `github.com/actions/scaleset/listener` 是否有变更(特别是 `listener.SetMaxRunners()` 和 `listener.Config`——该子包是我们依赖的 API 面)
3. 将 fork 的 `master` 分支 rebase 到上游 master
4. 重新构建镜像并推送到 Harbor

`capacity/` 包完全属于我们——不可能产生上游合并冲突。不过 fork 的改动面比 `main.go` 和 `capacity/` 更广:

- `cmd/ghalistener/main.go` — 修改以将容量监控器接入 errgroup
- `cmd/ghalistener/main_test.go` — 新增;测试 listener 的初始 `MaxRunners` 种子值(容量监控器启用时为 0)
- `cmd/ghalistener/capacity/` — 全新包(文件与规模见本文档顶部"变更内容")
- `cmd/ghalistener/metrics/` — `metrics.go` 与 `metrics_test.go` 中的容量监控器指标、重新生成的 `mocks_test.go`,以及新增的 `testdata/listener_metrics.yaml`
- `Dockerfile` — `LABEL org.opencontainers.image.source` 被改写为指向 fork
- `charts/*/Chart.yaml` — 每次 fork 发布时,`version` 和 `appVersion` 递增为 `<upstream>-jeanschmidt.<N>`
- `charts/gha-runner-scale-set-controller/values.yaml` — `image.repository` 默认为 `ghcr.io/jeanschmidt/gha-runner-scale-set-controller`;该行归上游所有,与 `Chart.yaml` 一样,rebase 时可能冲突
- `charts/gha-runner-scale-set/` — `templates/_helpers.tpl`、`values.yaml` 和 `tests/template_test.go`,用于支持可选的 `resourceName` 值
- `go.mod` — `github.com/prometheus/client_model` 从间接依赖改为直接依赖
- `.gitignore` — 忽略本地构建的 `ghalistener` 二进制,同时保持 `cmd/ghalistener/` 被跟踪
- 各特性分支上的其他提交(如 "Drop runner-class from runner placeholders"、"Split runner/workflow placeholder fleets"、"Require runner-class in workflow affinity")会触及 `capacity/` 之外的更多文件

## 陈旧 Listener 恢复

### 症状

- `arc-systems` 中的 listener pod(名为 `<runner-name>-<hash>-listener`)卡在 `CrashLoopBackOff`
- Pod 日志以 `Application returned an error: failed to create actions message session client ... 404 Not Found ... RunnerScaleSetNotFoundException` 结尾
- `modules/arc-runners/deploy.sh` 的部署后检查会打印 WARNING 横幅,列出受影响的 AutoscalingRunnerSets

### 根因

AutoscalingRunnerSet 注解 `runner-scale-set-id` 上盖章的 scale-set ID 已不存在于 GitHub 的 broker。controller 不会对照 broker 验证该注解——它信任任何语法有效的 ID。重启 listener pod 或 controller 都无济于事;只有重新注册才行。

常见触发原因:

- GitHub App 安装失去对仓库的访问权限(权限轮换、scope 变更)
- 仓库被重命名、归档或迁移
- 管理员通过 GitHub 界面或 API 手动删除了 scale sets
- broker 侧密钥轮换导致现有 scale-set ID 失效

### 恢复前——先验证 GitHub App 仍有访问权限

```bash
gh api /repos/<org>/<repo> | jq '{name, archived, full_name}'
gh api /app/installations | jq '.[] | select(.id == <installation-id>)'
```

如果 App 已失去访问权限,先修复它——否则 `CreateRunnerScaleSet` 同样会 404,恢复无法进行。

### 恢复

首先,在不触碰集群的前提下检查漂移(发现任何漂移则以非零码退出——可安全地由 CI/cron 运行):

```bash
just heal-arc-check <cluster>
```

恢复单个 ARS(爆炸半径最小——从这里开始验证):

```bash
just heal-arc <cluster> <ars-name>
```

恢复集群上所有已漂移的对象(用于 broker 密钥轮换等大范围事件):

```bash
just heal-arc <cluster>
```

`heal-arc` 会检测五类漂移,并按顺序逐类恢复:

1. **Controller 漂移** — `arc-systems` 中的 `arc-gha-rs-controller` Deployment 缺失,或就绪副本少于期望。恢复:执行 `kubectl rollout restart` + `rollout status`,超时 5 分钟。若 controller 无法恢复,则整个流程中止(无法进行下游 reconcile)。
2. **ARS 漂移** — AutoscalingRunnerSet 上缺少 `runner-scale-set-id` 注解。恢复:保持注解为清除状态并删除对应的 AutoscalingListener;controller 随后向 GitHub broker 调用 `CreateRunnerScaleSet`,盖入新 ID,并创建新的 listener pod。所有后代 EphemeralRunnerSets 进入级联删除队列(重新注册后它们会持有旧 ID)。
3. **Listener 漂移** — AutoscalingListener 缺失,或其 `spec.runnerScaleSetId` 与 ARS 规范 ID 不匹配,或其 pod 不处于 Running/Ready(CrashLoopBackOff、ImagePullBackOff 等)。恢复:删除 listener;controller 使用当前 ARS 注解重建它。
4. **Listener 孤儿漂移** — AutoscalingListener 的 `spec.ephemeralRunnerSetName` 引用了一个已不存在的 EphemeralRunnerSet。listener 的 spec 仅在创建时设置一次,不会跟踪 ERS 重命名,因此每当一个 ERS 被删除且 controller 用新的随机后缀重建它时,现有 listener 的 config secret 里仍嵌入旧的(已消亡的)ERS 名称,并因试图 patch 不存在的 ERS 而 crashloop。恢复:删除 listener;controller 会以当前 ERS 名称嵌入重建它。
5. **ERS 漂移** — `EphemeralRunnerSet.spec.ephemeralRunnerSpec.runnerScaleSetId` 与父 ARS 的规范 ID 不匹配。恢复:删除该 EphemeralRunnerSet(级联删除其 EphemeralRunners);父 ARS controller 会用当前 ID 创建新的 ERS。

**Listener/ERS 级联**: `AutoscalingListener` 和 `EphemeralRunnerSet` 是一对耦合体——每当一个 ERS 被删除(无论是因 `ers_drift`,还是作为 `ers_cascade_from_ars` 队列的一部分),父 ARS 的 listener 也会进入删除队列,以便 controller 能用一致的名称引用把它们一起重建。listener 先删除(比 ERS 删除快,还能避免现有 listener pod 在新 ERS 创建期间 crashloop)。删除操作以短暂间隔节流执行,防止 controller 的 reconcile 突发耗尽 listener 所在节点上的 AWS CNI IP 分配。

恢复完成后,`heal-arc` 会短暂休眠以让 controller 稳定,然后重新运行一次漂移报告。若 drift_count 为零,则报告恢复完成;否则打印剩余漂移,并以非零码退出。

`OSDC_CONFIRM=yes|no|ask` 门控适用于 `heal-arc`(默认 `ask`)。`heal-arc-check` 从不提示。

### 观察恢复过程

```bash
kubectl get autoscalinglisteners -n arc-systems -w
```

新的 listener pod 应在 30-60 秒内进入 `Running`。验证新的 scale-set ID:

```bash
kubectl get autoscalingrunnersets -n arc-runners -o json \
  | jq '.items[] | {name: .metadata.name, scaleSetId: .metadata.annotations["runner-scale-set-id"]}'
```

### 退出部署后检查

在快速迭代、觉得该检查是噪音时:

```bash
ARC_LISTENER_CHECK=skip just deploy-module <cluster> arc-runners
```

等待窗口可通过 `ARC_LISTENER_CHECK_TIMEOUT` 配置(单位秒,默认 60)。该检查每 5 秒轮询一次,一旦每个 listener 都稳定 Running 或 restartCount >= 2,即提前结束。
