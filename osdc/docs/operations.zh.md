# 运维指南

## 前置条件

- AWS CLI 已配置好凭据，且该 IAM 主体映射到目标集群 `access_config.cluster_admin_role_names` 中的某个角色（例如 `meta-staging-aws-uw1` 对应 `osdc_gha_staging`，`meta-prod-aws-ue2` 对应 `osdc_gha_prod`）。至于凭据如何获取（SSO、角色代入、静态密钥）由操作者所在组织自行决定——本项目不指定 profile 或登录流程。
- 已安装 `mise`（[mise.jdx.dev](https://mise.jdx.dev)）
- 已安装 `just`（不由 `mise` 管理——需单独安装）
- Docker 且已启用 **BuildKit**——本地部署会构建容器镜像（例如 `image-cache-janitor`），依赖 BuildKit；见下文 [Docker / BuildKit（本地构建）](#docker--buildkit本地构建)
- 工作目录：`osdc/`

`mise` 会在首次运行时自动安装所有其他工具（tofu、kubectl、helm、crane、awscli、packer、`uv`，以及各类 linter）。先运行一次 `just setup` 创建 Python 虚拟环境（`uv sync`），供 `cluster-config.py` 和其他辅助脚本使用。

`mise` 还会为每个以项目为根的 shell 导出 `AWS_REGION=us-west-2`。这是状态桶所在区域（state 和锁表始终位于 `us-west-2`），也是 `aws` 命令的默认区域；对其他区域的集群执行临时 `aws` 命令时，需显式覆盖 `AWS_REGION`（例如 `meta-prod-aws-ue2` 位于 `us-east-2`）。

### 企业代理（Meta 及类似环境）

在未将 `*.eks.amazonaws.com` 加入白名单的 HTTP 代理环境中，`aws eks ...`、`kubectl` 和 `helm` 调用会挂起或超时。请扩展绕过列表：

```bash
export NO_PROXY="${NO_PROXY:-},.eks.amazonaws.com"
export no_proxy="${no_proxy:-},.eks.amazonaws.com"
```

每个涉及 EKS API 的 `just` recipe 都会自动内联这个绕过设置。上面的 export 仅在直接调用 `aws`、`kubectl` 或 `helm` 时才需要——例如在下面的只读调试会话中。

### Docker / BuildKit（本地构建）

`just deploy` / `just deploy-base` 会在本地构建容器镜像（例如 `image-cache-janitor`），同时覆盖 `amd64` 和 `arm64`。这些构建**依赖 BuildKit**：Dockerfile 使用了 `TARGETARCH` 构建参数，只有 BuildKit 会自动填充它。在旧版构建器下 `TARGETARCH` 为空，架构相关的下载（crictl 等）会解析成格式错误的 URL 并以 HTTP 404 失败。

CI 上有 BuildKit；全新的本地 Docker（尤其是 Apple Silicon 上的 Colima）通常没有。接线方法：

```bash
# 1. 安装 buildx CLI 插件（Homebrew 示例）
brew install docker-buildx

# 2. 注册到 Docker CLI 查找插件的目录
mkdir -p ~/.docker/cli-plugins
ln -sfn "$(brew --prefix)/bin/docker-buildx" ~/.docker/cli-plugins/docker-buildx
docker buildx version   # 验证

# 3. 让 BuildKit 成为普通 `docker build` 的默认构建器（deploy.sh 直接使用它）
export DOCKER_BUILDKIT=1   # 加入 shell profile，或运行一次 `docker buildx install`
```

在 Apple Silicon 上，每次构建的 `amd64` 部分在模拟下运行。使用 Colima 时由 **Rosetta** 处理——无需手动注册 binfmt——前提是 VM 使用 `vmType: vz` 且 `rosetta: true`（例如 `colima start --vm-type vz --vz-rosetta`），并且主机上已安装 Rosetta：

```bash
softwareupdate --install-rosetta --agree-to-license
```

Colima 随后会自动在 VM 中注册 Rosetta handler；你**不需要** `docker run --privileged tonistiigi/binfmt --install all`（那会安装更慢的 QEMU handler）。

BuildKit 构建会显示 `#5` / `#6 DONE` 风格的步骤输出；旧版构建器显示 `Step N/M` / `Running in <hash>`，外加一条 `The requested image's platform ... does not match the detected host platform` 警告。该平台警告本身无害——真正的故障是空的 `TARGETARCH`。

## 首次设置

### 1. 引导状态存储

每个集群需要一个存放 tofu state 的 S3 桶和一个共享的 DynamoDB 锁表。

```bash
# 单个集群
just bootstrap meta-staging-aws-uw1

# clusters.yaml 中定义的所有集群
just bootstrap-all
```

幂等——可安全重复运行。

### 2. 部署集群

```bash
# 完整部署（base + 所有模块）
just deploy meta-staging-aws-uw1

# 或分步执行
just deploy-base meta-staging-aws-uw1
just deploy-module meta-staging-aws-uw1 karpenter
just deploy-module meta-staging-aws-uw1 arc
just deploy-module meta-staging-aws-uw1 nodepools
just deploy-module meta-staging-aws-uw1 arc-runners
just deploy-module meta-staging-aws-uw1 buildkit
just deploy-module meta-staging-aws-uw1 pypi-cache
just deploy-module meta-staging-aws-uw1 cache-enforcer
just deploy-module meta-staging-aws-uw1 zombie-cleanup
just deploy-module meta-staging-aws-uw1 harbor-cache-recovery
just deploy-module meta-staging-aws-uw1 monitoring
just deploy-module meta-staging-aws-uw1 logging
```

完整的每集群模块列表位于 `clusters.yaml` 的 `clusters.<id>.modules` 下。生产集群会增加 GPU 池（`nodepools-h100`、`nodepools-b200`）和对应的 runner（`arc-runners-h100`、`arc-runners-b200`）。

## 日常运维

模块契约（目录布局、部署阶段、新增模块）见 [`modules.md`](modules.md)。

### 部署指定模块

```bash
just deploy-module meta-staging-aws-uw1 nodepools
just deploy-module meta-staging-aws-uw1 arc-runners

# 即使 release 看起来未变化也强制执行 Helm upgrade
just deploy-module meta-staging-aws-uw1 arc force
```

### 预览变更而不应用

`just plan <cluster>` 对 base 和所有 terraform 后端模块运行 `tofu plan`。只读：不 apply，无 Kubernetes 副作用，可在 CI 中对 PR 安全运行。

```bash
just plan meta-staging-aws-uw1
```

### 查看集群状态

```bash
just show meta-staging-aws-uw1        # 配置、模块、tofu 变量
just list                    # 所有集群及其模块
```

### 查看已部署内容

部署命令会在 `osdc-system` 命名空间的 ConfigMap 中写入审计日志。

```bash
just deploy-history meta-staging-aws-uw1              # 部署记录列表（从旧到新）
just deploy-status meta-staging-aws-uw1               # 当前版本 + 近期历史（全部）
just deploy-status meta-staging-aws-uw1 arc-runners   # 收窄到单个模块/命令
```

### 部署后验证

```bash
just smoke meta-staging-aws-uw1              # 基于 pytest 的冒烟套件（按模块）
just integration-test meta-staging-aws-uw1   # 完整集成套件
just load-test meta-staging-aws-uw1          # 合成负载
just workload-test meta-staging-aws-uw1      # 生产工作负载回放
```

`just deploy` 本身会在最后提示运行 smoke（设置 `OSDC_SMOKE=no` 可跳过）。

### Runner 平滑刷新

在不中断进行中 CI 作业的情况下滚动更新 runner 配置或 AMI：

```bash
just drain-runners meta-staging-aws-uw1      # 将 maxRunners 补丁为 0、给节点加污点、等待进行中的 pod
just resume-runners meta-staging-aws-uw1     # 从 def 文件恢复 maxRunners，移除污点
```

`drain-runners` 最多等待 `OSDC_DRAIN_TIMEOUT_SECS`（默认 `3600`）让 runner pod 完成，之后列出仍未结束的残留任务。默认 1 小时与典型 PyTorch 套件时长匹配——测试窗口更长时请调大该值。

`taint-nodes` / `untaint-nodes` 为每个带有 `workload-type=github-runner` 标签的节点添加或移除 `deploy.osdc.io/refresh-pending=true:NoSchedule` 污点——适用于想把节点标记为过期、但不想同时排空 AutoscalingRunnerSets 的场景。

`recycle-nodes` 删除所有 Karpenter `NodeClaims`，迫使 Karpenter 按需重新供给。适用于 AMI/userData 变更后、且无需保留进行中作业的场景。Staging 会在 `just deploy` 结束时自动执行（由 `clusters.yaml` 中的 `recycle_karpenter_nodes: true` 驱动）。

### 只读调试

```bash
kubectl get nodes
kubectl get pods -A
kubectl get nodepools                                # 需要 karpenter 模块
kubectl get autoscalingrunnersets -n arc-runners
helm list -A
```

### `just deploy` 的环境变量

设置这些变量可抑制 `just deploy` 中的交互式提示——CI/自动化场景必需：

| 变量 | 取值 | 默认 | 作用 |
|------|------|------|------|
| `OSDC_CONFIRM` | `yes` / `no` / `ask` | `ask` | 在应用完整部署和每个 tofu plan 前确认。`no` 立即取消。 |
| `OSDC_TAINT_NODES` | `yes` / `no` / `ask` | `ask` | 部署完成后为 ARC runner 节点加污点（节点将被回收时跳过）。 |
| `OSDC_SMOKE` | `yes` / `no` / `ask` | `ask` | 部署后运行 `just smoke <cluster>`。 |
| `OSDC_DRAIN_TIMEOUT_SECS` | 秒 | `3600` | `just drain-runners` 在列出残留任务前等待进行中 runner pod 的时长。 |
| `OSDC_UNTAINT_NODES` | `yes` / `no` | `yes` | `just resume-runners` 结束时移除 runner 节点上的刷新污点。设为 `no` 可保留污点，留待单独的 untaint 步骤。 |

## 新增集群

1. 选定集群 ID、区域、VPC CIDR，并决定启用哪些模块。VPC CIDR 只决定 IPv4 的规模（节点、NAT、ENI 主 IP）——pod IP 来自 AWS 分配的 /56 IPv6 块，因此 `/16` 对超大规模集群也绰绰有余。模块名必须匹配 `modules/` 下的某个目录——`clusters.yaml` 中现有的 `meta-staging-aws-uw1` 和 `meta-prod-aws-ue2` 条目是完整可用的示例。最小骨架：

   ```yaml
   # clusters.yaml
   clusters:
     my-new-cluster:
       region: us-east-1
       cluster_name: pytorch-my-new-cluster
       state_bucket: ciforge-tfstate-my-new-cluster
       base:
         vpc_cidr: "10.2.0.0/16"
         base_node_count: 3
       modules:
         - karpenter
         - arc
         - nodepools
         - arc-runners
         - buildkit
   ```

2. **引导状态。** 创建 tfstate 桶和锁表。
   ```bash
   just bootstrap my-new-cluster
   ```

3. **隐身部署。** 在集群上设置 `pause_runners: true`——这是顶层键，不在 `arc-runners` 下——使每个 scale set 渲染为 `maxRunners: 0`。集群会向 GitHub 注册但不接任务。
   ```bash
   just deploy-base my-new-cluster
   ./create-osdc-secrets.sh my-new-cluster
   just deploy my-new-cluster
   ```

4. **验证。** Scale set 为 `RUNNING`，org 的 runner-group 页面上监听器心跳健康，然后：
   ```bash
   just smoke my-new-cluster
   ```
   暂停状态下"派发真实作业"的检查无法通过——那是第 6 步。

5. **填充 HF 缓存。** 桶初始为空且以只读方式挂载，因此在缓存填充之前，*所有*模型下载都会失败，报 `OSError: [Errno 30] Read-only file system`。用 [`hf_cache_sync.py`](https://github.com/pytorch/test-infra/blob/main/tools/scripts/hf_cache_sync.py) 从成熟区域镜像：
   ```bash
   python3 hf_cache_sync.py --mirror --source meta-prod-aws-ue1 --to my-new-cluster --apply
   ```

6. **接入流量。** 撤销本地 `pause_runners` 修改，然后为该集群派发 **OSDC: Deploy production**——不是本地部署。集群必须接入 [`osdc-deploy-prod.yml`](../../.github/workflows/osdc-deploy-prod.yml) 才会出现在下拉菜单中。

7. **检查 Grafana。** 新集群应出现在 **OSDC**、**OSDC Services Status** 和 **OSDC Cluster Utilization** 中——见 [`grafana/`](../../grafana/)。后两个仪表盘默认使用其 JSON 中保存的 `cluster` 选择，不会自动纳入新集群：需要在 [`osdc_services_status.json`](../../grafana/osdc_services_status.json) 和 [`osdc_utilization.json`](../../grafana/osdc_utilization.json) 中把新集群的 `cluster_name` 加进该变量的 `current.text` 和 `current.value`。**OSDC** 会自动识别名为 `meta-prod-aws-*` 或 `lf-prod-aws-*` 的集群，但其 ClickHouse 查询硬编码了 `runner_group_name` → `cluster` 的映射；按 [`grafana/AGENTS.md`](../../grafana/AGENTS.md) 的说明更新。Grafana UI 的保存对话框会拒绝这些仪表盘，每次触及 [`grafana/`](../../grafana/) 的 main 分支合并都会从 JSON 重新发布它们。

## 集群生命周期

关于销毁并重建现有集群（例如 IPv4 → IPv6 迁移，因为 EKS 的 `ip_family` 在创建后不可变），见 [`ipv6-cluster-recreation.md`](ipv6-cluster-recreation.md)。

## 新增模块

完整指南见 [modules.md](modules.md)。简版：

1. `mkdir -p modules/mymodule`
2. 添加 `deploy.sh`（可选 `terraform/`、`kubernetes/`）
3. 在 `clusters.yaml` 中目标集群的 `modules` 列表里加上模块名
4. `just deploy-module <cluster> mymodule`

## 新增 runner 类型

需要两个模块的定义：`nodepools`（计算）和 `arc-runners`（ARC scale set）。

1. 确保 `modules/nodepools/defs/` 下存在覆盖该实例类型的 NodePool fleet。文件按 **fleet** 命名（例如 `c7i.yaml`、`m7i.yaml`、`g5.yaml`），而不是按单个实例类型。fleet 包含有序的 `instances:` 列表，Karpenter 会挑选最合适的：

   ```yaml
   # modules/nodepools/defs/c7i.yaml
   fleet:
     name: c7i
     arch: amd64
     gpu: false
     instances:
       - type: c7i.48xlarge
         weight: 100
         node_disk_size: 3750
       - type: c7i.24xlarge
         weight: 85
         node_disk_size: 1900
       - type: c7i.12xlarge
         weight: 40
         node_disk_size: 1350
       - type: c7i.8xlarge
         weight: 20
         node_disk_size: 650
   ```

2. 在 `modules/arc-runners/defs/<name>.yaml` 中创建 runner 定义。命名遵循 [`runner_naming_convention.md`](runner_naming_convention.md) 的约定（`l-<arch><vendor><features>-<vcpu>-<memory>[-<gpu>[-<count>]]`）。`proactive_capacity` 和 `max_burst_capacity` 均为必填：

   ```yaml
   # modules/arc-runners/defs/l-x86iamx-8-16.yaml
   runner:
     name: l-x86iamx-8-16
     instance_type: c7i.12xlarge   # 必须是某个 NodePool fleet 中的实例类型
     disk_size: 150
     vcpu: 8
     memory: 16Gi
     gpu: 0
     proactive_capacity: 30        # 保持热备的最小 runner 数
     max_burst_capacity: 2000      # ARC 扩容上限
   ```

3. 部署：
   ```bash
   just deploy-module meta-staging-aws-uw1 nodepools
   just deploy-module meta-staging-aws-uw1 arc-runners
   ```

## 新增缓存的 git 仓库

git 缓存采用两层架构：中央 StatefulSet 从 GitHub 克隆仓库并通过 rsync 提供服务，每个节点上的 DaemonSet 负责本地同步。

1. 编辑 `base/kubernetes/git-cache/central-configmap.yaml` 中 `central.py` 脚本内的相应列表。有**两个**列表——选对：
   - `REPOS_FULL` —— 带子模块的仓库。以 `--recurse-submodules` 非裸克隆，使 `.git/modules/<name>/objects/` 可用于 `actions/checkout` 的 submodule alternates。目前：`pytorch/pytorch`。
   - `REPOS_BARE` —— 不带子模块的仓库。裸克隆（轻量）。目前：`pytorch/test-infra`。

   把带子模块的仓库加进 `REPOS_BARE` 会破坏 checkout——拿不准就用 `REPOS_FULL`。

2. 重新部署：
   ```bash
   just deploy-base meta-staging-aws-uw1
   ```

注意：Runner pod 使用 `CHECKOUT_GIT_CACHE_DIR`（而非 `GIT_ALTERNATE_OBJECT_DIRECTORIES`）定位缓存。`actions/checkout` action 使用 `reference-repository` 来利用缓存。新增仓库无需修改 runner 模板。

## Terraform 状态管理

状态存于 S3，用 DynamoDB 加锁：

```
s3://ciforge-tfstate-<cluster-id>/<cluster-id>/base/terraform.tfstate
s3://ciforge-tfstate-<cluster-id>/<cluster-id>/<module>/terraform.tfstate
```

无论集群自身区域在哪，状态桶和锁表始终位于 `us-west-2`——这是硬编码常量（`scripts/bootstrap-state.sh` 和 justfile 中的 `STATE_REGION`）。初始化 backend 时不要修改 `region=us-west-2` 参数，即使集群在其他区域。

查看状态：
```bash
cd modules/eks/terraform
tofu init -reconfigure \
    -backend-config="bucket=ciforge-tfstate-meta-staging-aws-uw1" \
    -backend-config="key=meta-staging-aws-uw1/base/terraform.tfstate" \
    -backend-config="region=us-west-2" \
    -backend-config="dynamodb_table=ciforge-terraform-locks"
tofu state list
tofu output
```
