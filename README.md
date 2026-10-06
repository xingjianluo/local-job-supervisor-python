# local-job-supervisor-python

基于 FastAPI 的本地并发作业执行子系统：用本地子进程与本地 JSON 状态文件模拟多类短时作业，
使并发提交、取消和重启恢复后的执行结果都可预测且可审计。所有验证只依赖本地环境，
不依赖外部服务、真实凭据或网络访问。

## 作业状态模型

```
                 ┌─────────┐   调度获得容量   ┌─────────┐
   submit ─────► │ QUEUED  │ ───────────────► │ RUNNING │
                 └────┬────┘                  └────┬────┘
                      │ cancel                     │ 全部子进程退出码为 0
                      ▼                            ▼
                ┌───────────┐   cancel/超时/非零退出/启动失败   终态：
                │ CANCELLED │ ◄──────────────┐   SUCCEEDED / FAILED / TIMEOUT / CANCELLED
                └───────────┘                ▼
                                    FAILED / TIMEOUT / CANCELLED
```

- 合法迁移只允许：`QUEUED→RUNNING|CANCELLED`，`RUNNING→SUCCEEDED|FAILED|TIMEOUT|CANCELLED`；
  终态不可再迁移（迁移表见 `app/models.py: ALLOWED_TRANSITIONS`，非法迁移直接抛错）。
- 每个终态都带可追溯原因（`reason` 字段）：
  `completed` / `exit_code` / `timeout` / `cancelled` / `spawn_failed` /
  `supervisor_restart` / `cleanup_failed`。
- 每次迁移都写日志（`job=<id> transition <old> -> <new> reason=<...>`），
  提交时打印输入约束（duration/memory/procs/timeout）。

## 资源限制语义

全局限制见 `app/models.py: Limits`（默认值可用环境或构造参数调整）：

| 限制 | 默认 | 语义 |
| --- | --- | --- |
| `max_concurrent` | 4 | 同时处于 RUNNING 的作业数上限 |
| `max_memory_mb` | 1024 | 所有运行中作业 `memory_mb` 预留量之和的上限（记账式模拟，非真实 RSS） |
| `max_procs_per_job` | 8 | 单作业派生子进程数上限 |
| `max_queue_size` | 64 | 排队中作业数上限 |

提交即校验，拒绝时返回 HTTP 422，`detail.reason` 为可区分的原因码：

- `invalid_params`：时长/内存/进程数非正、timeout 非正、exit_code 越界；
- `memory_exceeds_limit`：`memory_mb` 超过全局内存预算（单作业即不可能运行）；
- `process_count_exceeds_limit`：`procs` 超过单作业上限；
- `queue_full`：排队已满。

被拒绝的作业不会留下任何记录。运行中的作业只有在并发数与内存预算同时满足时才会被调度，
突发并发不会突破上限。

## 作业执行与取消（强语义）

- 运行一个作业 = 派生 `procs` 个本地子进程（`python -I -c "sleep(duration); exit(exit_code)"`），
  用 `exit_code` 模拟异常退出，用 `timeout_ms` 触发超时；提交时传 `ignore_sigterm=true`
  可让子进程忽略优雅终止信号，用于演练强制终止路径（默认 false，不传时行为与旧版一致）。

### 取消生命周期覆盖

取消在作业生命周期任意时刻都可发起（排队中 / 刚被调度起来 / 已运行一段时间）：

1. **排队中（QUEUED）**：立刻落 `CANCELLED`，作业永远不会被调度，没有任何子进程产生。
2. **运行中（RUNNING）**：两阶段收尾——
   - **宽限（grace）**：先向该作业全部存活派生子进程发优雅终止请求（SIGTERM），
     在 `cancel_grace_ms`（默认 2000ms）内等待它们自行退出；
   - **强制终止（force）**：宽限到期仍未退出的进程被 SIGKILL 强杀，再等待
     `cancel_force_wait_ms`（默认 2000ms）确认退出；
   - `cancel()` 返回时，属于该作业的派生子进程必须全部确认退出（或给出
     `cleanup_failed` 异常结论，见下），同时并发与内存额度已归还。
3. **刚被调度那一刻**：`QUEUED→RUNNING` 与取消互斥提交——取消要么看到 QUEUED
   （不占任何额度），要么看到 RUNNING（额度由 runner 持有到收尾结束）。因此取消完成后
   不会出现“槽位又被占一下”或作业丢失/重复执行。

**幂等与并发**：重复取消、多个取消请求同时打进来，真正收尾只发生一次；所有请求等待
同一个完成事件并返回同一份取消结果。取消不存在的作业仍返回 HTTP 404（无副作用）；
取消已终态作业仍原样返回、不改写任何字段（与旧版完全一致）。

### 取消结果（可审计、持久化）

作业记录新增两个字段（`GET /jobs/{id}` 返回，旧字段与状态码保持不变）：

- `cancel_requested_at`：首次取消请求的墙钟时间；
- `cancel_result`：取消完成后的审计结果，字段如下：

| 字段 | 含义 |
| --- | --- |
| `requested_at` / `completed_at` | 取消发起 / 完成的墙钟时间（秒） |
| `duration_ms` | 本次收尾实际耗时（`completed_at - requested_at`） |
| `outcome` | 收尾方式：`graceful` / `forced` / `cleanup_failed` / `not_required` |
| `grace_ms` | 本次生效的宽限配置（排队取消为 0） |
| `procs_total` | 进入收尾时仍存活的派生子进程数 |
| `procs_graceful` | 宽限期内自行退出的进程数 |
| `procs_forced` | 宽限后被强制终止的进程数 |
| `procs_remaining` | 强杀等待后仍未确认退出的进程数（>0 即异常） |
| `detail` | 附加说明；异常收尾时描述剩余进程数与超时时长 |

`outcome` 语义：

- `graceful`：作业在宽限期内自行退出；
- `forced`：有进程忽略优雅请求，宽限后被强制终止；
- `not_required`：排队中取消，或刚进入 RUNNING 但还没派生出任何子进程；
- **`cleanup_failed`（收尾异常，单独可区分）**：强杀后仍有进程无法确认退出。此时作业
  状态仍为 `CANCELLED`，但 `reason` 字段为 `cleanup_failed`（正常取消是 `cancelled`），
  `procs_remaining > 0`；重复取消会持续返回同一异常结果，绝不伪装成一次正常取消。

取消结果与作业一起原子写入状态文件，**重启后不丢**；重复取消拿到的是同一份结果。
取消相关的关键节点另写 INFO 日志（`cancel requested` /
`cancel finished outcome=... duration_ms=... procs=... graceful=... forced=... remaining=...`）。

### 对调度容量的影响

- 取消确认完成后，并发/内存额度立即归还，后续排队作业按原有 FIFO 顺序继续调度；
- 取消进行中，作业的额度仍被计在账上（收尾中的进程尚未消失），因此
  `max_concurrent` / `max_memory_mb` 在任何瞬间都不会被突破；
- 超时同样先杀子进程再落终态；`stop()` 会清理所有残留子进程。

### 取消相关配置与默认值

通过 `Limits` 构造参数，或下列环境变量（`main.py` 启动时读取）配置：

| 配置 | 环境变量 | 默认 |
| --- | --- | --- |
| 宽限期 | `JOB_CANCEL_GRACE_MS` | 2000 ms |
| 强杀后确认等待 | `JOB_CANCEL_FORCE_WAIT_MS` | 2000 ms |
| 最大并发 | `JOB_MAX_CONCURRENT` | 4 |
| 内存预算 | `JOB_MAX_MEMORY_MB` | 1024 |
| 单作业进程上限 | `JOB_MAX_PROCS_PER_JOB` | 8 |
| 队列上限 | `JOB_MAX_QUEUE_SIZE` | 64 |

### 兼容性

HTTP 路径、方法、状态码（201/200/404/422）与原有请求体均不变；`ignore_sigterm`、
`cancel_requested_at`、`cancel_result` 均为新增字段，旧客户端与旧状态文件可直接读取
（旧 JSON 中缺失的新字段按默认值反序列化）。

## 恢复边界

状态持久化在 `$JOB_STATE_DIR/jobs.json`（默认 `.job-state/`，原子写入）。重启后：

- 已完成作业（SUCCEEDED/FAILED/TIMEOUT/CANCELLED）保持原终态，**不会重复执行**；
- 崩溃时处于 RUNNING 的作业 → 迁移为 `FAILED / supervisor_restart`，终态可追溯；
- 崩溃时处于 QUEUED 的作业 → 重新入队，**不静默丢失**，恢复后恰好执行一次。

恢复只保证作业级语义；运行中作业已派生的子进程随旧进程消亡，不做输出级断点续跑。

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 原有根接口，行为不变（`{"Hello": "World"}`） |
| POST | `/jobs` | 提交作业，body: `duration_ms, memory_mb, procs?, timeout_ms?, exit_code?, ignore_sigterm?` |
| GET | `/jobs` | 列出全部作业 |
| GET | `/jobs/{id}` | 查询单个作业（不存在 → 404） |
| POST | `/jobs/{id}/cancel` | 幂等取消（不存在 → 404） |

## 本地验证

```bash
uv sync                 # 安装依赖（仅 fastapi 与 pytest）
uv run pytest -q        # 运行全部测试（24 个，含 9 个强取消场景）

# 查看取消场景覆盖与每个场景的断言值（SCENARIO/PASS 横幅 + supervisor 收尾日志）
uv run pytest tests/test_cancel.py -o log_cli=true -o log_cli_level=INFO

JOB_CANCEL_GRACE_MS=500 JOB_CANCEL_FORCE_WAIT_MS=500 \
  JOB_STATE_DIR=.job-state uv run python main.py   # 本地启动服务（可自定义宽限）
```

取消语义的手工验证（服务启动后另开终端）：

```bash
# 正常宽限内自行退出
curl -s -XPOST localhost:8000/jobs -d '{"duration_ms":30000,"memory_mb":16}' -H 'content-type: application/json'
curl -s -XPOST localhost:8000/jobs/<id>/cancel     # cancel_result.outcome == graceful

# 需要强制终止：子进程忽略 SIGTERM，宽限后被强杀
curl -s -XPOST localhost:8000/jobs -d '{"duration_ms":30000,"memory_mb":16,"ignore_sigterm":true}' -H 'content-type: application/json'
curl -s -XPOST localhost:8000/jobs/<id>/cancel     # cancel_result.outcome == forced
```

`tests/test_cancel.py` 覆盖：排队中取消、刚调度那一刻取消、运行到一半取消、
重复与并发取消（单次收尾/同结果）、升级为强制终止、收尾异常（cleanup_failed 可区分）、
取消后额度归还与排队顺序、收尾期间容量不被突破、取消结果跨重启持久化。
测试用例日志以 `SCENARIO:`/`PASS [...] asserted=...` 打印场景与断言事实。
其余关键日志还包含作业标识、输入约束与状态迁移（`logging` 级别 INFO）。
