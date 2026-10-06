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
  `completed` / `exit_code` / `timeout` / `cancelled` / `cancel_cleanup_failed`
  / `spawn_failed` / `supervisor_restart`。
  其中 `cancel_cleanup_failed` 专指“取消收尾本身失败（进程强制后仍清不掉）”，
  与普通 `cancelled` 明确区分。
- 每次迁移都写日志（`job=<id> transition <old> -> <new> reason=<...>`），
  提交时打印输入约束（duration/memory/procs/timeout）。

## 资源限制语义

全局限制见 `app/models.py: Limits`（默认值可用环境或构造参数调整）：

| 限制 | 默认 | 环境变量 | 语义 |
| --- | --- | --- | --- |
| `max_concurrent` | 4 | `JOB_MAX_CONCURRENT` | 同时处于 RUNNING 的作业数上限 |
| `max_memory_mb` | 1024 | `JOB_MAX_MEMORY_MB` | 所有运行中作业 `memory_mb` 预留量之和的上限（记账式模拟，非真实 RSS） |
| `max_procs_per_job` | 8 | `JOB_MAX_PROCS_PER_JOB` | 单作业派生子进程数上限 |
| `max_queue_size` | 64 | `JOB_MAX_QUEUE_SIZE` | 排队中作业数上限 |
| `cancel_grace_period_ms` | 5000 | `JOB_CANCEL_GRACE_PERIOD_MS` | 取消宽限期：发起取消后先请求优雅退出，超过该时间仍存活的进程被强制终止 |
| `cancel_kill_wait_ms` | 2000 | `JOB_CANCEL_KILL_WAIT_MS` | 强制终止后等待进程真正退出的最长时间，超时即判定收尾异常 |

提交即校验，拒绝时返回 HTTP 422，`detail.reason` 为可区分的原因码：

- `invalid_params`：时长/内存/进程数非正、timeout 非正、exit_code 越界；
- `memory_exceeds_limit`：`memory_mb` 超过全局内存预算（单作业即不可能运行）；
- `process_count_exceeds_limit`：`procs` 超过单作业上限；
- `queue_full`：排队已满。

被拒绝的作业不会留下任何记录。运行中的作业只有在并发数与内存预算同时满足时才会被调度，
突发并发不会突破上限。

## 作业执行与取消

- 运行一个作业 = 派生 `procs` 个本地子进程（`python -c "sleep(duration); exit(exit_code)"`），
  用 `exit_code` 模拟异常退出，用 `timeout_ms` 触发超时。
- 取消对外接口保持兼容：`POST /jobs/{id}/cancel` 与 `JobSupervisor.cancel(job_id)`
  的调用方式、返回形态（作业记录）、状态码均不变；不存在仍 404，终态作业仍原样返回。
- 取消覆盖作业生命周期任意时刻：排队中、刚被调度起来（QUEUED→RUNNING 的交接瞬间）、
  已运行到一半都可取消。取消返回时该作业派生出的本地进程必定已全部退出（或被明确报告为收尾异常）。

### 宽限与强制终止规则

对运行中作业发起取消时，收尾按两个阶段进行（计时从该次取消真正开始起算）：

1. **优雅阶段（grace period）**：向所有存活派生进程发送优雅停止信号，
   在 `cancel_grace_period_ms`（默认 5000ms）内等待它们自行退出；宽限期共享同一个截止点，
   不按进程数叠加。
2. **强制阶段（force）**：宽限期结束仍存活的进程被强制终止（SIGKILL），
   随后最多等待 `cancel_kill_wait_ms`（默认 2000ms）确认其真正退出。

- 所有进程在宽限期内自行退出 → 取消结果 `outcome=graceful`，取消在宽限期内很快返回。
- 有进程忽略优雅信号、被强制终止后全部确认退出 → `outcome=forced`，
  作业 `reason` 仍为普通 `cancelled`。
- 强制终止后仍有进程无法确认退出 → `outcome=cleanup_failed`，且作业落为
  **独立可区分的终止原因 `cancel_cleanup_failed`**（不是一次“正常取消”）；
  `cancel_result.detail` 会说明残留进程数。此情况下并发/内存额度仍立即归还，
  残留进程登记保留，`stop()` 关闭时会再次强制回收，不遗留真实泄漏。
- 排队中取消没有任何进程需要清理 → `outcome=not_needed`，立即完成。

### 单发与幂等

- 同一作业的取消是**单发（single-flight）**的：重复取消、多个取消请求同时打进来，
  只会真正收尾一次；后来的请求等待并返回**同一个最终结果**（同一 `requested_at`、
  同一终态写入时间 `finished_at`）。
- 取消不存在的作业 → `None` / HTTP 404，无副作用；取消终态作业 → 原样返回，不改写任何字段。
- 取消与自然结束（成功/失败/超时）竞争时以“先到的终局决定者”为准，状态不会被二次改写。

### 取消结果字段（`cancel_result`，随作业持久化）

每次真正终结一个作业的取消都会在作业记录里留下 `cancel_result`（重启后不丢）：

| 字段 | 含义 |
| --- | --- |
| `outcome` | `graceful`（宽限期内自行退出）/ `forced`（宽限后强制终止）/ `cleanup_failed`（收尾异常）/ `not_needed`（排队取消，无进程） |
| `requested_at` | 本次取消发起的墙钟时间（`time.time()`）；并发/重复取消共享同一个值 |
| `duration_ms` | 从取消开始到收尾完成实际花费的毫秒数（单调时钟） |
| `processes_total` | 取消时该作业登记的派生进程总数（排队取消为 0） |
| `processes_graceful` | 宽限期内自行退出的进程数 |
| `processes_forced` | 被强制终止并确认退出的进程数 |
| `processes_left` | 强制后仍无法确认退出的进程数；>0 时必定是 `cleanup_failed` |
| `grace_period_ms` | 本次取消实际使用的宽限配置值 |
| `detail` | 补充说明；收尾异常时包含残留进程数等信息 |

收尾异常同时体现在作业顶层：`status=cancelled`、`reason=cancel_cleanup_failed`，
便于与普通 `reason=cancelled` 明确区分。

### 取消与调度容量

- 作业确认取消后立即归还并发与内存额度；排队取消不占用也不遗留额度。
- 取消进行的整个窗口内，全局并发/内存上限不会被突破（旧进程未退出前其额度不会提前释放给新作业）。
- 排队作业被取消后调度器直接跳过、永不执行；其后排队作业按原有 FIFO 顺序继续调度，
  不会因取消而丢失或重复执行。

- 超时同样先杀子进程再落终态；`stop()` 会清理所有残留子进程（含 `cleanup_failed` 遗留）。

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
| POST | `/jobs` | 提交作业，body: `duration_ms, memory_mb, procs?, timeout_ms?, exit_code?` |
| GET | `/jobs` | 列出全部作业 |
| GET | `/jobs/{id}` | 查询单个作业（不存在 → 404） |
| POST | `/jobs/{id}/cancel` | 幂等取消（不存在 → 404） |

## 本地验证

```bash
uv sync                 # 安装依赖（仅 fastapi 与 pytest）
uv run pytest -q        # 运行全部测试（默认 INFO 日志打印作业迁移与取消逐条断言）
uv run pytest tests/test_cancellation.py -v          # 只跑强化取消语义的 8 个场景
JOB_CANCEL_GRACE_PERIOD_MS=1000 JOB_CANCEL_KILL_WAIT_MS=1000 \
  JOB_STATE_DIR=.job-state uv run python main.py     # 自定义宽限/强杀等待后启动服务
JOB_STATE_DIR=.job-state uv run python main.py       # 默认配置启动服务
```

取消语义的场景化单测见 `tests/test_cancellation.py`，覆盖：排队中取消、
刚被调度那一刻取消、运行到一半取消（优雅退出）、重复与并发取消（同一最终结果）、
宽限期后升级强制终止、收尾异常（独立原因 `cancel_cleanup_failed`）、
取消后并发/内存额度归还且排队 FIFO 不乱、取消审计结果跨重启保留。
每个场景都会在日志里打印 `[场景名] assert N PASS: <断言内容>` 和
`SCENARIO COMPLETE with N assertions`，可直观看出覆盖了哪些场景及各自断言结果。
