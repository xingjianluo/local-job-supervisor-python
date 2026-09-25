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
  `completed` / `exit_code` / `timeout` / `cancelled` / `spawn_failed` / `supervisor_restart`。
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

## 作业执行与取消

- 运行一个作业 = 派生 `procs` 个本地子进程（`python -c "sleep(duration); exit(exit_code)"`），
  用 `exit_code` 模拟异常退出，用 `timeout_ms` 触发超时。
- 取消是幂等的：
  - 取消不存在的作业 → HTTP 404，无副作用；
  - 取消终态作业 → 原样返回当前状态，不改写任何字段；
  - 取消排队作业 → 标记 CANCELLED，调度器跳过，永不执行；
  - 取消运行中作业 → 杀死全部派生子进程并等待 runner 收尾，返回时不遗留子进程。
- 超时同样先杀子进程再落终态；`stop()` 会清理所有残留子进程。

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
uv run pytest -q        # 运行全部测试（并发上限/重复取消/资源超限/异常退出/超时/重启恢复）
JOB_STATE_DIR=.job-state uv run python main.py   # 本地启动服务
```

测试关键用例的日志会打印作业标识、输入约束与状态迁移（`logging` 级别 INFO）。
