# local-job-supervisor-python

基于 FastAPI 的本地并发作业执行子系统：用本地子进程与本地 JSON 状态文件模拟多类
短时作业，使并发提交、取消与重启恢复后的执行结果可预测、可审计。根接口
`GET /` 行为保持不变（返回 `{"Hello": "World"}`）。

## 作业状态模型

```
                +---------+        +-----------+
   submit ----> | queued  | -----> |  running  | -----> succeeded (completed)
                +---------+        +-----------+ -----> failed     (exit_nonzero / start_failed)
                     |                  |        -----> timeout    (timeout)
                     v                  v
                cancelled          cancelled       (cancelled)
```

- 状态：`queued → running → succeeded | failed | timeout | cancelled`。
- 终态：`succeeded / failed / timeout / cancelled`，进入终态后不再迁移。
- 每次迁移都写入 `history`（含时间戳与原因）并立即持久化到状态文件；
  日志同时打印作业标识、输入约束与状态迁移。
- 终态原因（`reason`）可区分：`completed`、`exit_nonzero`、`start_failed`、
  `timeout`、`cancelled`、`interrupted_by_restart`。

## 作业类型与约束

作业以独立本地子进程（`python -m jobsupervisor.worker`）运行，各自处于独立进程组：

| 类型    | 行为                                       |
|---------|--------------------------------------------|
| `sleep` | 空闲 `duration_ms` 后退出 0                |
| `alloc` | 驻留 `memory_mb` MiB 内存 `duration_ms`    |
| `fail`  | 以非零码（3）异常退出                      |
| `spawn` | 派生 `max_procs` 个子进程并等待            |
| `hang`  | 运行 10 倍 `duration_ms`（确定性触发超时） |

每个作业可带约束：`duration_ms`（运行时长上限）、`memory_mb`（内存预留）、
`max_procs`（派生进程数）。

## 资源限制语义

全局限制由环境变量配置（默认值见下）：

| 环境变量                   | 默认  | 含义                     |
|----------------------------|-------|--------------------------|
| `JOB_MAX_CONCURRENCY`      | 4     | 全局并发运行上限         |
| `JOB_MAX_QUEUE_SIZE`       | 16    | 排队队列长度上限         |
| `JOB_MAX_MEMORY_MB_TOTAL`  | 512   | 排队+运行内存总预算      |
| `JOB_MAX_MEMORY_MB_PER_JOB`| 256   | 单作业内存上限           |
| `JOB_MAX_PROCS_PER_JOB`    | 4     | 单作业派生进程数上限     |
| `JOB_MAX_DURATION_MS`      | 60000 | 单作业运行时长上限       |
| `JOB_TIMEOUT_GRACE_MS`     | 1000  | 超时时限的宽限           |
| `JOB_STATE_FILE`           | job_state.json | 状态文件路径    |

提交时的拒绝原因可区分（HTTP 状态 + `detail.reason`）：

- `invalid_params`（400）：参数非法（负值、零时长等）；
- `resource_limit`（400）：超出单项资源上限（内存/进程数/时长）；
- `insufficient_resources`（409）：全局内存预算不足；
- `queue_full`（409）：排队队列已满。

突发并发下，超出并发上限的作业排队等待，运行数永不突破
`JOB_MAX_CONCURRENCY`；运行时长超过 `duration_ms + 宽限` 的作业被判为
`timeout` 并整组终止。

## 取消语义

- `POST /jobs/{id}/cancel` 幂等：并发重复取消只产生一次 `cancelled` 迁移；
  取消已结束作业无副作用，返回当前终态；取消不存在作业返回 404。
- 取消运行中作业会对整个进程组发送 SIGKILL，不会遗留已派生的子进程。

## 恢复边界

- 所有状态迁移即时持久化（原子写入：临时文件 + rename）。
- 重启后（`recover()`）：
  - 已完成作业保持终态，**不会重复执行**（`attempts` 不变）；
  - 原 `running` 作业（其进程已随旧服务消亡）被关闭为
    `failed / interrupted_by_restart`，**不静默丢失**；
  - 原 `queued` 作业重新入队并继续调度执行。
- 服务关闭时会杀掉运行中的进程组（不泄漏子进程），记录保持 `running`
  留待下次恢复时结算。

## HTTP 接口

- `GET /` — 原根接口，行为不变。
- `POST /jobs` — 提交作业，201 返回作业记录；拒绝时 400/409 + 原因。
- `GET /jobs` / `GET /jobs/{id}` — 查询作业（含 `history` 审计轨迹）。
- `POST /jobs/{id}/cancel` — 幂等取消。

## 本地验证（无外部服务/凭据/网络）

```bash
uv sync                       # 安装依赖（含 dev: pytest、httpx）
uv run python -m pytest tests/ -q          # 运行全部测试
uv run python main.py  # 或 uvicorn main:app  # 本地启动服务
```

测试覆盖：并发上限、重复/并发取消、参数非法、资源超限、队列已满、
异常退出、超时、取消后子进程释放、重启恢复；关键用例断言日志中包含
作业标识、输入约束与状态迁移。
