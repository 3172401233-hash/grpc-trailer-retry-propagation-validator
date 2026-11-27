# grpc-trailer-retry-propagation-validator

纯内存模拟工具，用于验证 gRPC 流式调用在 **trailer、状态码、重试与业务部分结果传播** 上的行为，
覆盖以下问题场景：

- 流在返回 trailer 前断开，代理只看到 non-OK 的 HTTP/RST 状态，拿不到原始 `grpc-status`、
  `grpc-message`、`grpc-status-details-bin` 错误详情；
- 重试层把不可重试错误（如 `INTERNAL`、客户端取消）误判为可重试而重发；
- 流已经向应用投递过部分消息后仍被重试，造成重复副作用；
- trailer 中携带的业务部分结果（`biz-result` / `biz-result-bin`）被丢弃。

## 不建立真实 HTTP/2 连接

本项目 **没有任何 socket、TLS 或网络 I/O**：

- `src/mockStream.js` 用 `node:events` 的 `EventEmitter` 模拟一条 HTTP/2 流，
  按脚本顺序发出 response headers、DATA 消息、trailers，或 RST/中断/取消事件；
- 所有"线上"metadata 只是普通对象，二进制 `-bin` 字段用 base64 字符串表示，
  不经过任何真实帧编码；
- 退避时间只计算数值，不会真的 `setTimeout` 等待（测试里用注入的确定性 `rng`）。

因此可以在离线环境用 `node --test` 完整验证决策逻辑。

## 目录结构

```
src/
  status.js       gRPC <-> HTTP 状态码双向映射、RST 推断
  metadata.js     -bin 二进制 metadata 的 base64 编解码与 ASCII 校验
  errors.js       GrpcStreamError、trailer 错误构造、trailer 丢失时的合成错误
  trailers.js     grpc-status 解析、early/late 分类、status-details 与业务结果提取
  retry.js        重试策略、退避、pushback、decideRetry 核心决策
  mockStream.js   纯内存模拟 HTTP/2 流与脚本执行器
  proxy.js        代理视角转换、trailer 截断处理、HTTP 状态重建、二进制完整性校验
  orchestrator.js 多 attempt 执行器，串联模拟流、代理转换与重试决策
test/             node:test 测试（61 个用例）
examples/demo.js  三个关键场景的可运行演示
```

## 输入与输出

`decideRetry({ attempt, streamState, trailers, reason, clientCancelled, policy, rng })`
的输入覆盖任务要求的全部要素：

- **headers / trailers**：内部形式 metadata（`-bin` 键为 `Buffer`）；
- **状态码与错误详情**：`grpc-status`、`grpc-message`（百分号解码）、
  `grpc-status-details-bin`（JSON 可解析时还原为对象，否则保留 Buffer）；
- **attempt**：当前尝试序号，配合 `maxAttempts` 控制上限；
- **流式消息**：区分 `messagesBuffered`（透明重试缓冲，未交给应用）与
  `deliveredMessages`（已越过缓冲、应用可见的部分输出）；
- **代理转换**：`proxyTransform` 同时给出线上（base64）与内部两个视角，
  并在 trailer 缺失时从 HTTP/RST 重建 gRPC 状态、标注 `diff`。

决策结果包含三部分：

- `retryAllowed` / `decision` / `reasonCode`：是否允许重试及原因；
- `retained`：可保留的业务结果（已投递消息、trailer 业务结果、被丢弃的缓冲消息、
  陈旧结果标记 `staleTrailerResultsOnRetry`）；
- `propagatedError`：需要向应用传播的 `GrpcStreamError`（重试时为 `null`）。

## 重试决策规则

| 场景 | 决策 | 业务结果处理 |
| --- | --- | --- |
| 客户端取消 | 不重试，传播 `CANCELLED` | 已投递消息保留 |
| OK trailer | `complete` | 消息与 trailer 业务结果全部保留 |
| early trailer + 可重试状态（默认 `UNAVAILABLE`/`DEADLINE_EXCEEDED`） | 重试 | 缓冲消息丢弃；trailer 业务结果标记为陈旧 |
| late trailer（已有消息投递），即使状态可重试 | 硬保护，不重试 | 已投递消息与 trailer 业务结果保留，传播错误 |
| 不可重试状态（如 `INTERNAL`、`NOT_FOUND`） | 不重试，原样传播错误详情 | 已投递消息保留 |
| `grpc-retry-pushback-ms: -1` | 服务端压制重试 | 传播错误 |
| `grpc-retry-pushback-ms >= 0` | 重试，退避取 pushback 值 | 同 early trailer |
| trailer 丢失且无输出（RST/连接中断） | 合成 `UNAVAILABLE` 后 transport 重试 | 缓冲消息丢弃 |
| trailer 丢失但已有部分输出 | 不重试 | 已投递消息保留，传播合成错误 |
| RST `CANCEL` / 鉴权类合成码 | 硬性不重试 | 传播合成错误 |
| 达到 `maxAttempts` | 不重试 | 传播最后一次错误 |

关键硬保护：**只要消息已经投递给应用（`deliveredMessages` 非空），任何失败都不再重试**，
与 gRPC 透明重试"未提交应用前才能重试"的语义一致。

## 代理视角与状态码转换

- trailer 完整时，代理以 trailer 中的 `grpc-status` 为权威来源；
- trailer 缺失时只能从 HTTP `:status` 或 RST 码推断，重建结果通过 `reconstruction.diff`
  显式标注可能与真实业务码不同（例如 HTTP 400 会被还原成 `INTERNAL`，而真实码可能是
  `INVALID_ARGUMENT`）；
- `checkBinaryIntegrity` 对 headers/trailers 中的每个 `-bin` 字段做 base64 往返校验，
  保证代理转发不损坏二进制字节。

## 运行

要求 Node.js 20+（开发与验证环境为 Node.js 24.16.0），零第三方依赖。

```bash
node --test        # 或 npm test
node examples/demo.js
```
