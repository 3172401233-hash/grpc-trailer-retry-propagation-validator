import { executeWithRetry } from '../src/orchestrator.js';
import { GRPC_STATUS } from '../src/status.js';

const rngZero = () => 0;

function print(title, out) {
  console.log(`\n=== ${title} ===`);
  for (const entry of out.attemptsLog) {
    console.log(
      `attempt ${entry.attempt}:`,
      `trailer=${entry.state.trailersReceived ? entry.state.trailerTiming : 'missing'}`,
      `delivered=${entry.state.delivered}`,
      `aborted=${entry.state.aborted}`,
      `-> ${entry.decision.reasonCode}`,
      entry.decision.retryAllowed ? `(retry in ${entry.decision.backoffMs}ms)` : ''
    );
  }
  console.log('outcome        :', out.outcome);
  if (out.propagatedError) {
    const e = out.propagatedError;
    console.log(
      'propagatedError:',
      `${e.codeName}(${e.code})`,
      e.message,
      `| trailersReceived=${e.trailersReceived}`,
      e.debugInfo ? `| details=${JSON.stringify(e.debugInfo)}` : ''
    );
  }
  console.log(
    'retained       :',
    `${out.retainedResults.deliveredMessages.length} delivered message(s),`,
    `${out.retainedResults.trailerBusinessResults.length} trailer result(s)`
  );
}

// 场景 1：流在 trailer 前断开，代理只见 non-OK；无输出 -> 重试后成功。
print(
  '1. trailer 丢失（连接重置），无输出 -> 透明重试',
  executeWithRetry(
    [
      {
        headers: { ':status': 200 },
        abort: { kind: 'aborted', message: 'connection reset by peer' }
      },
      { trailers: { 'grpc-status': String(GRPC_STATUS.OK) } }
    ],
    { rng: rngZero }
  )
);

// 场景 2：已投递部分消息后流中断 -> 不重试，保留部分输出并传播合成 UNAVAILABLE。
print(
  '2. 部分输出后中断 -> 禁止重试、保留结果',
  executeWithRetry(
    [
      {
        headers: { ':status': 200 },
        messages: [{ payload: 'page-1' }],
        abort: { kind: 'aborted', message: 'reset after data' }
      }
    ],
    { rng: rngZero }
  )
);

// 场景 3：late trailer 带业务部分结果且状态不可重试 -> 保留业务结果、传播原始错误。
print(
  '3. late non-OK trailer -> 业务结果保留、错误原样传播',
  executeWithRetry(
    [
      {
        messages: [{ payload: 'page-1' }],
        trailers: {
          'grpc-status': String(GRPC_STATUS.INTERNAL),
          'grpc-message': 'downstream%20assertion',
          'grpc-status-details-bin': Buffer.from(
            JSON.stringify({ downstream: 'billing', reason: 'mismatch' })
          ),
          'biz-result': JSON.stringify({ savedIds: [101, 102] })
        }
      }
    ],
    { rng: rngZero }
  )
);
