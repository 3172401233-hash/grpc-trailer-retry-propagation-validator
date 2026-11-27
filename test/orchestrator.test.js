import { test } from 'node:test';
import assert from 'node:assert/strict';
import { executeWithRetry } from '../src/orchestrator.js';
import { GRPC_STATUS } from '../src/status.js';

const rngZero = () => 0;

test('第一次 early UNAVAILABLE，第二次 OK：重试成功且无错误传播', () => {
  const scripts = [
    { trailers: { 'grpc-status': String(GRPC_STATUS.UNAVAILABLE) } },
    {
      messages: [{ payload: 'final' }],
      trailers: { 'grpc-status': '0' }
    }
  ];
  const out = executeWithRetry(scripts, { rng: rngZero });
  assert.equal(out.outcome, 'success');
  assert.equal(out.attempts, 2);
  assert.equal(out.attemptsLog[0].decision.retryAllowed, true);
  assert.equal(out.attemptsLog[0].decision.backoffMs, 100);
  assert.equal(out.attemptsLog[1].decision.reasonCode, 'OK');
  assert.equal(out.propagatedError, null);
  assert.equal(out.retainedResults.deliveredMessages.length, 1);
});

test('全部尝试失败：达到 maxAttempts 后传播最后一次错误', () => {
  const scripts = [
    { trailers: { 'grpc-status': String(GRPC_STATUS.UNAVAILABLE) } },
    { trailers: { 'grpc-status': String(GRPC_STATUS.UNAVAILABLE) } }
  ];
  const out = executeWithRetry(scripts, {
    policy: { maxAttempts: 2 },
    rng: rngZero
  });
  assert.equal(out.outcome, 'error');
  assert.equal(out.attempts, 2);
  assert.equal(out.attemptsLog.at(-1).decision.reasonCode, 'MAX_ATTEMPTS_REACHED');
  assert.ok(out.propagatedError.isGrpcStreamError === true);
  assert.equal(out.propagatedError.code, GRPC_STATUS.UNAVAILABLE);
});

test('第一次流中断（trailer 丢失），第二次成功', () => {
  const scripts = [
    { abort: { kind: 'aborted', message: 'connection reset' } },
    { trailers: { 'grpc-status': '0' } }
  ];
  const out = executeWithRetry(scripts, { rng: rngZero });
  assert.equal(out.outcome, 'success');
  assert.equal(
    out.attemptsLog[0].decision.reasonCode,
    'TRANSPORT_FAILURE_BEFORE_OUTPUT'
  );
});

test('流已投递部分输出后中断：不重试，保留消息并传播合成错误', () => {
  const scripts = [
    {
      messages: [{ payload: 'partial' }],
      abort: { kind: 'aborted', message: 'reset after data' }
    }
  ];
  const out = executeWithRetry(scripts, { rng: rngZero });
  assert.equal(out.outcome, 'error');
  assert.equal(out.attempts, 1);
  assert.equal(
    out.attemptsLog[0].decision.reasonCode,
    'OUTPUT_ALREADY_DELIVERED'
  );
  assert.equal(out.retainedResults.deliveredMessages.length, 1);
  assert.equal(out.propagatedError.trailersReceived, false);
  assert.equal(out.propagatedError.code, GRPC_STATUS.UNAVAILABLE);
});

test('late non-OK trailer：不重试，trailer 中的业务结果被保留', () => {
  const scripts = [
    {
      messages: [{ payload: 'm1' }],
      trailers: {
        'grpc-status': String(GRPC_STATUS.INTERNAL),
        'grpc-message': 'boom',
        'biz-result': JSON.stringify({ savedIds: [7] })
      }
    }
  ];
  const out = executeWithRetry(scripts, { rng: rngZero });
  assert.equal(out.outcome, 'error');
  assert.equal(out.attempts, 1);
  assert.equal(out.propagatedError.code, GRPC_STATUS.INTERNAL);
  assert.equal(out.retainedResults.trailerBusinessResults.length, 1);
  assert.deepEqual(
    out.retainedResults.trailerBusinessResults[0].value,
    { savedIds: [7] }
  );
});

test('不可重试状态（INTERNAL）立即失败，绝不重发', () => {
  const scripts = [
    { trailers: { 'grpc-status': String(GRPC_STATUS.INTERNAL) } }
  ];
  const out = executeWithRetry(scripts, { rng: rngZero });
  assert.equal(out.attempts, 1);
  assert.equal(
    out.attemptsLog[0].decision.reasonCode,
    'NON_RETRYABLE_STATUS'
  );
});

test('脚本工厂支持按 attempt 动态生成', () => {
  const out = executeWithRetry(
    (attempt) =>
      attempt < 3
        ? { trailers: { 'grpc-status': String(GRPC_STATUS.UNAVAILABLE) } }
        : { trailers: { 'grpc-status': '0' } },
    { rng: rngZero }
  );
  assert.equal(out.outcome, 'success');
  assert.equal(out.attempts, 3);
});

test('代理视角贯穿每次尝试：trailer 丢失时 reconstruction 带 diff', () => {
  const scripts = [
    {
      headers: { ':status': 400 },
      abort: { kind: 'aborted' }
    },
    { trailers: { 'grpc-status': '0' } }
  ];
  const out = executeWithRetry(scripts, { rng: rngZero });
  const first = out.attemptsLog[0];
  assert.equal(first.proxy.trailersReceived, false);
  assert.equal(first.reconstruction.source, 'http-status');
  assert.match(first.reconstruction.diff, /trailer missing/);
});

test('客户端取消：立即终止且不重试', () => {
  const out = executeWithRetry(
    [{ trailers: { 'grpc-status': String(GRPC_STATUS.UNAVAILABLE) } }],
    { clientCancelled: true, rng: rngZero }
  );
  assert.equal(out.outcome, 'error');
  assert.equal(out.attempts, 1);
  assert.equal(
    out.attemptsLog[0].decision.reasonCode,
    'CLIENT_CANCELLED'
  );
  assert.equal(out.propagatedError.code, GRPC_STATUS.CANCELLED);
});

test('缓冲但未投递的消息在重试时被丢弃，不计入保留结果', () => {
  const scripts = [
    {
      delivery: 'buffer',
      messages: [{ payload: 'buffered-only' }],
      trailers: { 'grpc-status': String(GRPC_STATUS.UNAVAILABLE) }
    },
    { trailers: { 'grpc-status': '0' } }
  ];
  const out = executeWithRetry(scripts, { rng: rngZero });
  assert.equal(out.outcome, 'success');
  assert.equal(
    out.attemptsLog[0].decision.retained.discardedBufferedMessages.length,
    1
  );
  assert.equal(out.retainedResults.deliveredMessages.length, 0);
});
