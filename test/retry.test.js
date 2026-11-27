import { test } from 'node:test';
import assert from 'node:assert/strict';
import { GRPC_STATUS } from '../src/status.js';
import {
  decideRetry,
  computeBackoff,
  DEFAULT_RETRY_POLICY,
  mergePolicy
} from '../src/retry.js';

const rngZero = () => 0;
const emptyState = {
  trailersReceived: false,
  deliveredMessages: [],
  messagesBuffered: []
};

function trailerState(trailers, extra = {}) {
  return {
    trailersReceived: true,
    trailerTiming: extra.trailerTiming ?? 'early',
    deliveredMessages: extra.deliveredMessages ?? [],
    messagesBuffered: extra.messagesBuffered ?? [],
    trailers
  };
}

test('early trailer + UNAVAILABLE：允许重试，计算退避，缓冲消息丢弃', () => {
  const state = trailerState(
    { 'grpc-status': String(GRPC_STATUS.UNAVAILABLE), 'grpc-message': 'reset' },
    { messagesBuffered: [{ seq: 0, payload: 'buffered' }] }
  );
  const d = decideRetry({ attempt: 1, streamState: state, trailers: state.trailers, rng: rngZero });
  assert.equal(d.retryAllowed, true);
  assert.equal(d.decision, 'retry');
  assert.equal(d.nextAttempt, 2);
  assert.equal(d.reasonCode, 'RETRYABLE_STATUS');
  assert.equal(d.backoffMs, DEFAULT_RETRY_POLICY.initialBackoffMs);
  assert.equal(d.retained.discardedBufferedMessages.length, 1);
  assert.equal(d.propagatedError, null);
});

test('DEADLINE_EXCEEDED 默认也可重试', () => {
  const state = trailerState({ 'grpc-status': String(GRPC_STATUS.DEADLINE_EXCEEDED) });
  const d = decideRetry({ attempt: 1, streamState: state, trailers: state.trailers, rng: rngZero });
  assert.equal(d.retryAllowed, true);
});

test('INTERNAL 等不可重试状态：禁止重发并传播原始错误详情', () => {
  const trailers = {
    'grpc-status': String(GRPC_STATUS.INTERNAL),
    'grpc-message': 'assertion%20failed',
    'grpc-status-details-bin': Buffer.from(JSON.stringify({ detail: 'ctx' }))
  };
  const state = trailerState(trailers);
  const d = decideRetry({ attempt: 1, streamState: state, trailers });
  assert.equal(d.retryAllowed, false);
  assert.equal(d.hardNonRetryable, true);
  assert.equal(d.reasonCode, 'NON_RETRYABLE_STATUS');
  assert.ok(d.propagatedError.isGrpcStreamError === true);
  assert.equal(d.propagatedError.code, GRPC_STATUS.INTERNAL);
  assert.equal(d.propagatedError.message, 'assertion failed');
  assert.deepEqual(d.propagatedError.debugInfo, { detail: 'ctx' });
  assert.equal(d.propagatedError.trailersReceived, true);
});

test('late trailer 且已投递消息：即使状态可重试也硬保护，不重发', () => {
  const trailers = { 'grpc-status': String(GRPC_STATUS.UNAVAILABLE) };
  const state = trailerState(trailers, {
    trailerTiming: 'late',
    deliveredMessages: [{ seq: 0, payload: 'sent-to-app' }]
  });
  const d = decideRetry({ attempt: 1, streamState: state, trailers });
  assert.equal(d.retryAllowed, false);
  assert.equal(d.reasonCode, 'OUTPUT_ALREADY_DELIVERED');
  assert.equal(d.hardNonRetryable, true);
  assert.equal(d.retained.partialOutputCount, 1);
  assert.equal(d.propagatedError.code, GRPC_STATUS.UNAVAILABLE);
  assert.equal(d.propagatedError.trailerTiming, 'late');
});

test('early trailer 携带业务结果且可重试：结果标记为陈旧，不直接保留', () => {
  const trailers = {
    'grpc-status': String(GRPC_STATUS.UNAVAILABLE),
    'biz-result': JSON.stringify({ partial: true })
  };
  const state = trailerState(trailers);
  const d = decideRetry({ attempt: 1, streamState: state, trailers, rng: rngZero });
  assert.equal(d.retryAllowed, true);
  assert.equal(d.retained.trailerBusinessResults.length, 1);
  assert.equal(d.retained.staleTrailerResultsOnRetry, true);
});

test('服务端 pushback = -1：禁止重试', () => {
  const trailers = {
    'grpc-status': String(GRPC_STATUS.UNAVAILABLE),
    'grpc-retry-pushback-ms': '-1'
  };
  const state = trailerState(trailers);
  const d = decideRetry({ attempt: 1, streamState: state, trailers });
  assert.equal(d.retryAllowed, false);
  assert.equal(d.reasonCode, 'SERVER_PUSHBACK_SUPPRESSED');
  assert.equal(d.propagatedError.code, GRPC_STATUS.UNAVAILABLE);
});

test('服务端 pushback 正数：用其值覆盖退避', () => {
  const trailers = {
    'grpc-status': String(GRPC_STATUS.UNAVAILABLE),
    'grpc-retry-pushback-ms': '300'
  };
  const state = trailerState(trailers);
  const d = decideRetry({ attempt: 1, streamState: state, trailers });
  assert.equal(d.retryAllowed, true);
  assert.equal(d.backoffMs, 300);
});

test('达到 maxAttempts：不再重试，传播最后一次错误', () => {
  const trailers = { 'grpc-status': String(GRPC_STATUS.UNAVAILABLE) };
  const state = trailerState(trailers);
  const d = decideRetry({
    attempt: 4,
    streamState: state,
    trailers,
    policy: { maxAttempts: 4 },
    rng: rngZero
  });
  assert.equal(d.retryAllowed, false);
  assert.equal(d.reasonCode, 'MAX_ATTEMPTS_REACHED');
  assert.ok(d.propagatedError.isGrpcStreamError === true);
});

test('trailer 丢失且无输出：合成 UNAVAILABLE 并允许 transport 重试', () => {
  const d = decideRetry({
    attempt: 1,
    streamState: { ...emptyState, endReason: 'aborted' },
    trailers: null,
    reason: { kind: 'aborted', message: 'connection reset by peer' },
    rng: rngZero
  });
  assert.equal(d.retryAllowed, true);
  assert.equal(d.reasonCode, 'TRANSPORT_FAILURE_BEFORE_OUTPUT');
  assert.equal(d.propagatedError, null);
  assert.equal(d.nextAttempt, 2);
});

test('trailer 丢失但已有部分输出：保留消息、禁止重试、传播合成错误', () => {
  const d = decideRetry({
    attempt: 2,
    streamState: {
      ...emptyState,
      endReason: 'aborted',
      deliveredMessages: [{ seq: 0, payload: 'partial' }]
    },
    trailers: null,
    reason: { kind: 'aborted', message: 'reset after data' }
  });
  assert.equal(d.retryAllowed, false);
  assert.equal(d.reasonCode, 'OUTPUT_ALREADY_DELIVERED');
  assert.equal(d.retained.partialOutputCount, 1);
  assert.equal(d.propagatedError.code, GRPC_STATUS.UNAVAILABLE);
  assert.equal(d.propagatedError.trailersReceived, false);
  assert.match(d.propagatedError.message, /trailers missing/);
  assert.equal(d.propagatedError.details, 'reset after data');
});

test('RST CANCEL 在无输出时也不可重试（硬集合）', () => {
  const d = decideRetry({
    attempt: 1,
    streamState: { ...emptyState, endReason: 'rst' },
    trailers: null,
    reason: { kind: 'rst', rstCode: 'CANCEL' }
  });
  assert.equal(d.retryAllowed, false);
  assert.equal(d.reasonCode, 'HARD_NON_RETRYABLE_TRANSPORT_CODE');
  assert.equal(d.propagatedError.code, GRPC_STATUS.CANCELLED);
});

test('RST REFUSED_STREAM 映射 UNAVAILABLE，可重试', () => {
  const d = decideRetry({
    attempt: 1,
    streamState: { ...emptyState, endReason: 'rst' },
    trailers: null,
    reason: { kind: 'rst', rstCode: 'REFUSED_STREAM' },
    rng: rngZero
  });
  assert.equal(d.retryAllowed, true);
  assert.equal(d.propagatedError, null);
});

test('关闭 transport 重试策略后，trailer 丢失直接失败', () => {
  const d = decideRetry({
    attempt: 1,
    streamState: { ...emptyState, endReason: 'aborted' },
    trailers: null,
    reason: { kind: 'aborted' },
    policy: { retryOnTransportFailure: false }
  });
  assert.equal(d.retryAllowed, false);
  assert.equal(d.reasonCode, 'TRANSPORT_RETRY_DISABLED');
});

test('客户端取消：不可重试，传播 CANCELLED', () => {
  const d = decideRetry({
    attempt: 1,
    streamState: emptyState,
    trailers: null,
    clientCancelled: true
  });
  assert.equal(d.retryAllowed, false);
  assert.equal(d.reasonCode, 'CLIENT_CANCELLED');
  assert.equal(d.propagatedError.code, GRPC_STATUS.CANCELLED);
});

test('OK trailer：complete，无错误传播', () => {
  const trailers = { 'grpc-status': '0' };
  const state = trailerState(trailers);
  const d = decideRetry({ attempt: 1, streamState: state, trailers });
  assert.equal(d.decision, 'complete');
  assert.equal(d.retryAllowed, false);
  assert.equal(d.propagatedError, null);
});

test('自定义策略把 ABORTED 纳入可重试集合', () => {
  const policy = mergePolicy({
    retryableCodes: [GRPC_STATUS.UNAVAILABLE, GRPC_STATUS.ABORTED]
  });
  const trailers = { 'grpc-status': String(GRPC_STATUS.ABORTED) };
  const state = trailerState(trailers);
  const d = decideRetry({ attempt: 1, streamState: state, trailers, policy, rng: rngZero });
  assert.equal(d.retryAllowed, true);
  assert.equal(d.reasonCode, 'RETRYABLE_STATUS');
});

test('computeBackoff：确定性 rng 与指数上限', () => {
  const policy = DEFAULT_RETRY_POLICY;
  assert.equal(computeBackoff(1, policy, rngZero), 100);
  assert.equal(computeBackoff(2, policy, rngZero), 100); // 200 区间的下界仍是 initial
  const high = computeBackoff(10, policy, () => 1);
  assert.equal(high, policy.maxBackoffMs);
});
