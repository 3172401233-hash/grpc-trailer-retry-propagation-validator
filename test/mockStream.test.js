import { test } from 'node:test';
import assert from 'node:assert/strict';
import { MockHttp2Stream, runScript } from '../src/mockStream.js';

test('完整脚本：headers -> data -> trailers 的事件顺序', () => {
  const { state, events } = runScript({
    headers: { ':status': 200 },
    messages: [{ payload: 'a' }, { payload: 'b' }],
    trailers: { 'grpc-status': '0' }
  });
  assert.deepEqual(
    events.map((e) => e.name),
    ['response', 'data', 'data', 'trailers', 'close']
  );
  assert.equal(state.trailerTiming, 'late');
  assert.equal(state.deliveredMessages.length, 2);
  assert.equal(state.closed, true);
});

test('缓冲模式：消息不投递到应用，trailer 仍是 early', () => {
  const { state } = runScript({
    headers: { ':status': 200 },
    delivery: 'buffer',
    messages: [{ payload: 'a' }],
    trailers: { 'grpc-status': '14' }
  });
  assert.equal(state.messagesBuffered.length, 1);
  assert.equal(state.deliveredMessages.length, 0);
  assert.equal(state.trailerTiming, 'early');
});

test('消息级 deliver 覆盖：部分投递部分缓冲', () => {
  const { state } = runScript({
    delivery: 'buffer',
    messages: [
      { payload: 'a', deliver: true },
      { payload: 'b' }
    ]
  });
  assert.equal(state.deliveredMessages.length, 1);
  assert.equal(state.messagesBuffered.length, 2);
});

test('无 trailer 的中断：记录 abort 原因与 RST 码', () => {
  const { state, events } = runScript({
    headers: { ':status': 200 },
    messages: [{ payload: 'a' }],
    abort: { kind: 'rst', rstCode: 'CANCEL', message: 'peer reset' }
  });
  assert.equal(state.aborted, true);
  assert.equal(state.trailersReceived, false);
  assert.equal(state.abortReason.rstCode, 'CANCEL');
  assert.deepEqual(
    events.map((e) => e.name),
    ['response', 'data', 'error', 'close']
  );
});

test('客户端 cancel：标记 clientCancelled', () => {
  const stream = new MockHttp2Stream();
  const cancelled = [];
  stream.on('cancelled', () => cancelled.push(1));
  stream.cancel();
  assert.deepEqual(cancelled, [1]);
  assert.equal(stream.state.clientCancelled, true);
  assert.equal(stream.state.closed, true);
});

test('snapshot 返回副本，外部修改不污染内部状态', () => {
  const { state } = runScript({
    trailers: { 'grpc-status': '0' }
  });
  state.trailers['grpc-status'] = '99';
  const fresh = runScript({ trailers: { 'grpc-status': '0' } }).state;
  assert.equal(fresh.trailers['grpc-status'], '0');
});
