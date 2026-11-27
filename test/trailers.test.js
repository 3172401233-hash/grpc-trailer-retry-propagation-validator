import { test } from 'node:test';
import assert from 'node:assert/strict';
import { GRPC_STATUS } from '../src/status.js';
import {
  parseGrpcStatus,
  parseTrailers,
  parseStatusDetails,
  extractBusinessResults,
  classifyTrailerTiming
} from '../src/trailers.js';

test('parseGrpcStatus：合法/缺失/非法值', () => {
  assert.equal(parseGrpcStatus({ 'grpc-status': '14' }), GRPC_STATUS.UNAVAILABLE);
  assert.equal(parseGrpcStatus({}), null);
  assert.equal(parseGrpcStatus(null), null);
  assert.equal(parseGrpcStatus({ 'grpc-status': '99' }), null);
  assert.equal(parseGrpcStatus({ 'grpc-status': 'oops' }), null);
});

test('early vs late trailer 按已投递消息数分类', () => {
  assert.equal(
    classifyTrailerTiming({ trailersReceived: true, messagesDelivered: 0 }),
    'early'
  );
  assert.equal(
    classifyTrailerTiming({ trailersReceived: true, messagesDelivered: 3 }),
    'late'
  );
  assert.equal(classifyTrailerTiming({ trailersReceived: false }), null);
});

test('status-details-bin：JSON 可解析时还原对象，否则保留 Buffer', () => {
  const json = Buffer.from(JSON.stringify({ reason: 'downstream reset' }));
  assert.deepEqual(parseStatusDetails(json), { reason: 'downstream reset' });

  const raw = Buffer.from([0xff, 0xfe]);
  assert.ok(parseStatusDetails(raw) instanceof Buffer);
  assert.equal(parseStatusDetails(undefined), null);
});

test('提取文本与二进制两种业务部分结果', () => {
  const trailers = {
    'biz-result': JSON.stringify({ savedIds: [1, 2] }),
    'biz-result-bin': Buffer.from(JSON.stringify({ page: 2 }), 'utf8')
  };
  const results = extractBusinessResults(trailers);
  assert.equal(results.length, 2);
  assert.deepEqual(results[0].value, { page: 2 });
  assert.equal(results[0].encoding, 'binary');
  assert.deepEqual(results[1].value, { savedIds: [1, 2] });
  assert.equal(results[1].encoding, 'text');
  assert.equal(extractBusinessResults({}).length, 0);
});

test('parseTrailers 汇总 OK 场景', () => {
  const parsed = parseTrailers(
    {
      'grpc-status': '0',
      'grpc-message': '',
      'biz-result': '{"ok":true}'
    },
    { trailersReceived: true, messagesDelivered: 2 }
  );
  assert.equal(parsed.ok, true);
  assert.equal(parsed.timing, 'late');
  assert.equal(parsed.businessResults.length, 1);
});

test('parseTrailers 处理 trailer 缺失', () => {
  const parsed = parseTrailers(null, { trailersReceived: false });
  assert.equal(parsed.received, false);
  assert.equal(parsed.code, null);
  assert.equal(parsed.timing, null);
});

test('pushback 解析为数字', () => {
  const parsed = parseTrailers(
    { 'grpc-status': '14', 'grpc-retry-pushback-ms': '250' },
    { trailersReceived: true, messagesDelivered: 0 }
  );
  assert.equal(parsed.pushbackMs, 250);
});
