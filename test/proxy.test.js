import { test } from 'node:test';
import assert from 'node:assert/strict';
import { GRPC_STATUS } from '../src/status.js';
import { runScript } from '../src/mockStream.js';
import {
  proxyEncode,
  proxyDecode,
  buildProxyView,
  reconstructGrpcStatusFromHttp,
  proxyTransform,
  checkBinaryIntegrity
} from '../src/proxy.js';

test('代理编码/解码：二进制 trailer base64 化再还原', () => {
  const bytes = Buffer.from([1, 2, 3, 255]);
  const wire = proxyEncode({ 'biz-result-bin': bytes, 'x-id': 'abc' });
  assert.equal(wire['biz-result-bin'], bytes.toString('base64'));
  assert.equal(wire['x-id'], 'abc');
  const back = proxyDecode(wire);
  assert.ok(back['biz-result-bin'].equals(bytes));
});

test('完整 late trailer：代理可见真实 grpc-status 与业务 trailer', () => {
  const { state } = runScript({
    headers: { ':status': 200, 'content-type': 'application/grpc' },
    messages: [{ payload: 'm1' }],
    trailers: {
      'grpc-status': String(GRPC_STATUS.OK),
      'biz-result-bin': Buffer.from('{"page":1}')
    }
  });
  const view = buildProxyView(state);
  assert.equal(view.trailersReceived, true);
  assert.equal(view.trailerTiming, 'late');
  assert.equal(view.visibleGrpcStatus, GRPC_STATUS.OK);
  assert.equal(view.httpStatus, 200);
  assert.equal(view.businessTrailerVisible, true);
  assert.equal(view.sawPartialData, true);
  assert.deepEqual(view.events, ['response-headers', 'data#0', 'trailers']);
});

test('trailer 前流断开：代理只见推断的 non-OK，错误详情不可见', () => {
  const { state } = runScript({
    headers: { ':status': 200 },
    messages: [{ payload: 'm1' }],
    abort: { kind: 'aborted', message: 'tcp reset', rstCode: 'PROTOCOL_ERROR' }
  });
  const view = buildProxyView(state, true);
  assert.equal(view.trailersReceived, false);
  assert.equal(view.visibleGrpcStatus, GRPC_STATUS.INTERNAL);
  assert.equal(view.httpStatus, 500);
  assert.equal(view.statusDetailsVisible, false);
  assert.equal(view.businessTrailerVisible, false);
  assert.equal(view.sawPartialData, true);
  assert.ok(view.events.includes('aborted'));
  // 线上视角的 RST 原因仍可用于重建，但 grpc-message 永远拿不到。
  assert.equal(view.rstCode, 'PROTOCOL_ERROR');
});

test('无 trailer 的连接中断：默认推断 UNAVAILABLE/503', () => {
  const { state } = runScript({
    headers: { ':status': 200 },
    abort: { kind: 'connect', message: 'connection refused' }
  });
  const view = buildProxyView(state);
  assert.equal(view.visibleGrpcStatus, GRPC_STATUS.UNAVAILABLE);
  assert.equal(view.httpStatus, 503);
});

test('reconstruct：trailer 存在时以 trailer 为权威来源', () => {
  const { state } = runScript({
    trailers: { 'grpc-status': String(GRPC_STATUS.NOT_FOUND) }
  });
  const view = buildProxyView(state);
  const r = reconstructGrpcStatusFromHttp(view);
  assert.equal(r.source, 'trailer');
  assert.equal(r.grpcCode, GRPC_STATUS.NOT_FOUND);
  assert.equal(r.diff, null);
});

test('reconstruct：trailer 丢失时从 HTTP 推断并标注 diff', () => {
  const { state } = runScript({
    headers: { ':status': 400 },
    abort: { kind: 'aborted' }
  });
  const view = buildProxyView(state);
  const r = reconstructGrpcStatusFromHttp(view);
  assert.equal(r.source, 'http-status');
  // 流在 trailer 前中断，代理最终暴露推断出的 503 -> UNAVAILABLE。
  assert.equal(r.grpcCode, GRPC_STATUS.UNAVAILABLE);
  assert.match(r.diff, /trailer missing/);
});

test('reconstruct：RST 来源标注 rst-stream', () => {
  const { state } = runScript({
    headers: { ':status': 200 },
    abort: { kind: 'rst', rstCode: 'REFUSED_STREAM' }
  });
  const r = reconstructGrpcStatusFromHttp(buildProxyView(state));
  assert.equal(r.source, 'rst-stream');
  assert.equal(r.grpcCode, GRPC_STATUS.UNAVAILABLE);
});

test('proxyTransform：线上/内部双视角 + 二进制完整性校验', () => {
  const bytes = Buffer.from([0, 127, 255]);
  const { state } = runScript({
    headers: { ':status': 200 },
    trailers: {
      'grpc-status': String(GRPC_STATUS.UNAVAILABLE),
      'grpc-status-details-bin': bytes
    }
  });
  const out = proxyTransform(state);
  assert.equal(typeof out.wire.wireTrailers['grpc-status-details-bin'], 'string');
  assert.ok(
    out.internal.wireTrailers['grpc-status-details-bin'] instanceof Buffer
  );
  assert.ok(
    out.binaryIntegrity.some(
      (c) => c.key === 'grpc-status-details-bin' && c.roundTripOk
    )
  );
});

test('checkBinaryIntegrity 覆盖 headers 与 trailers 两个桶', () => {
  const { state } = runScript({
    headers: { ':status': 200, 'auth-bin': Buffer.from('token') },
    trailers: { 'grpc-status': '0', 'biz-result-bin': Buffer.from('{}') }
  });
  const checks = checkBinaryIntegrity(state);
  assert.deepEqual(
    checks.map((c) => c.bucket).sort(),
    ['headers', 'trailers']
  );
  assert.ok(checks.every((c) => c.roundTripOk));
});
