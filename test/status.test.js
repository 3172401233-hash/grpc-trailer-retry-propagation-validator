import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  GRPC_STATUS,
  grpcToHttp,
  httpToGrpc,
  grpcStatusName
} from '../src/status.js';

test('gRPC -> HTTP：规范映射全部成立', () => {
  assert.equal(grpcToHttp(GRPC_STATUS.OK), 200);
  assert.equal(grpcToHttp(GRPC_STATUS.INVALID_ARGUMENT), 400);
  assert.equal(grpcToHttp(GRPC_STATUS.OUT_OF_RANGE), 400);
  assert.equal(grpcToHttp(GRPC_STATUS.FAILED_PRECONDITION), 400);
  assert.equal(grpcToHttp(GRPC_STATUS.UNAUTHENTICATED), 401);
  assert.equal(grpcToHttp(GRPC_STATUS.PERMISSION_DENIED), 403);
  assert.equal(grpcToHttp(GRPC_STATUS.NOT_FOUND), 404);
  assert.equal(grpcToHttp(GRPC_STATUS.ALREADY_EXISTS), 409);
  assert.equal(grpcToHttp(GRPC_STATUS.ABORTED), 409);
  assert.equal(grpcToHttp(GRPC_STATUS.RESOURCE_EXHAUSTED), 429);
  assert.equal(grpcToHttp(GRPC_STATUS.CANCELLED), 499);
  assert.equal(grpcToHttp(GRPC_STATUS.INTERNAL), 500);
  assert.equal(grpcToHttp(GRPC_STATUS.DATA_LOSS), 500);
  assert.equal(grpcToHttp(GRPC_STATUS.UNKNOWN), 500);
  assert.equal(grpcToHttp(GRPC_STATUS.UNIMPLEMENTED), 501);
  assert.equal(grpcToHttp(GRPC_STATUS.UNAVAILABLE), 503);
  assert.equal(grpcToHttp(GRPC_STATUS.DEADLINE_EXCEEDED), 504);
});

test('未知 gRPC 码回落到 HTTP 500', () => {
  assert.equal(grpcToHttp(99), 500);
  assert.equal(grpcStatusName(99), 'UNKNOWN_CODE(99)');
});

test('HTTP -> gRPC：代理推断的反向映射', () => {
  assert.equal(httpToGrpc(400), GRPC_STATUS.INTERNAL);
  assert.equal(httpToGrpc(401), GRPC_STATUS.UNAUTHENTICATED);
  assert.equal(httpToGrpc(403), GRPC_STATUS.PERMISSION_DENIED);
  assert.equal(httpToGrpc(404), GRPC_STATUS.UNIMPLEMENTED);
  assert.equal(httpToGrpc(409), GRPC_STATUS.ABORTED);
  assert.equal(httpToGrpc(429), GRPC_STATUS.RESOURCE_EXHAUSTED);
  assert.equal(httpToGrpc(499), GRPC_STATUS.CANCELLED);
  assert.equal(httpToGrpc(500), GRPC_STATUS.INTERNAL);
  assert.equal(httpToGrpc(501), GRPC_STATUS.UNIMPLEMENTED);
  assert.equal(httpToGrpc(502), GRPC_STATUS.UNAVAILABLE);
  assert.equal(httpToGrpc(503), GRPC_STATUS.UNAVAILABLE);
  assert.equal(httpToGrpc(504), GRPC_STATUS.DEADLINE_EXCEEDED);
});

test('HTTP 400 推断与真实业务码可能不同（信息丢失的根源）', () => {
  // 真实 grpc-status=INVALID_ARGUMENT(3) 经代理只看 HTTP 时会被还原成 INTERNAL(13)。
  const httpStatus = grpcToHttp(GRPC_STATUS.INVALID_ARGUMENT);
  assert.equal(httpStatus, 400);
  assert.notEqual(httpToGrpc(httpStatus), GRPC_STATUS.INVALID_ARGUMENT);
  assert.equal(httpToGrpc(httpStatus), GRPC_STATUS.INTERNAL);
});

test('未知 HTTP 状态回落到 UNKNOWN', () => {
  assert.equal(httpToGrpc(418), GRPC_STATUS.UNKNOWN);
});
