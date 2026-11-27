import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  isBinaryKey,
  normalizeKey,
  encodeMetadata,
  decodeMetadata,
  roundTrip
} from '../src/metadata.js';

test('-bin 后缀识别与键名规范化', () => {
  assert.equal(isBinaryKey('grpc-status-details-bin'), true);
  assert.equal(isBinaryKey('x-request-id'), false);
  assert.equal(normalizeKey('Content-Type'), 'content-type');
});

test('二进制 metadata 编码为 base64，解码还原原始字节', () => {
  const bytes = Buffer.from([0x00, 0x01, 0x02, 0xff, 0xfe]);
  const wire = encodeMetadata({ 'biz-result-bin': bytes });
  assert.equal(typeof wire['biz-result-bin'], 'string');
  assert.equal(wire['biz-result-bin'], bytes.toString('base64'));

  const back = decodeMetadata(wire);
  assert.ok(back['biz-result-bin'] instanceof Buffer);
  assert.ok(back['biz-result-bin'].equals(bytes));
});

test('普通 metadata 保持 ASCII 字符串，往返一致', () => {
  const input = { 'x-request-id': 'abc-123', 'grpc-encoding': 'identity' };
  assert.deepEqual(roundTrip(input), input);
});

test('二进制内容含非 ASCII 字节时往返不损坏', () => {
  const jsonPayload = Buffer.from(JSON.stringify({ kept: true, n: 42 }), 'utf8');
  const back = roundTrip({ 'biz-result-bin': jsonPayload });
  assert.deepEqual(JSON.parse(back['biz-result-bin'].toString('utf8')), {
    kept: true,
    n: 42
  });
});

test('非 -bin 键传非 ASCII 字符串会抛错', () => {
  assert.throws(() => encodeMetadata({ 'x-bad': '中文' }), /ASCII/);
});

test('-bin 键传字符串而非 Buffer 会抛错', () => {
  assert.throws(() => encodeMetadata({ 'x-bad-bin': 'not-bytes' }), /Buffer/);
});

test('解码非法类型会抛错而非静默吞掉', () => {
  assert.throws(() => decodeMetadata({ 'x-bad': true }), /字符串或数字/);
});
