import { GRPC_STATUS } from './status.js';

// 业务部分结果约定：
//   biz-result      : ASCII/JSON 字符串 trailer
//   biz-result-bin  : 二进制 trailer（Buffer，内容通常是 UTF-8 JSON）
export const BIZ_RESULT_KEY = 'biz-result';
export const BIZ_RESULT_BIN_KEY = 'biz-result-bin';

// trailer 到达时机：流还没向应用投递任何消息 -> early；已有消息投递 -> late。
export function classifyTrailerTiming(streamState) {
  if (!streamState || !streamState.trailersReceived) return null;
  return streamState.messagesDelivered > 0 ? 'late' : 'early';
}

export function parseGrpcStatus(trailers) {
  if (!trailers || !Object.prototype.hasOwnProperty.call(trailers, 'grpc-status')) {
    return null;
  }
  const code = Number(trailers['grpc-status']);
  return Number.isInteger(code) && code >= 0 && code <= 16 ? code : null;
}

// 简化版 google.rpc.Status details 解析：优先按 UTF-8 JSON 解码，
// 解不出来时保留原始 Buffer，不丢失信息。
export function parseStatusDetails(binValue) {
  if (binValue == null) return null;
  const buf = binValue instanceof Uint8Array ? Buffer.from(binValue) : null;
  if (!buf) return binValue;
  try {
    return JSON.parse(buf.toString('utf8'));
  } catch {
    return buf;
  }
}

function tryJson(text) {
  try {
    return { value: JSON.parse(text), json: true };
  } catch {
    return { value: text, json: false };
  }
}

// 提取 trailer 中携带的业务部分结果。
export function extractBusinessResults(trailers) {
  const results = [];
  if (!trailers) return results;

  if (trailers[BIZ_RESULT_BIN_KEY] instanceof Uint8Array) {
    const text = Buffer.from(trailers[BIZ_RESULT_BIN_KEY]).toString('utf8');
    const parsed = tryJson(text);
    results.push({
      source: BIZ_RESULT_BIN_KEY,
      encoding: 'binary',
      value: parsed.value,
      json: parsed.json
    });
  }

  if (typeof trailers[BIZ_RESULT_KEY] === 'string') {
    const parsed = tryJson(trailers[BIZ_RESULT_KEY]);
    results.push({
      source: BIZ_RESULT_KEY,
      encoding: 'text',
      value: parsed.value,
      json: parsed.json
    });
  }

  return results;
}

// 综合解析一次流结束时拿到的 trailer。
export function parseTrailers(trailers, streamState) {
  const code = parseGrpcStatus(trailers);
  const timing = classifyTrailerTiming(streamState);
  return {
    received: trailers != null,
    timing,
    code,
    ok: code === GRPC_STATUS.OK,
    message: trailers?.['grpc-message'] ?? null,
    pushbackMs:
      trailers?.['grpc-retry-pushback-ms'] != null
        ? Number(trailers['grpc-retry-pushback-ms'])
        : null,
    statusDetails: parseStatusDetails(trailers?.['grpc-status-details-bin']),
    businessResults: extractBusinessResults(trailers),
    raw: trailers ?? null
  };
}
