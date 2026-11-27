import { GRPC_STATUS, grpcStatusName } from './status.js';
import { parseStatusDetails } from './trailers.js';

// HTTP/2 RST_STREAM 错误码到 gRPC 状态的映射，与 grpc-go 的 transport 行为对齐。
export const RST_TO_GRPC = {
  NO_ERROR: GRPC_STATUS.INTERNAL,
  PROTOCOL_ERROR: GRPC_STATUS.INTERNAL,
  INTERNAL_ERROR: GRPC_STATUS.INTERNAL,
  FLOW_CONTROL_ERROR: GRPC_STATUS.INTERNAL,
  SETTINGS_TIMEOUT: GRPC_STATUS.INTERNAL,
  STREAM_CLOSED: GRPC_STATUS.INTERNAL,
  FRAME_SIZE_ERROR: GRPC_STATUS.INTERNAL,
  REFUSED_STREAM: GRPC_STATUS.UNAVAILABLE,
  CANCEL: GRPC_STATUS.CANCELLED,
  COMPRESSION_ERROR: GRPC_STATUS.INTERNAL,
  CONNECT_ERROR: GRPC_STATUS.UNAVAILABLE,
  ENHANCE_YOUR_CALM: GRPC_STATUS.RESOURCE_EXHAUSTED,
  INADEQUATE_SECURITY: GRPC_STATUS.PERMISSION_DENIED,
  HTTP_1_1_REQUIRED: GRPC_STATUS.PERMISSION_DENIED
};

export function rstToGrpc(rstCode) {
  return RST_TO_GRPC[rstCode] ?? GRPC_STATUS.UNAVAILABLE;
}

export class GrpcStreamError extends Error {
  constructor(code, message, options = {}) {
    super(message);
    this.name = 'GrpcStreamError';
    this.isGrpcStreamError = true;
    this.code = code;
    this.codeName = grpcStatusName(code);
    this.details = options.details ?? '';
    this.debugInfo = options.debugInfo ?? null;
    this.metadata = options.metadata ?? {};
    this.trailersReceived = options.trailersReceived ?? false;
    this.trailerTiming = options.trailerTiming ?? null;
    this.rstCode = options.rstCode ?? null;
    this.attempt = options.attempt ?? null;
  }
}

function decodePercent(s) {
  try {
    return decodeURIComponent(s);
  } catch {
    return s;
  }
}

// 从收到的 trailer 构造终端错误。
export function buildErrorFromTrailers(trailers, attempt) {
  const code = Number(trailers['grpc-status']);
  const rawMessage = trailers['grpc-message'] ?? '';
  const metadata = { ...trailers };
  delete metadata['grpc-status'];
  delete metadata['grpc-message'];
  delete metadata['grpc-status-details-bin'];
  delete metadata['grpc-retry-pushback-ms'];
  return new GrpcStreamError(code, decodePercent(rawMessage), {
    details: rawMessage,
    debugInfo: parseStatusDetails(trailers['grpc-status-details-bin']),
    metadata,
    trailersReceived: true,
    trailerTiming: trailers.__timing ?? null,
    attempt
  });
}

// trailer 缺失（流在终端帧之前断开 / RST / 连接失败）时合成 UNAVAILABLE，
// 并保留底层原因，避免把原始错误详情彻底吞掉。
export function buildSynthesizedError(reason = {}, attempt) {
  const code = reason.rstCode ? rstToGrpc(reason.rstCode) : GRPC_STATUS.UNAVAILABLE;
  const base = reason.message ?? reason.kind ?? 'stream terminated before trailers';
  return new GrpcStreamError(
    code,
    `trailers missing: ${base}`,
    {
      details: reason.message ?? base,
      debugInfo: reason.debugInfo ?? null,
      metadata: reason.metadata ?? {},
      trailersReceived: false,
      trailerTiming: null,
      rstCode: reason.rstCode ?? null,
      attempt
    }
  );
}
