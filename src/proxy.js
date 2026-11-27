import { GRPC_STATUS, grpcToHttp, httpToGrpc } from './status.js';
import { encodeMetadata, decodeMetadata, isBinaryKey } from './metadata.js';
import { rstToGrpc } from './errors.js';

// 代理转发：内部 metadata -> 线上 metadata（-bin 键 base64 化）。
export function proxyEncode(internalMetadata) {
  return encodeMetadata(internalMetadata);
}

// 代理收到线上 metadata -> 内部形式。
export function proxyDecode(wireMetadata) {
  return decodeMetadata(wireMetadata);
}

// 根据一条流的最终状态，构造代理视角看到的东西。
// 关键问题：流在 trailer 前断开时，代理只有 HTTP/RST 层面的 non-OK 状态，
// 看不到原始 grpc-status / grpc-message / status-details。
export function buildProxyView(streamState, wire = false) {
  const events = [];
  if (streamState.headersReceived) events.push('response-headers');
  for (const msg of streamState.deliveredMessages ?? []) events.push(`data#${msg.seq}`);
  if (streamState.trailersReceived) events.push('trailers');
  if (streamState.aborted) events.push('aborted');

  const view = {
    events,
    trailersReceived: Boolean(streamState.trailersReceived),
    trailerTiming: streamState.trailerTiming ?? null,
    sawPartialData: (streamState.deliveredMessages?.length ?? 0) > 0,
    httpStatus: null,
    visibleGrpcStatus: null,
    visibleGrpcMessage: null,
    statusDetailsVisible: false,
    businessTrailerVisible: false,
    rstCode: streamState.abortReason?.rstCode ?? null,
    wireHeaders: null,
    wireTrailers: null
  };

  if (streamState.headersReceived) {
    view.wireHeaders = wire
      ? proxyEncode(streamState.headers)
      : { ...streamState.headers };
    const headerStatus = Number(streamState.headers?.[':status']);
    if (Number.isInteger(headerStatus)) view.httpStatus = headerStatus;
  }

  if (streamState.trailersReceived && streamState.trailers) {
    view.wireTrailers = wire
      ? proxyEncode(streamState.trailers)
      : { ...streamState.trailers };
    const code = Number(streamState.trailers['grpc-status']);
    if (Number.isInteger(code)) {
      view.visibleGrpcStatus = code;
      view.httpStatus = view.httpStatus ?? grpcToHttp(code);
      view.visibleGrpcMessage = streamState.trailers['grpc-message'] ?? null;
      view.statusDetailsVisible =
        streamState.trailers['grpc-status-details-bin'] != null;
      view.businessTrailerVisible =
        streamState.trailers['biz-result'] != null ||
        streamState.trailers['biz-result-bin'] != null;
    }
  } else if (streamState.aborted) {
    // trailer 丢失：代理只能依据 RST / 连接中断推断 HTTP 状态。
    const reason = streamState.abortReason ?? {};
    const inferred = reason.rstCode
      ? rstToGrpc(reason.rstCode)
      : GRPC_STATUS.UNAVAILABLE;
    view.visibleGrpcStatus = inferred;
    // 即使响应头里带过 200，流在 trailer 前断开时代理最终对外暴露的是推断错误状态。
    view.httpStatus = grpcToHttp(inferred);
    view.statusDetailsVisible = false;
  }

  return view;
}

// 反向转换：代理只能拿到 HTTP 状态时，重建一个“代理推断”的 gRPC 状态。
// 重建结果可能与真实 grpc-status 不同（例如 HTTP 400 -> INTERNAL，
// 而真实业务码可能是 INVALID_ARGUMENT），差异会在 diff 字段里体现。
export function reconstructGrpcStatusFromHttp(proxyView) {
  if (proxyView.visibleGrpcStatus != null && proxyView.trailersReceived) {
    return {
      grpcCode: proxyView.visibleGrpcStatus,
      source: 'trailer',
      diff: null
    };
  }
  if (proxyView.rstCode) {
    return {
      grpcCode: proxyView.visibleGrpcStatus,
      source: 'rst-stream',
      inferredHttpStatus: proxyView.httpStatus,
      diff: 'trailer missing: reconstructed gRPC status may differ from the original'
    };
  }
  const httpStatus = proxyView.httpStatus ?? 503;
  const grpcCode = httpToGrpc(httpStatus);
  return {
    grpcCode,
    source: 'http-status',
    inferredHttpStatus: httpStatus,
    diff: 'trailer missing: reconstructed gRPC status may differ from the original'
  };
}

// 模拟代理做的一次完整转换，返回线上/内部两个视角以及重建结果。
export function proxyTransform(streamState) {
  const wireView = buildProxyView(streamState, true);
  const internalView = buildProxyView(streamState, false);

  // 校验二进制 metadata 往返：代理转发不能损坏 -bin 字节。
  const binaryIntegrity = checkBinaryIntegrity(streamState);

  return {
    wire: wireView,
    internal: internalView,
    reconstruction: reconstructGrpcStatusFromHttp(wireView),
    binaryIntegrity
  };
}

export function checkBinaryIntegrity(streamState) {
  const checks = [];
  const buckets = [
    ['headers', streamState.headers],
    ['trailers', streamState.trailers]
  ];
  for (const [bucket, metadata] of buckets) {
    if (!metadata) continue;
    for (const [key, value] of Object.entries(metadata)) {
      if (!isBinaryKey(key)) continue;
      const encoded = encodeMetadata({ [key]: value })[key];
      const decoded = decodeMetadata({ [key]: encoded })[key];
      const same =
        value instanceof Uint8Array &&
        decoded instanceof Uint8Array &&
        Buffer.from(value).equals(Buffer.from(decoded));
      checks.push({ bucket, key, roundTripOk: same });
    }
  }
  return checks;
}
