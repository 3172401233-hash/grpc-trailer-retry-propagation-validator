import { GRPC_STATUS } from './status.js';
import {
  GrpcStreamError,
  buildErrorFromTrailers,
  buildSynthesizedError
} from './errors.js';
import { extractBusinessResults } from './trailers.js';

// 默认可重试状态：仅透明重试语义下安全的瞬时错误。
export const DEFAULT_RETRYABLE_CODES = [
  GRPC_STATUS.UNAVAILABLE,
  GRPC_STATUS.DEADLINE_EXCEEDED
];

// 即使在 transport 失败路径上合成出这些状态，也绝不重试
//（取消是客户端主动行为，鉴权类失败重试无意义且有放大风险）。
export const HARD_NON_RETRYABLE_CODES = [
  GRPC_STATUS.CANCELLED,
  GRPC_STATUS.PERMISSION_DENIED,
  GRPC_STATUS.UNAUTHENTICATED
];

export const DEFAULT_RETRY_POLICY = {
  maxAttempts: 4,
  retryableCodes: DEFAULT_RETRYABLE_CODES,
  retryOnTransportFailure: true,
  initialBackoffMs: 100,
  maxBackoffMs: 1000,
  backoffMultiplier: 2
};

export function mergePolicy(overrides = {}) {
  return { ...DEFAULT_RETRY_POLICY, ...overrides };
}

// 退避：在 initialBackoff 与 min(initial * multiplier^n, maxBackoff) 之间均匀取值。
// rng 可注入，方便测试得到确定性结果。
export function computeBackoff(attempt, policy, rng = Math.random) {
  const { initialBackoffMs, maxBackoffMs, backoffMultiplier } = policy;
  const capped = Math.min(
    initialBackoffMs * backoffMultiplier ** (attempt - 1),
    maxBackoffMs
  );
  return initialBackoffMs + rng() * (capped - initialBackoffMs);
}

// 输入：
//   attempt         当前是第几次尝试（1 起）
//   streamState     模拟流状态（headers/trailers/已投递消息/缓冲消息/中断原因）
//   trailers        已解码的 trailer（内部形式，-bin 为 Buffer），缺失时为 null
//   reason          trailer 缺失时的底层原因 { kind, message, rstCode, debugInfo }
//   clientCancelled 客户端是否主动取消
//   policy          重试策略
export function decideRetry(input = {}) {
  const {
    attempt = 1,
    streamState = {},
    trailers = null,
    reason = null,
    clientCancelled = false,
    policy: rawPolicy
  } = input;
  const policy = rawPolicy ? mergePolicy(rawPolicy) : DEFAULT_RETRY_POLICY;
  const rng = input.rng ?? Math.random;

  const deliveredMessages = streamState.deliveredMessages ?? [];
  const bufferedMessages = streamState.messagesBuffered ?? [];
  const trailerBusinessResults = trailers ? extractBusinessResults(trailers) : [];

  const baseRetention = {
    deliveredMessages: [...deliveredMessages],
    partialOutputCount: deliveredMessages.length,
    trailerBusinessResults,
    discardedBufferedMessages: [],
    staleTrailerResultsOnRetry: false
  };

  const build = (extra) => ({
    attempt,
    nextAttempt: null,
    backoffMs: null,
    hardNonRetryable: false,
    reasonCode: extra.reasonCode,
    retained: { ...baseRetention, ...(extra.retained ?? {}) },
    propagatedError: extra.propagatedError ?? null,
    retryAllowed: false,
    decision: extra.decision ?? 'fail',
    ...extra
  });

  // 1. 客户端主动取消：不可重试，直接传播 CANCELLED。
  if (clientCancelled) {
    return build({
      reasonCode: 'CLIENT_CANCELLED',
      hardNonRetryable: true,
      propagatedError: new GrpcStreamError(
        GRPC_STATUS.CANCELLED,
        'client cancelled the stream',
        { trailersReceived: Boolean(streamState.trailersReceived), attempt }
      )
    });
  }

  // 2. 收到完整 trailer：终端状态以 grpc-status 为准。
  if (streamState.trailersReceived && trailers && trailers['grpc-status'] != null) {
    const code = Number(trailers['grpc-status']);
    const error = buildErrorFromTrailers(trailers, attempt);
    error.trailerTiming = streamState.trailerTiming ?? null;

    if (code === GRPC_STATUS.OK) {
      return { ...build({ reasonCode: 'OK' }), decision: 'complete' };
    }

    // 2a. 流已经向应用投递过消息（late trailer）：透明重试缓冲已失效，硬保护。
    if (deliveredMessages.length > 0) {
      return build({
        reasonCode: 'OUTPUT_ALREADY_DELIVERED',
        hardNonRetryable: true,
        propagatedError: error
      });
    }

    // 2b. 服务端 pushback = -1 明确要求不要重试。
    const pushback = trailers['grpc-retry-pushback-ms'];
    if (pushback != null && Number(pushback) < 0) {
      return build({
        reasonCode: 'SERVER_PUSHBACK_SUPPRESSED',
        hardNonRetryable: true,
        propagatedError: error
      });
    }

    // 2c. 状态码不在可重试集合内：不可重试错误，禁止重发。
    if (!policy.retryableCodes.includes(code)) {
      return build({
        reasonCode: 'NON_RETRYABLE_STATUS',
        hardNonRetryable: true,
        propagatedError: error
      });
    }

    // 2d. 达到最大尝试次数。
    if (attempt >= policy.maxAttempts) {
      return build({
        reasonCode: 'MAX_ATTEMPTS_REACHED',
        propagatedError: error
      });
    }

    // 2e. early trailer + 可重试状态：允许重试。
    const backoffMs =
      pushback != null && Number(pushback) >= 0
        ? Number(pushback)
        : computeBackoff(attempt, policy, rng);
    return {
      attempt,
      nextAttempt: attempt + 1,
      backoffMs,
      hardNonRetryable: false,
      reasonCode: 'RETRYABLE_STATUS',
      retryAllowed: true,
      decision: 'retry',
      retained: {
        ...baseRetention,
        // early trailer 里的业务结果来自失败尝试，标记为陈旧，避免被当成新结果提交。
        staleTrailerResultsOnRetry: trailerBusinessResults.length > 0,
        discardedBufferedMessages: [...bufferedMessages]
      },
      propagatedError: null
    };
  }

  // 3. trailer 丢失：流在终端帧之前断开（RST / 连接中断 / 代理截断）。
  const synthesized = buildSynthesizedError(
    reason ?? { kind: streamState.endReason ?? 'aborted' },
    attempt
  );

  // 3a. 已经产生部分输出：保留已投递消息，禁止重试（重发会产生重复副作用）。
  if (deliveredMessages.length > 0) {
    return build({
      reasonCode: 'OUTPUT_ALREADY_DELIVERED',
      hardNonRetryable: true,
      propagatedError: synthesized
    });
  }

  // 3b. 合成状态命中硬性不可重试集合（如 RST CANCEL）。
  if (HARD_NON_RETRYABLE_CODES.includes(synthesized.code)) {
    return build({
      reasonCode: 'HARD_NON_RETRYABLE_TRANSPORT_CODE',
      hardNonRetryable: true,
      propagatedError: synthesized
    });
  }

  // 3c. 策略不允许 transport 级重试，或次数用尽。
  if (!policy.retryOnTransportFailure) {
    return build({
      reasonCode: 'TRANSPORT_RETRY_DISABLED',
      hardNonRetryable: true,
      propagatedError: synthesized
    });
  }
  if (attempt >= policy.maxAttempts) {
    return build({
      reasonCode: 'MAX_ATTEMPTS_REACHED',
      propagatedError: synthesized
    });
  }

  // 3d. 无输出的 transport 失败：允许重试，缓冲消息丢弃。
  return {
    attempt,
    nextAttempt: attempt + 1,
    backoffMs: computeBackoff(attempt, policy, rng),
    hardNonRetryable: false,
    reasonCode: 'TRANSPORT_FAILURE_BEFORE_OUTPUT',
    retryAllowed: true,
    decision: 'retry',
    retained: {
      ...baseRetention,
      staleTrailerResultsOnRetry: false,
      discardedBufferedMessages: [...bufferedMessages]
    },
    propagatedError: null
  };
}
