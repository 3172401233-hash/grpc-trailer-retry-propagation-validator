import { runScript } from './mockStream.js';
import { decideRetry } from './retry.js';
import { proxyTransform } from './proxy.js';
import { extractBusinessResults } from './trailers.js';
import { GrpcStreamError } from './errors.js';
import { GRPC_STATUS } from './status.js';

// 多次尝试的执行器。attempts 是脚本数组，或 (attemptNumber) => script 的工厂。
// 不做任何真实等待：退避时间只记录数值，由测试决定是否 sleep。
export function executeWithRetry(attempts, options = {}) {
  const policy = options.policy;
  const clientCancelled = Boolean(options.clientCancelled);
  const log = [];

  let attempt = 1;
  let totalDelivered = [];
  let retainedTrailerResults = [];

  while (true) {
    const script = typeof attempts === 'function' ? attempts(attempt) : attempts[attempt - 1];
    const { state } = runScript(script ?? {});
    const proxy = proxyTransform(state);
    const decision = decideRetry({
      attempt,
      streamState: state,
      trailers: state.trailers,
      reason: state.abortReason,
      clientCancelled,
      policy,
      rng: options.rng
    });

    // 已投递的消息一旦越过缓冲就永久保留（无论后面重试与否）。
    totalDelivered = [...totalDelivered, ...state.deliveredMessages];
    if (!decision.retryAllowed && state.trailersReceived) {
      retainedTrailerResults = extractBusinessResults(state.trailers);
    }

    log.push({
      attempt,
      state: summarizeState(state),
      proxy: proxy.wire,
      reconstruction: proxy.reconstruction,
      decision: {
        retryAllowed: decision.retryAllowed,
        reasonCode: decision.reasonCode,
        nextAttempt: decision.nextAttempt,
        backoffMs: decision.backoffMs,
        hardNonRetryable: decision.hardNonRetryable,
        retained: decision.retained
      }
    });

    if (!decision.retryAllowed) {
      const ok =
        state.trailersReceived &&
        Number(state.trailers?.['grpc-status']) === GRPC_STATUS.OK;
      return {
        outcome: ok ? 'success' : 'error',
        attempts: attempt,
        attemptsLog: log,
        retainedResults: {
          deliveredMessages: totalDelivered,
          trailerBusinessResults: ok
            ? extractBusinessResults(state.trailers)
            : retainedTrailerResults
        },
        propagatedError: decision.propagatedError
      };
    }

    if (attempt >= (options.maxLoopGuard ?? 32)) {
      throw new GrpcStreamError(
        GRPC_STATUS.INTERNAL,
        'retry loop guard exceeded',
        { attempt }
      );
    }
    attempt = decision.nextAttempt;
  }
}

function summarizeState(state) {
  return {
    headersReceived: state.headersReceived,
    trailersReceived: state.trailersReceived,
    trailerTiming: state.trailerTiming,
    buffered: state.messagesBuffered.length,
    delivered: state.deliveredMessages.length,
    aborted: state.aborted,
    endReason: state.endReason,
    rstCode: state.abortReason?.rstCode ?? null
  };
}
