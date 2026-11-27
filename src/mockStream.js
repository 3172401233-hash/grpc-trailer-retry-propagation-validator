import { EventEmitter } from 'node:events';

// 纯内存模拟的 HTTP/2 流：不打开任何 socket / 连接，
// 仅按脚本顺序发出 response headers、DATA 消息、trailers 或中断事件，
// 并维护一份供决策层读取的状态快照。
export class MockHttp2Stream extends EventEmitter {
  constructor() {
    super();
    this.state = {
      headers: null,
      headersReceived: false,
      messagesBuffered: [],
      deliveredMessages: [],
      trailers: null,
      trailersReceived: false,
      trailerTiming: null,
      aborted: false,
      endReason: null,
      abortReason: null,
      clientCancelled: false,
      closed: false
    };
  }

  emitHeaders(headers) {
    this.state.headers = { ...headers };
    this.state.headersReceived = true;
    this.emit('response', this.state.headers);
    return this;
  }

  // deliver=true 表示消息已经越过透明重试缓冲、交给应用（流产生了部分输出）。
  pushMessage(payload, { deliver = true } = {}) {
    const envelope = { seq: this.state.messagesBuffered.length, payload };
    this.state.messagesBuffered.push(envelope);
    if (deliver) {
      this.state.deliveredMessages.push(envelope);
      this.emit('data', envelope);
    }
    return this;
  }

  emitTrailers(trailers) {
    this.state.trailers = { ...trailers };
    this.state.trailersReceived = true;
    this.state.trailerTiming =
      this.state.deliveredMessages.length > 0 ? 'late' : 'early';
    this.emit('trailers', this.state.trailers);
    this.close();
    return this;
  }

  // reason: { kind, rstCode?, message?, debugInfo?, metadata? }
  abort(reason = { kind: 'aborted' }) {
    this.state.aborted = true;
    this.state.endReason = reason.kind ?? 'aborted';
    this.state.abortReason = reason;
    const err = new Error(reason.message ?? this.state.endReason);
    err.reason = reason;
    this.emit('error', err);
    this.close();
    return this;
  }

  cancel() {
    this.state.clientCancelled = true;
    this.state.endReason = 'cancelled';
    this.emit('cancelled');
    this.close();
    return this;
  }

  close() {
    if (!this.state.closed) {
      this.state.closed = true;
      this.emit('close', this.snapshot());
    }
  }

  snapshot() {
    return {
      ...this.state,
      headers: this.state.headers ? { ...this.state.headers } : null,
      trailers: this.state.trailers ? { ...this.state.trailers } : null,
      messagesBuffered: [...this.state.messagesBuffered],
      deliveredMessages: [...this.state.deliveredMessages]
    };
  }
}

// 按脚本驱动一条模拟流，返回最终状态快照。
// script:
// {
//   headers: {...},
//   messages: [{ payload, deliver? }, ...],
//   delivery: 'deliver' | 'buffer',   // 消息默认投递方式
//   trailers: {...} | null,
//   abort: { kind, rstCode?, message?, debugInfo? } | null
// }
export function runScript(script = {}) {
  const stream = new MockHttp2Stream();
  const events = [];
  for (const name of ['response', 'data', 'trailers', 'error', 'cancelled', 'close']) {
    stream.on(name, (payload) => events.push({ name, payload }));
  }

  if (script.headers) stream.emitHeaders(script.headers);

  const defaultDeliver = script.delivery !== 'buffer';
  for (const item of script.messages ?? []) {
    const deliver = item.deliver ?? defaultDeliver;
    stream.pushMessage(item.payload, { deliver });
  }

  if (script.trailers) {
    stream.emitTrailers(script.trailers);
  } else if (script.abort) {
    stream.abort(script.abort);
  } else {
    stream.close();
  }

  return { stream, state: stream.snapshot(), events };
}
