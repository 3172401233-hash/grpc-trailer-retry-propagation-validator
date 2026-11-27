// gRPC 二进制 metadata：键以 `-bin` 结尾时承载 base64 编码的字节。
// 普通 metadata 只能是 ASCII 字符串，二进制 metadata 必须是 Buffer/Uint8Array。

const BIN_SUFFIX = '-bin';

export function isBinaryKey(key) {
  return key.toLowerCase().endsWith(BIN_SUFFIX);
}

export function normalizeKey(key) {
  return String(key).toLowerCase();
}

function assertAscii(key, value) {
  if (typeof value !== 'string' || /[^\x20-\x7e]/.test(value)) {
    throw new TypeError(
      `metadata "${key}" 不是 -bin 键，值必须是可打印 ASCII 字符串`
    );
  }
}

// 把内部 metadata（Buffer 或 string）转成线上形式（base64 字符串或 ASCII 字符串）。
export function encodeMetadata(input = {}) {
  const out = {};
  for (const [rawKey, value] of Object.entries(input)) {
    const key = normalizeKey(rawKey);
    if (isBinaryKey(key)) {
      if (!(value instanceof Uint8Array)) {
        throw new TypeError(`二进制 metadata "${key}" 的值必须是 Buffer/Uint8Array`);
      }
      out[key] = Buffer.from(value).toString('base64');
    } else {
      const text = typeof value === 'number' ? String(value) : value;
      assertAscii(key, text);
      out[key] = text;
    }
  }
  return out;
}

// 把线上 metadata 还原成内部形式：-bin 键解码为 Buffer，其余保持字符串。
export function decodeMetadata(wire = {}) {
  const out = {};
  for (const [rawKey, value] of Object.entries(wire)) {
    const key = normalizeKey(rawKey);
    if (isBinaryKey(key)) {
      if (value instanceof Uint8Array) {
        out[key] = Buffer.from(value);
      } else if (typeof value === 'string') {
        out[key] = Buffer.from(value, 'base64');
      } else {
        throw new TypeError(`二进制 metadata "${key}" 的线上值必须是 base64 字符串`);
      }
    } else if (typeof value !== 'string' && typeof value !== 'number') {
      throw new TypeError(`metadata "${key}" 的线上值必须是字符串或数字`);
    } else {
      out[key] = String(value);
    }
  }
  return out;
}

// 往返校验：编码 -> 解码必须还原出相同的字节/字符串。
export function roundTrip(input = {}) {
  return decodeMetadata(encodeMetadata(input));
}
