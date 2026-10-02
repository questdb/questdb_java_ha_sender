// Timestamps reach the page as DIGIT STRINGS: QWP carries 64-bit integers as BigInt, and
// BigInt is not JSON-serialisable, so server.mjs stringifies them.
//
// The unit is not fixed. QuestDB carries both microsecond and nanosecond timestamps, and a
// column's unit is not visible in the value itself, only in its magnitude. Assuming one
// silently renders the other as 1970, which is exactly what the scan tab did to core_price.
export const EPOCH_DIGITS = /^\d{13,19}$/;

/**
 * Interpret an epoch digit string, inferring the unit from magnitude.
 *
 * The thresholds are far from any plausible date: 1e14 ms and 1e17 us both land in the year
 * 5138, so a real millisecond timestamp can never be mistaken for microseconds, nor
 * microseconds for nanoseconds, for any date this side of the fourth millennium.
 */
export function epochDate(digits) {
  const n = Number(digits);
  if (n >= 1e17) return new Date(n / 1e6);   // nanoseconds
  if (n >= 1e14) return new Date(n / 1e3);   // microseconds
  return new Date(n);                        // milliseconds
}

/** `HH:MM:SS.mmm`, for panels where every row is from the last minute anyway. */
export const epochTime = (digits) => epochDate(digits).toISOString().slice(11, 23);

/** `YYYY-MM-DD HH:MM:SS.mmm`, for rows that can come from anywhere in a table's history. */
export const epochStamp = (digits) =>
  epochDate(digits).toISOString().replace("T", " ").slice(0, 23);

const u64 = (v) => BigInt.asUintN(64, BigInt(v)).toString(16).padStart(16, "0");

/**
 * Render a QWP value that is not a plain scalar.
 *
 * Several QuestDB types cross the wire as structured objects rather than strings: a UUID is
 * a {low, high} pair of 64-bit halves, a LONG256 is four little-endian words, a DECIMAL is an
 * unscaled integer plus a scale. Without this they all render as "[object Object]", which is
 * what SELECT * on fx_trades produced.
 */
export function fmtStructured(v) {
  if (v.low !== undefined && v.high !== undefined) {
    const hex = u64(v.high) + u64(v.low);
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${
      hex.slice(16, 20)}-${hex.slice(20)}`;
  }
  if (Array.isArray(v.words)) {
    // Word 0 is least significant, so the printed order is the reverse of the array.
    return `0x${[...v.words].reverse().map(u64).join("")}`;
  }
  if (v.unscaled !== undefined) {
    const digits = BigInt(v.unscaled).toString();
    if (!v.scale) return digits;
    const neg = digits.startsWith("-");
    const body = (neg ? digits.slice(1) : digits).padStart(v.scale + 1, "0");
    return `${neg ? "-" : ""}${body.slice(0, -v.scale)}.${body.slice(-v.scale)}`;
  }
  if (Array.isArray(v.dimensions)) return `array[${v.dimensions.join("x")}]`;
  if (v.bits !== undefined) return `geohash(${v.bits})`;
  return JSON.stringify(v);
}
