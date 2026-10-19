"""Lua sources for the Redis backend.

Each script receives one key per rate and runs in two phases inside Redis:
evaluate every rate, then commit only if all of them allow the hit. Redis runs
a script atomically, so concurrent callers can never interleave between the
read and the write.

KEYS[i]       state key for rate i
ARGV[1]       now in milliseconds, or "" to use the Redis server clock
ARGV[2]       cost
ARGV[3]       unique token for this call (sliding log members)
ARGV[2+2i]    limit for rate i
ARGV[3+2i]    period in milliseconds for rate i

Returns {all_ok, ok_1, remaining_1, retry_ms_1, reset_ms_1, ok_2, ...}.
"""

from __future__ import annotations

_PROLOGUE = """
local now
if ARGV[1] == '' then
  local t = redis.call('TIME')
  now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
else
  now = tonumber(ARGV[1])
end
local cost = tonumber(ARGV[2])
local token = ARGV[3]
local EPS = 1e-6
local function num(x) return string.format('%.17g', x) end
local function ttl(ms)
  local v = math.ceil(ms)
  if v < 1 then v = 1 end
  return v
end
"""

_EPILOGUE = """
local evals = {}
local all_ok = 1
for i = 1, #KEYS do
  local e = evaluate(KEYS[i], tonumber(ARGV[2 + 2 * i]), tonumber(ARGV[3 + 2 * i]))
  if not e.ok then all_ok = 0 end
  evals[i] = e
end
local out = {all_ok}
for i = 1, #KEYS do
  local e = evals[i]
  local remaining, reset, retry = e.remaining, e.reset, e.retry
  if all_ok == 1 then
    reset = e.commit()
    remaining = remaining - cost
  end
  if remaining < 0 then remaining = 0 end
  if retry < 0 then retry = 0 end
  if reset < 0 then reset = 0 end
  out[#out + 1] = e.ok and 1 or 0
  out[#out + 1] = math.floor(remaining)
  out[#out + 1] = math.ceil(retry)
  out[#out + 1] = math.ceil(reset)
end
return out
"""

FIXED_WINDOW = """
local function evaluate(key, limit, period)
  local window = math.floor(now / period)
  local data = redis.call('HMGET', key, 'w', 'c')
  local count = 0
  if tonumber(data[1]) == window then count = tonumber(data[2]) end
  local ok = count + cost <= limit
  local reset = (window + 1) * period - now
  local retry = 0
  if not ok then retry = reset end
  return {ok = ok, remaining = limit - count, retry = retry, reset = reset,
    commit = function()
      redis.call('HSET', key, 'w', num(window), 'c', num(count + cost))
      redis.call('PEXPIRE', key, ttl(reset))
      return reset
    end}
end
"""

SLIDING_LOG = """
local function evaluate(key, limit, period)
  redis.call('ZREMRANGEBYSCORE', key, '-inf', num(now - period))
  local count = redis.call('ZCARD', key)
  local ok = count + cost <= limit
  local retry = 0
  if not ok then
    local idx = count + cost - limit - 1
    local entry = redis.call('ZRANGE', key, idx, idx, 'WITHSCORES')
    retry = tonumber(entry[2]) + period - now
  end
  local reset = 0
  local newest = redis.call('ZRANGE', key, -1, -1, 'WITHSCORES')
  if newest[2] then reset = tonumber(newest[2]) + period - now end
  return {ok = ok, remaining = limit - count, retry = retry, reset = reset,
    commit = function()
      for j = 1, cost do
        redis.call('ZADD', key, num(now), token .. ':' .. j)
      end
      redis.call('PEXPIRE', key, ttl(period))
      return period
    end}
end
"""

SLIDING_WINDOW = """
local function evaluate(key, limit, period)
  local window = math.floor(now / period)
  local data = redis.call('HMGET', key, 'w', 'c', 'p')
  local stored = tonumber(data[1])
  local curr, prev = 0, 0
  if stored == window then
    curr, prev = tonumber(data[2]), tonumber(data[3])
  elseif stored == window - 1 then
    prev = tonumber(data[2])
  end
  local elapsed = now - window * period
  local estimate = prev * (1 - elapsed / period) + curr
  local ok = estimate + cost <= limit + EPS
  local reset = period - elapsed
  local retry = 0
  if not ok then
    if curr + cost <= limit and prev > 0 then
      retry = period * (1 - (limit - curr - cost) / prev) - elapsed
    else
      local need = 0
      if curr > 0 then need = period * (1 - (limit - cost) / curr) end
      if need < 0 then need = 0 end
      retry = (period - elapsed) + need
    end
  end
  return {ok = ok, remaining = limit - estimate, retry = retry, reset = reset,
    commit = function()
      redis.call('HSET', key, 'w', num(window), 'c', num(curr + cost), 'p', num(prev))
      redis.call('PEXPIRE', key, ttl(2 * period))
      return reset
    end}
end
"""

TOKEN_BUCKET = """
local function evaluate(key, limit, period)
  local data = redis.call('HMGET', key, 't', 'ts')
  local tokens, ts = tonumber(data[1]), tonumber(data[2])
  if tokens == nil then
    tokens, ts = limit, now
  elseif now > ts then
    tokens = math.min(limit, tokens + (now - ts) * limit / period)
    ts = now
  end
  local ok = tokens + EPS >= cost
  local retry = 0
  if not ok then retry = (cost - tokens) * period / limit end
  local reset = (limit - tokens) * period / limit
  return {ok = ok, remaining = tokens, retry = retry, reset = reset,
    commit = function()
      local left = tokens - cost
      local full_in = (limit - left) * period / limit
      redis.call('HSET', key, 't', num(left), 'ts', num(ts))
      redis.call('PEXPIRE', key, ttl(full_in))
      return full_in
    end}
end
"""

GCRA = """
local function evaluate(key, limit, period)
  local interval = period / limit
  local tat = tonumber(redis.call('GET', key)) or now
  if tat < now then tat = now end
  local new_tat = tat + interval * cost
  local allow_at = new_tat - period
  local ok = allow_at - now <= EPS
  local retry = 0
  if not ok then retry = allow_at - now end
  return {ok = ok, remaining = (now + period - tat) / interval + EPS, retry = retry,
    reset = tat - now,
    commit = function()
      redis.call('SET', key, num(new_tat), 'PX', ttl(new_tat - now))
      return new_tat - now
    end}
end
"""


def build(body: str) -> str:
    return _PROLOGUE + body + _EPILOGUE


SCRIPTS = {
    "fixed_window": build(FIXED_WINDOW),
    "sliding_log": build(SLIDING_LOG),
    "sliding_window": build(SLIDING_WINDOW),
    "token_bucket": build(TOKEN_BUCKET),
    "gcra": build(GCRA),
}
