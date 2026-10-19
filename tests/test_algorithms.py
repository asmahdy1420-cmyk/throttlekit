"""Behaviour shared by every algorithm, run against every backend."""

from __future__ import annotations

import pytest

from throttlekit import Algorithm

ALL = list(Algorithm)


@pytest.mark.parametrize("algorithm", ALL)
async def test_allows_up_to_limit_then_denies(make_backend, algorithm):
    backend = make_backend(algorithm)
    for expected_remaining in (4, 3, 2, 1, 0):
        result = await backend.hit("user", ["5/s"])
        assert result.allowed
        assert result.binding.remaining == expected_remaining

    denied = await backend.hit("user", ["5/s"])
    assert not denied.allowed
    assert denied.binding.remaining == 0
    # The sliding window counter may need part of the next window too.
    assert 0 < denied.retry_after <= 2.0
    assert int(denied.headers()["Retry-After"]) >= 1


@pytest.mark.parametrize("algorithm", ALL)
async def test_recovers_after_retry_after(make_backend, clock, algorithm):
    backend = make_backend(algorithm)
    for _ in range(5):
        await backend.hit("user", ["5/s"])
    denied = await backend.hit("user", ["5/s"])
    assert not denied.allowed

    clock.advance(denied.retry_after)
    assert (await backend.hit("user", ["5/s"])).allowed


@pytest.mark.parametrize("algorithm", ALL)
async def test_keys_are_isolated(make_backend, algorithm):
    backend = make_backend(algorithm)
    for _ in range(2):
        assert (await backend.hit("alice", ["2/m"])).allowed
    assert not (await backend.hit("alice", ["2/m"])).allowed
    assert (await backend.hit("bob", ["2/m"])).allowed


@pytest.mark.parametrize("algorithm", ALL)
async def test_multiple_rates_are_all_or_nothing(make_backend, clock, algorithm):
    backend = make_backend(algorithm)
    rates = ["3/s", "5/m"]
    for _ in range(3):
        assert (await backend.hit("k", rates)).allowed
    clock.advance(2)
    for _ in range(2):
        assert (await backend.hit("k", rates)).allowed
    clock.advance(2)

    denied = await backend.hit("k", rates)
    assert not denied.allowed
    per_second, per_minute = denied.decisions
    assert per_second.allowed
    assert not per_minute.allowed
    assert denied.binding is per_minute
    # The per-second rate allowed the hit but was not charged for it.
    assert per_second.remaining == 3
    for _ in range(3):
        assert (await backend.hit("k", ["3/s"])).allowed


@pytest.mark.parametrize("algorithm", ALL)
async def test_cost_consumes_several_units(make_backend, algorithm):
    backend = make_backend(algorithm)
    result = await backend.hit("batch", ["5/s"], cost=3)
    assert result.allowed
    assert result.binding.remaining == 2
    assert not (await backend.hit("batch", ["5/s"], cost=3)).allowed
    assert (await backend.hit("batch", ["5/s"], cost=2)).allowed
    with pytest.raises(ValueError, match="can never fit"):
        await backend.hit("batch", ["5/s"], cost=6)


# --- algorithm-specific behaviour -------------------------------------------


async def test_fixed_window_allows_double_burst_at_boundary(make_backend, clock):
    backend = make_backend(Algorithm.FIXED_WINDOW)
    clock.advance(0.9)
    assert all([(await backend.hit("k", ["5/s"])).allowed for _ in range(5)])
    clock.advance(0.1)  # new window
    assert all([(await backend.hit("k", ["5/s"])).allowed for _ in range(5)])


async def test_sliding_log_is_exact_across_boundary(make_backend, clock):
    backend = make_backend(Algorithm.SLIDING_LOG)
    clock.advance(0.9)
    for _ in range(5):
        await backend.hit("k", ["5/s"])
    clock.advance(0.1)
    denied = await backend.hit("k", ["5/s"])
    assert not denied.allowed
    assert denied.retry_after == pytest.approx(0.9)


async def test_sliding_window_weights_previous_window(make_backend, clock):
    backend = make_backend(Algorithm.SLIDING_WINDOW)
    for _ in range(10):
        await backend.hit("k", ["10/s"])
    clock.advance(1.5)  # halfway into the next window: estimate = 10 * 0.5
    first = await backend.hit("k", ["10/s"])
    assert first.allowed
    assert first.binding.remaining == 4
    for _ in range(4):
        assert (await backend.hit("k", ["10/s"])).allowed
    denied = await backend.hit("k", ["10/s"])
    assert not denied.allowed
    assert denied.retry_after == pytest.approx(0.1)


async def test_token_bucket_refills_continuously(make_backend, clock):
    backend = make_backend(Algorithm.TOKEN_BUCKET)
    for _ in range(10):
        assert (await backend.hit("k", ["10/s"])).allowed
    denied = await backend.hit("k", ["10/s"])
    assert denied.retry_after == pytest.approx(0.1)
    clock.advance(0.25)  # 2.5 tokens back
    assert (await backend.hit("k", ["10/s"])).allowed
    assert (await backend.hit("k", ["10/s"])).allowed
    assert not (await backend.hit("k", ["10/s"])).allowed


async def test_gcra_spaces_requests_after_burst(make_backend, clock):
    backend = make_backend(Algorithm.GCRA)
    for _ in range(10):
        assert (await backend.hit("k", ["10/s"])).allowed
    denied = await backend.hit("k", ["10/s"])
    assert denied.retry_after == pytest.approx(0.1)
    clock.advance(0.1)
    assert (await backend.hit("k", ["10/s"])).allowed
    assert not (await backend.hit("k", ["10/s"])).allowed
