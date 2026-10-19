from __future__ import annotations

import pytest

from throttlekit import Decision, Rate, Result


@pytest.mark.parametrize(
    ("text", "limit", "period"),
    [
        ("10/s", 10, 1),
        ("1000/h", 1000, 3600),
        ("100/5m", 100, 300),
        ("5 per minute", 5, 60),
        ("3/2 hours", 3, 7200),
        ("50/500ms", 50, 0.5),
        ("1/day", 1, 86400),
    ],
)
def test_parse(text, limit, period):
    rate = Rate.parse(text)
    assert rate.limit == limit
    assert rate.period == pytest.approx(period)


@pytest.mark.parametrize("text", ["", "ten/s", "10/fortnight", "0/s", "10"])
def test_parse_rejects_garbage(text):
    with pytest.raises(ValueError, match=r"rate|limit"):
        Rate.parse(text)


def test_headers_report_the_binding_rate():
    second, hour = Rate.parse("10/s"), Rate.parse("1000/h")
    result = Result(
        allowed=False,
        decisions=(
            Decision(second, allowed=True, remaining=9, reset_after=0.1, retry_after=0),
            Decision(hour, allowed=False, remaining=0, reset_after=1800.2, retry_after=3.6),
        ),
    )
    assert result.headers() == {
        "RateLimit-Limit": "1000",
        "RateLimit-Remaining": "0",
        "RateLimit-Reset": "1801",
        "RateLimit-Policy": "10;w=1, 1000;w=3600",
        "Retry-After": "4",
    }
