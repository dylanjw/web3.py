from hypothesis import (
    given,
    strategies as st,
)
import pytest

from web3.utils.events import (
    EventFilterBuilder,
    EventArgument,
    normalize_topic_list,
)


@pytest.mark.parametrize(
    "topic_list,expected",
    (
        (
            ("0x1", "0x2", ["0x3"], None, "0x4", None, None, None),
            ("0x1", "0x2", "0x3", None, "0x4")
        ),
        (
            (None, ["0x2", "0x2a"], "0x3", None, "0x4", None, [None], None),
            (None, ["0x2", "0x2a"], "0x3", None, "0x4")
        ),
        (
            (None, None, [None]),
            tuple()
        )
    )
)
def test_normalize_topic_list(topic_list, expected):
    assert normalize_topic_list(topic_list) == expected
def test_indexed_static_argument():
    arg = EventArgument("uint256", "myArg", True)
    arg.match_single(12345)
    assert arg.match_values == (12345,)
    arg.match_any(123456, 7890)
    assert arg.match_values == (123456, 7890)


def test_indexed_dynamic_argument():
    arg = EventArgument("string", "myArg", True)
    arg.match_single("pizza")
    assert arg.match_values == ("pizza",)
    arg.match_any("pizza", "pancake")
    assert arg.match_values == ("pizza", "pancake")


def test_indexed_array_argument():
    arg = EventArgument("string[]", "myArg", True)
    arg.match_single(["pizza"])
    assert arg.match_values == (["pizza"],)
    arg.match_any(["pizza"], ["pancake"])
    assert arg.match_values == (["pizza"], ["pancake"])
