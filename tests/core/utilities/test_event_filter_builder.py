from hypothesis import (
    given,
    strategies as st,
)
import pytest

from web3.utils.events import (
    ArgumentFilter,
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


@given(st.text(), st.booleans())
def test_match_single_string_type_properties(value, is_indexed):
    ea = ArgumentFilter(arg_type="string", name="arg", indexed=is_indexed)
    ea.match_single(value)


@given(st.lists(elements=st.text(), max_size=10, min_size=0), st.booleans())
def test_match_any_string_type_properties(values, is_indexed):
    ea = ArgumentFilter(arg_type="string", name="arg", indexed=is_indexed)
    ea.match_any(*values)
    assert len(ea.raw_match_values) == len(values)
    assert len(ea.encoded_match_values) == len(values)
