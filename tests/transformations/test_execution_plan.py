import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[2]))
from etl.transformation.model import Model, build_execution_plan


# --- minimal stubs (no real data needed) ---


class _A(Model):
    def __init__(self):
        super().__init__("a", "test")

    def _build(self) -> pl.DataFrame:
        return pl.DataFrame()


class _B(Model):
    def __init__(self):
        super().__init__("b", "test")

    def _build(self) -> pl.DataFrame:
        return pl.DataFrame()


class _C(Model):
    def __init__(self):
        super().__init__("c", "test")

    def _build(self) -> pl.DataFrame:
        return pl.DataFrame()


class _D(Model):
    def __init__(self):
        super().__init__("d", "test")

    def _build(self) -> pl.DataFrame:
        return pl.DataFrame()


# --- helpers ---


def _order_valid(ordered: list[Model]) -> bool:
    """Every model's dependencies (that appear in the list) must precede it."""
    present = {type(m) for m in ordered}
    seen: set[type] = set()
    for model in ordered:
        for dep_cls in model.dependencies:
            if dep_cls in present and dep_cls not in seen:
                return False
        seen.add(type(model))
    return True


# --- tests ---


def test_linear_chain():
    # _A depends on _B depends on _C  →  expected order: _C, _B, _A
    a, b, c = _A(), _B(), _C()
    a.configure_dependencies([_B])
    b.configure_dependencies([_C])
    result = build_execution_plan([a, b, c])
    assert _order_valid(result)


def test_diamond():
    # _D is the common base; _B and _C both depend on _D; _A depends on _B and _C
    a, b, c, d = _A(), _B(), _C(), _D()
    a.configure_dependencies([_B, _C])
    b.configure_dependencies([_D])
    c.configure_dependencies([_D])
    result = build_execution_plan([a, b, c, d])
    assert _order_valid(result)


def test_no_dependencies_returns_all_instances():
    a, b, c = _A(), _B(), _C()
    result = build_execution_plan([a, b, c])
    assert set(result) == {a, b, c}


def test_two_node_cycle_raises():
    a, b = _A(), _B()
    a.configure_dependencies([_B])
    b.configure_dependencies([_A])
    with pytest.raises(RuntimeError, match="Circular dependency"):
        build_execution_plan([a, b])


def test_three_node_cycle_raises():
    a, b, c = _A(), _B(), _C()
    a.configure_dependencies([_B])
    b.configure_dependencies([_C])
    c.configure_dependencies([_A])
    with pytest.raises(RuntimeError, match="Circular dependency"):
        build_execution_plan([a, b, c])
