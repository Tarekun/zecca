import ast
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parents[2]))
import etl.ingestion.french_library  # noqa: F401  (registers Source subclasses)
import etl.ingestion.sec  # noqa: F401
import etl.ingestion.yfinance  # noqa: F401
import etl.transformation.gold  # noqa: F401  (registers Model subclasses)
import etl.transformation.silver  # noqa: F401
from etl.ingestion.source import Source
from etl.transformation.model import Model

REPO = Path(__file__).parents[2]
# the pipeline entrypoint builds the configured models on the default root on purpose
EXEMPT = {REPO / "etl" / "transformation" / "builder.py"}


def _subclasses(cls: type) -> set[type]:
    found = set()
    for sub in cls.__subclasses__():
        found |= {sub} | _subclasses(sub)
    return found


def test_models_and_sources_are_always_given_the_dataplatform_root():
    """A model/source created inside another one without forwarding
    `dataplatform_root` silently reads ./dataplatform instead of the caller's
    platform, so reading any model from a different root (or cwd) breaks."""
    names = {c.__name__ for c in _subclasses(Model) | _subclasses(Source)}
    offenders = []
    for path in sorted((REPO / "etl").rglob("*.py")):
        if path in EXEMPT:
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id in names
                and not any(k.arg in ("dataplatform_root", None) for k in node.keywords)
            ):
                offenders.append(
                    f"{path.relative_to(REPO)}:{node.lineno} {node.func.id}()"
                )
    assert not offenders, "Missing dataplatform_root:\n" + "\n".join(offenders)
