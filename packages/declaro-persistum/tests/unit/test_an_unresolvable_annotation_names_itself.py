"""A model with an unresolvable annotation blamed a different field.

MEASURED 2026-08-19. A model whose annotation names a type that does not
exist:

    @table("widgets")
    class Widget(BaseModel):
        id: int = field(primary_key=True)
        thing: NeverDefined = None

    LoaderError: cannot tell what column type 'int' means.

`int` is not the problem. `NeverDefined` is, and the message names neither it
nor the field it is on.

WHY IT BLAMES `int`. `get_type_hints(model_cls)` raises NameError on an
unresolvable forward reference, and the loader catches it and falls back to
`model_cls.__annotations__`, which under `from __future__ import annotations`
holds every annotation as a STRING. So `id: int` becomes the string `'int'`,
which is not the type `int`, and the type mapper refuses the first field it
meets. The field that broke resolution is never reached.

THE FALLBACK'S OWN JUSTIFICATION NO LONGER HOLDS. Its comment reads: "Fall
back if a forward reference is unresolvable. Caller will see the same
wrong-but-consistent behavior as pre-0.1.3 -- better than crashing during
schema load." Two things changed under it. `declared_type` was taught on
2026-08-17 to raise on an annotation it does not recognise, rather than
quietly emitting `text`, so the load crashes regardless; and what it crashes
with is a message about a field that is fine.

Better than crashing was the trade. It is not on offer, so what is left is
crashing with the right name.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from declaro_persistum.exceptions import LoaderError
from declaro_persistum.pydantic_loader import load_schema_from_models

pytestmark = [pytest.mark.precommit]

UNRESOLVABLE = '''
from __future__ import annotations
from pydantic import BaseModel
from declaro_persistum import table, field


@table("widgets")
class Widget(BaseModel):
    id: int = field(primary_key=True)
    thing: NeverDefined = None
'''

RESOLVABLE = '''
from __future__ import annotations
from pydantic import BaseModel
from declaro_persistum import table, field


@table("widgets")
class Widget(BaseModel):
    id: int = field(primary_key=True)
    name: str = "unnamed"
'''


def _models(body: str) -> str:
    directory = Path(tempfile.mkdtemp())
    (directory / "models.py").write_text(body)
    return str(directory)


def test_it_is_refused() -> None:
    with pytest.raises(LoaderError):
        load_schema_from_models(_models(UNRESOLVABLE))


def test_the_refusal_names_the_annotation_that_cannot_be_resolved() -> None:
    """Not `int`, which is the first field and is fine."""
    with pytest.raises(LoaderError) as caught:
        load_schema_from_models(_models(UNRESOLVABLE))

    assert "NeverDefined" in str(caught.value), caught.value


def test_the_refusal_does_not_blame_a_field_that_is_fine() -> None:
    """Stated separately because it is what the author acts on.

    Told their `int` column is unrecognisable, an author changes a line that
    was never wrong.
    """
    with pytest.raises(LoaderError) as caught:
        load_schema_from_models(_models(UNRESOLVABLE))

    assert "'int'" not in str(caught.value), caught.value


def test_the_refusal_names_the_model() -> None:
    with pytest.raises(LoaderError) as caught:
        load_schema_from_models(_models(UNRESOLVABLE))

    assert "Widget" in str(caught.value), caught.value


def test_a_resolvable_deferred_annotation_still_loads() -> None:
    """The control, and the reason `get_type_hints` is called at all.

    `from __future__ import annotations` makes every annotation a string, so
    a loader reading `__annotations__` directly would see `'int'` and `'str'`
    for a model that is perfectly well formed. This is that model.
    """
    schema = load_schema_from_models(_models(RESOLVABLE))

    assert schema["widgets"]["columns"]["id"]["type"] == "integer"
    assert schema["widgets"]["columns"]["name"]["type"] == "text"
