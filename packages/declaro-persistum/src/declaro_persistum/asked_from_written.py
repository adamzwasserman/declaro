"""Read the question a caller typed and lift the parts out of it.

ONE READER, FOUR SPELLINGS. It takes the text and the method names that spelling
uses, and hands back the parts under this library's own words. What it hands back
is parts and not a declaration: `fn declaration_from_asked` turns parts into a
declaration, and that is the one place a part a declaration needs and nobody
wrote is told no. This card reads, and nothing else.

WRITTEN 2026-11-16, REPLACING FOUR CARDS AND THE LOOKUP THAT FANNED OUT TO THEM.
`declaration_from_pseudo_sql` and its three siblings each took a dict somebody
else had already taken apart, and the taking apart happened in a hand-written
table inside each step file, so the features wrote the real expression and the
library never saw it. The four differed by their method names alone.

PYTHON'S OWN PARSER READS THE TEXT, which decides most of what follows. `ast` is
in the standard library, it touches nothing, and it is the same parser that will
read the caller's file, so a question this accepts is a question that compiles
and one it refuses would not have run either. Writing a parser here would mean a
second opinion about Python's syntax, and two opinions disagree.

A SYNTAX ERROR CARRIES ITS OWN POSITION, which is what lets the refusal say where
the writing stopped making sense. A caller handed "that is not a question" has to
find the mistake by eye.

THE CHAIN IS WALKED BY RECURSION AND NOT BY A LOOP WITH A WAY OUT. Python nests a
chain from the outside in: `Person.objects.filter(...).values(...)` is a call
whose function is an attribute of a call, so the last method written is the first
one reached. Each link reads its own part and asks what is inside it for the
rest, which needs no branch and no accumulator the reader has to carry.

THE ORDER THE CALLER WROTE IN DOES NOT REACH THE ANSWER. The parts land in one
map, so the same question written `.filter(...).values(...)` in one spelling and
`.select(...).where(...)` in another gives one set of parts. Two scenarios turn
on that.

THE MODEL IS THE NAME AT THE ROOT, AND IT IS THE CALLER'S OWN WORD. A caller
writing django types `Person`, the class; one writing this library's own spelling
types `people`, the table. Both are kept as written, because only the caller knows
which name they have. `orchestrator fn query_answer` puts the question under the
table's name later, where the model is in hand to ask.

A MARKING ATTRIBUTE IS STEPPED OVER, NOT READ AS A PART. `.objects.` and
`.query.` are how `fn grammar_of` told the spelling apart, and by the time this
runs the spelling is settled, so they carry nothing.

A METHOD NO TABLE NAMES IS REFUSED AND THE REFUSAL NAMES IT. Dropping it would
hand back the parts this did recognise, and the caller would get rows for a
question missing a clause they wrote, with nothing to say so.
"""

from __future__ import annotations

import ast
from collections.abc import Callable
from typing import Any, cast, get_args

from declaro_persistum.types import Asked, Fault, MethodNames, Reading, Writing

# WHAT MARKS A SPELLING RATHER THAN NAMING A PART. `fn grammar_of` has already
# read these and the spelling is settled, so they are stepped over here.
THE_MARKING_ATTRIBUTES = frozenset({"objects", "query"})

# PRISMA'S ONE CALL, which carries every part as a keyword rather than as a chain.
# Its keywords are already this library's words, which is why prisma renames
# nothing in `fn methods_of`.
THE_ONE_CALL_CARRYING_NAMED_PARTS = "find_many"

# EVERY METHOD THIS LIBRARY ANSWERS TO. A spelling renames some of these and
# reaches the rest by this library's own word, which is why a reader handed an
# empty table still reads every part. The words come from
# `HOW-IT-SHOULD-WORK.md`'s vocabulary, which sits above the declaration.
THE_PARTS_A_QUESTION_MAY_CARRY = frozenset(
    {
        "where",
        "select",
        "ordered_by",
        "grouped_by",
        "having",
        "page",
        "limit",
        "offset",
        "distinct",
        "across",
        "add",
        "change",
        "change_all",
        "remove",
        "remove_all",
        "replace",
        "as_objects",
        "as_dictionaries",
        "all",
        "one",
        "first",
        "exists",
        "count",
        "count_distinct",
        "sum",
        "average",
        "largest",
        "smallest",
    }
)


def _refused(went_wrong: str, do_instead: str) -> Fault:
    return {
        "kind": "refused",
        "what_went_wrong": went_wrong,
        "what_to_do_instead": do_instead,
    }


def _is_fault(piece: object) -> bool:
    return isinstance(piece, dict) and set(piece) == {
        "kind",
        "what_went_wrong",
        "what_to_do_instead",
    }


# --- reading the text --------------------------------------------------------


def _the_tree(written: str) -> Any:
    """The expression the caller wrote, or the refusal its syntax earns."""
    try:
        return ast.parse(written, mode="eval").body
    except SyntaxError as broke:
        return _refused(
            f"This question stopped making sense at column {broke.offset} of "
            f"{written!r}: {broke.msg}.",
            "Write a question Python can read: a model, then the methods you "
            "want called on it, with their brackets closed.",
        )


# --- reading one argument ----------------------------------------------------


def _a_literal(node: ast.expr) -> Any:
    """A value the caller wrote, or a refusal naming what cannot be read."""
    try:
        return ast.literal_eval(node)
    except (ValueError, SyntaxError, TypeError):
        return _refused(
            f"A part of this question is not a value this reader can take: {ast.unparse(node)!r}.",
            "Write the part as a literal: a string, a number, a list or a map. "
            "A question is data, so it names nothing and calls nothing.",
        )


def _the_first_refusal_or(read: list[Any], build: Callable[[], Any]) -> Any:
    """A refusal anywhere among the pieces is the answer; otherwise build one."""
    refused = [piece for piece in read if _is_fault(piece)]
    return _WHETHER_ANYTHING_WAS_REFUSED[refused != []](refused, build)


_WHETHER_ANYTHING_WAS_REFUSED: dict[bool, Callable[[list[Any], Callable[[], Any]], Any]] = {
    True: lambda refused, _build: refused[0],
    False: lambda _refused, build: build(),
}


# --- what one call carries ---------------------------------------------------


def _the_keywords_as_a_map(call: ast.Call) -> Any:
    """Keyword arguments as a map, which is a narrowing written the short way.

    `filter(name="bob")` and `where({"name": "bob"})` are two ways of writing one
    narrowing, so both arrive as the same map.
    """
    read = {word.arg: _a_literal(word.value) for word in call.keywords if word.arg}
    return _the_first_refusal_or(list(read.values()), lambda: read)


def _the_one_argument(call: ast.Call) -> Any:
    return _a_literal(call.args[0])


def _every_argument_as_a_list(call: ast.Call) -> Any:
    read = [_a_literal(node) for node in call.args]
    return _the_first_refusal_or(read, lambda: read)


# HOW MANY ARGUMENTS A CALL CARRIES DECIDES HOW IT IS READ. None is the asking
# word standing alone, as in `all`; one is that value; several are a list.
_BY_HOW_MANY_ARGUMENTS: dict[int, Callable[[ast.Call], Any]] = {
    0: lambda _call: True,
    1: _the_one_argument,
    2: _every_argument_as_a_list,
}


def _what_the_arguments_carry(call: ast.Call) -> Any:
    return _BY_HOW_MANY_ARGUMENTS[min(len(call.args), 2)](call)


_WHETHER_IT_CARRIES_KEYWORDS: dict[bool, Callable[[ast.Call], Any]] = {
    True: _the_keywords_as_a_map,
    False: _what_the_arguments_carry,
}


def _what_the_call_carries(call: ast.Call) -> Any:
    """What one call hands over: its keywords where it has them, else its arguments."""
    return _WHETHER_IT_CARRIES_KEYWORDS[call.keywords != []](call)


# --- which word this library uses for a method -------------------------------


def _this_librarys_word_for(method: str, methods: MethodNames) -> str:
    """This library's own word for a method the spelling renamed, or the method.

    `fn methods_of` maps this library's word to the spelling's, so reading it
    back the other way is what turns `filter` into `where`.
    """
    theirs_to_ours = {theirs: ours for ours, theirs in methods.items()}
    return _WHETHER_THIS_SPELLING_RENAMED_IT[method in theirs_to_ours](method, theirs_to_ours)


_WHETHER_THIS_SPELLING_RENAMED_IT: dict[bool, Callable[[str, dict[str, str]], str]] = {
    True: lambda method, theirs_to_ours: theirs_to_ours[method],
    False: lambda method, _theirs_to_ours: method,
}


# --- walking the chain -------------------------------------------------------


def _from_a_name(node: Any, _methods: MethodNames, _written: str) -> Any:
    """The root of the chain, which is the model under the caller's own word."""
    return {"model": cast("ast.Name", node).id}


def _from_a_marking_attribute(node: Any, methods: MethodNames, written: str) -> Any:
    """`.objects` and `.query` carry nothing, so this reads what is inside them."""
    attribute = cast("ast.Attribute", node)
    return _WHETHER_THE_ATTRIBUTE_MARKS_A_SPELLING[attribute.attr in THE_MARKING_ATTRIBUTES](
        attribute, methods, written
    )


def _an_attribute_naming_no_part(
    attribute: ast.Attribute, _methods: MethodNames, written: str
) -> Fault:
    """An attribute that is neither a marking one nor a call names nothing."""
    return _refused(
        f"This question reads {attribute.attr!r} as a part, and a part is "
        f"something you call: {written!r}.",
        f"Call it, as in .{attribute.attr}(...), or take it out.",
    )


_WHETHER_THE_ATTRIBUTE_MARKS_A_SPELLING: dict[
    bool, Callable[[ast.Attribute, MethodNames, str], Any]
] = {
    True: lambda attribute, methods, written: _walked(attribute.value, methods, written),
    False: _an_attribute_naming_no_part,
}


def _the_parts_a_one_call_carries(
    call: ast.Call, inside: dict[str, Any], _ours: str, _written: str
) -> Any:
    """Prisma's one call: every keyword in it is a part, under its own name."""
    read = {word.arg: _a_literal(word.value) for word in call.keywords if word.arg}
    return _the_first_refusal_or(list(read.values()), lambda: {**inside, **read})


def _the_part_this_method_names(
    call: ast.Call, inside: dict[str, Any], ours: str, written: str
) -> Any:
    """One link of a chain: the method names the part and the call carries it."""
    return _WHETHER_THIS_LIBRARY_HAS_THE_METHOD[ours in THE_PARTS_A_QUESTION_MAY_CARRY](
        call, inside, ours, written
    )


def _a_part_under_its_own_name(
    call: ast.Call, inside: dict[str, Any], ours: str, _written: str
) -> Any:
    carried = _what_the_call_carries(call)
    return _the_first_refusal_or([carried], lambda: {**inside, ours: carried})


def _a_method_no_table_names(
    _call: ast.Call, _inside: dict[str, Any], ours: str, written: str
) -> Fault:
    """Refused, and the refusal names the method, because dropping it would hand
    back the parts this did recognise and answer a question nobody asked."""
    return _refused(
        f"This question calls {ours!r}, and no spelling this package reads has a "
        f"method by that name: {written!r}.",
        f"Call one of {', '.join(sorted(THE_PARTS_A_QUESTION_MAY_CARRY))}, or the "
        f"name your own spelling gives it.",
    )


_WHETHER_THIS_LIBRARY_HAS_THE_METHOD: dict[
    bool, Callable[[ast.Call, dict[str, Any], str, str], Any]
] = {
    True: _a_part_under_its_own_name,
    False: _a_method_no_table_names,
}

_HOW_THIS_CALL_GIVES_ITS_PARTS: dict[bool, Callable[[ast.Call, dict[str, Any], str, str], Any]] = {
    True: _the_parts_a_one_call_carries,
    False: _the_part_this_method_names,
}


def _from_a_call(node: Any, methods: MethodNames, written: str) -> Any:
    """One call in the chain, with what is inside it read first."""
    call = cast("ast.Call", node)
    return _WHETHER_THE_CALL_NAMES_A_METHOD[type(call.func) is ast.Attribute](
        call, methods, written
    )


def _a_call_naming_a_method(call: ast.Call, methods: MethodNames, written: str) -> Any:
    named = cast("ast.Attribute", call.func)
    ours = _this_librarys_word_for(named.attr, methods)
    inside = _walked(named.value, methods, written)
    return _the_first_refusal_or(
        [inside],
        lambda: _HOW_THIS_CALL_GIVES_ITS_PARTS[named.attr == THE_ONE_CALL_CARRYING_NAMED_PARTS](
            call, cast("dict[str, Any]", inside), ours, written
        ),
    )


def _a_call_naming_nothing(call: ast.Call, _methods: MethodNames, written: str) -> Fault:
    """A call that is not a method on something, as in a bare `filter(...)`."""
    return _refused(
        f"This question calls something that is not a method on a model: "
        f"{ast.unparse(call)!r} in {written!r}.",
        "Write the model first, then the methods called on it, as in "
        'people.select("id").where({"name": "bob"}).',
    )


_WHETHER_THE_CALL_NAMES_A_METHOD: dict[bool, Callable[[ast.Call, MethodNames, str], Any]] = {
    True: _a_call_naming_a_method,
    False: _a_call_naming_nothing,
}


def _not_a_question(_node: Any, _methods: MethodNames, written: str) -> Fault:
    """Writing that is neither a call, a marking attribute nor a model name."""
    return _refused(
        f"This is not a question: {written!r}. A question is a model with methods "
        f"called on it, and this calls nothing.",
        "Write the model, then the methods you want called on it, as in "
        'people.select("id").where({"name": "bob"}).',
    )


# WHICH LINK OF THE CHAIN THIS IS. Three shapes carry a question and everything
# else is not one, which is why the last row exists rather than a default.
_WHAT_EACH_SHAPE_IS: dict[type, str] = {
    ast.Call: "a call",
    ast.Attribute: "an attribute",
    ast.Name: "a name",
}

_READ_EACH_LINK: dict[str, Callable[[Any, MethodNames, str], Any]] = {
    "a call": _from_a_call,
    "an attribute": _from_a_marking_attribute,
    "a name": _from_a_name,
    "nothing this reads": _not_a_question,
}


def _which_link(node: Any) -> str:
    return _WHETHER_THE_SHAPE_IS_ONE_THIS_READS[type(node) in _WHAT_EACH_SHAPE_IS](node)


_WHETHER_THE_SHAPE_IS_ONE_THIS_READS: dict[bool, Callable[[Any], str]] = {
    True: lambda node: _WHAT_EACH_SHAPE_IS[type(node)],
    False: lambda _node: "nothing this reads",
}


def _walked(node: Any, methods: MethodNames, written: str) -> Any:
    """Every part this link and everything inside it carries."""
    return _READ_EACH_LINK[_which_link(node)](node, methods, written)


# --- the asking word, which silence also answers -----------------------------

# EVERY ASKING WORD, from the vocabularies rather than a list here.
EVERY_ASKING_WORD: frozenset[str] = frozenset((*get_args(Reading), *get_args(Writing)))

# WHAT A QUESTION WITH NO ASKING WORD ASKS FOR. All four spellings mean the same
# thing by silence: a Django queryset you have only narrowed is every matching
# row, Prisma's `find_many` says so in its name, and this library's own
# `people.select(...).where(...)` is read in SQL's word order, where no row
# limit means every row. So silence is a reading and not a guess, and it is the
# same reading in all four.
WHAT_SILENCE_ASKS_FOR = "all"


def _the_word_among(parts: dict[str, Any]) -> str:
    """The asking word the caller called, or what their silence asks for."""
    called = [part for part in parts if part in EVERY_ASKING_WORD]
    return _WHETHER_AN_ASKING_WORD_WAS_CALLED[called != []](called)


_WHETHER_AN_ASKING_WORD_WAS_CALLED: dict[bool, Callable[[list[str]], str]] = {
    True: lambda called: called[0],
    False: lambda _called: WHAT_SILENCE_ASKS_FOR,
}


def _with_the_asking_word(parts: dict[str, Any]) -> Asked:
    """The parts with the asking word under `wanted`, where a declaration reads it.

    A caller writes the word as a method, `.all` or `.one`, so it arrives as a
    part named for itself. A declaration has one field for it, so it is moved
    there and the part it arrived as is dropped: carrying both would be the same
    fact in two places.
    """
    word = _the_word_among(parts)
    return cast(
        "Asked",
        {
            **{part: said for part, said in parts.items() if part not in EVERY_ASKING_WORD},
            "wanted": word,
        },
    )


# --- the door ----------------------------------------------------------------


def _read_the_whole_question(tree: Any, methods: MethodNames, written: str) -> Asked | Fault:
    """A question is a call at the top, so a bare word is refused here.

    `people` on its own parses as a name, and a name is the model when something
    calls a method on it. Standing alone it is a word, not a question.
    """
    return cast(
        "Asked | Fault",
        _WHETHER_ANYTHING_IS_CALLED[type(tree) is ast.Call](tree, methods, written),
    )


_WHETHER_ANYTHING_IS_CALLED: dict[bool, Callable[[Any, MethodNames, str], Any]] = {
    True: _walked,
    False: lambda tree, methods, written: _not_a_question(tree, methods, written),
}


def asked_from_written(written: str, methods: MethodNames) -> Asked | Fault:
    """The parts this question carries, under this library's own words.

    Reads nothing but the text and the table it was handed, so the same question
    always gives the same parts.
    """
    tree = _the_tree(written)
    read = _the_first_refusal_or([tree], lambda: _read_the_whole_question(tree, methods, written))
    return cast(
        "Asked | Fault",
        _the_first_refusal_or([read], lambda: _with_the_asking_word(cast("dict[str, Any]", read))),
    )
