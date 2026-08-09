"""Prototypes for two L1.18b false-positive filters, with their counter-cases.

This is a SPEC, not a drop-in. The analyzer's classifier is tree-sitter; this
is stdlib ``ast`` so the conditions can be stated exactly and each one can be
shown failing on a real case. Port the conditions, not the code.

The axis under both rules is DECISION SPACE, not data cardinality. An
attribute that carries unbounded DATA but takes a bounded number of BRANCHES
is finitely testable. The filters below identify two shapes where that holds.

  Rule A — memoization cache. Presence-gated and result-invariant.
  Rule B — drives-a-decision requires an actual test expression.

The three rules (with the earlier receiver-aware write-once filter) are
NON-OVERLAPPING, and each is load-bearing on its own case:

  _rows      write-once      cleared by write-once only
  _tokens    memoization     cleared by Rule A only  (Rule B keeps it: a
                             membership test IS a real test expression)
  _queryset  carried value   cleared by Rule B only  (Rule A keeps it: it is
                             not a cache)

So Rule B alone does not subsume Rule A, and neither subsumes write-once.

Run: ``uv run python deploy/state_bounds_filters.py``
"""

import ast
import sys

# ----------------------------------------------------------------------
# Rule B: an attribute drives a decision only inside a real test expression
# ----------------------------------------------------------------------

# The node fields that are genuinely branch conditions. Everything else --
# a plain read, a method call, a subscript, an argument -- is data flow.
_TEST_FIELDS: dict[type, tuple[str, ...]] = {
    ast.If: ("test",),
    ast.While: ("test",),
    ast.IfExp: ("test",),
    ast.Assert: ("test",),
    ast.Match: ("subject",),
}


def _decision_expressions(tree: ast.AST) -> list[ast.AST]:
    """Every expression that a branch actually tests."""
    found: list[ast.AST] = []
    for node in ast.walk(tree):
        for field in _TEST_FIELDS.get(type(node), ()):
            found.append(getattr(node, field))
        # Comprehension guards are branch conditions too.
        if isinstance(node, ast.comprehension):
            found.extend(node.ifs)
    return found


def _mentions_attribute(expr: ast.AST, attr: str) -> bool:
    """True if ``self.<attr>`` appears anywhere inside this expression.

    Nested occurrences count: ``if self._x.enabled:`` and ``if f(self._x):``
    both genuinely branch on the attribute. Only whole-expression matching
    would miss them, so walk.
    """
    return any(
        isinstance(node, ast.Attribute)
        and node.attr == attr
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
        for node in ast.walk(expr)
    )


def drives_a_decision(tree: ast.AST, attr: str) -> bool:
    """Rule B. Reads and calls are not decisions; only test expressions are."""
    return any(_mentions_attribute(e, attr) for e in _decision_expressions(tree))


# ----------------------------------------------------------------------
# Rule A: presence-gated, result-invariant memoization cache
# ----------------------------------------------------------------------


def _self_attr_targets(node: ast.AST, attr: str) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == attr
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    )


def _is_membership_test_on(expr: ast.AST, attr: str) -> bool:
    """``k in self._d`` / ``k not in self._d`` -- and nothing else about _d."""
    if not isinstance(expr, ast.Compare) or len(expr.ops) != 1:
        return False
    if not isinstance(expr.ops[0], (ast.In, ast.NotIn)):
        return False
    right = expr.comparators[0]
    # The container must be the attribute itself, and the probed key must not
    # itself mention the attribute (``self._d[a] in self._d`` is not a
    # presence test, it is a value inspection).
    return _self_attr_targets(right, attr) and not _mentions_attribute(expr.left, attr)


def _value_appears_in_a_condition(tree: ast.AST, attr: str) -> bool:
    """Condition 2. Does any *stored value* reach a branch condition?

    A read of ``self._d[k]`` inside a test expression means the cached value
    decides something. A bare ``k in self._d`` does not.
    """
    for expr in _decision_expressions(tree):
        if _is_membership_test_on(expr, attr):
            continue
        # A BoolOp of pure membership tests is still presence-only.
        if isinstance(expr, ast.BoolOp) and all(
            _is_membership_test_on(v, attr) or not _mentions_attribute(v, attr)
            for v in expr.values
        ):
            continue
        if _mentions_attribute(expr, attr):
            return True
    return False


# Mutations that keep a cache a cache. Anything else (``+=``, ``.update`` from
# another cache, ``.setdefault`` used for its return, sorting in place) means
# the structure is doing more than remembering.
_CACHE_METHODS = {"pop", "clear", "get", "values", "keys", "items", "__contains__"}


def _writes_are_plain_stores(tree: ast.AST, attr: str) -> bool:
    """Condition 3. Only ``d[k] = v``, ``del d[k]``, ``.pop``, ``.clear``."""
    for node in ast.walk(tree):
        # Augmented assignment through the attribute is accumulation, not a store.
        if isinstance(node, ast.AugAssign):
            if _mentions_attribute(node.target, attr):
                return False
        if isinstance(node, ast.Assign):
            for target in node.targets:
                # Rebinding the whole dict to something other than a literal
                # empty dict is not cache maintenance.
                if _self_attr_targets(target, attr) and not (
                    isinstance(node.value, ast.Dict) and not node.value.keys
                ):
                    return False
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if _self_attr_targets(node.func.value, attr):
                if node.func.attr not in _CACHE_METHODS:
                    return False
    return True


def _has_presence_gate(tree: ast.AST, attr: str) -> bool:
    """Condition 1+4. At least one membership test gates a store of the key."""
    return any(
        _is_membership_test_on(e, attr) for e in _decision_expressions(tree)
    ) and any(
        isinstance(n, ast.Assign)
        and any(
            isinstance(t, ast.Subscript) and _self_attr_targets(t.value, attr)
            for t in n.targets
        )
        for n in ast.walk(tree)
    )


def is_memoization_cache(tree: ast.AST, attr: str) -> bool:
    """Rule A, conditions 1-4. Condition 5 is checked separately below."""
    return (
        _has_presence_gate(tree, attr)
        and not _value_appears_in_a_condition(tree, attr)
        and _writes_are_plain_stores(tree, attr)
    )


def is_result_invariant(tree: ast.AST, attr: str) -> bool:
    """Condition 5 (audit's addition). Same answer whether cached or not.

    Structural proxy: on the miss branch, the value that gets stored is also
    what gets returned, and the hit branch returns a read of the same key. A
    ``seen``-set that returns True on miss and False on hit fails here -- the
    presence of the key IS the answer, so it is a decision, not an
    optimisation.
    """
    for node in ast.walk(tree):
        if not isinstance(node, ast.If):
            continue
        if not _is_membership_test_on(node.test, attr):
            continue
        returns = [
            n
            for n in ast.walk(node)
            if isinstance(n, ast.Return) and n.value is not None
        ]
        if not returns:
            continue
        # Every return in a memoizing accessor reads the cache (or returns
        # the freshly-stored local). A literal return means the presence of
        # the key decided the answer.
        if any(isinstance(r.value, ast.Constant) for r in returns):
            return False
    return True


def clears_as_cache(source: str, attr: str) -> bool:
    tree = ast.parse(source)
    return is_memoization_cache(tree, attr) and is_result_invariant(tree, attr)


# ----------------------------------------------------------------------
# Vectors. CLEAR = false positive, must be suppressed. KEEP = genuine.
# ----------------------------------------------------------------------

TOKENS_REAL = '''
class TursoCloudManager:
    def __init__(self):
        self._tokens = {}
    def get_token(self, db_name):
        if db_name not in self._tokens:
            self._tokens[db_name] = self._mint(db_name)
        return self._tokens[db_name]
    def forget(self, db_name):
        self._tokens.pop(db_name, None)
    def close(self):
        self._tokens.clear()
'''

FIRST_FAILURE_REAL = '''
class WriteQueue:
    def __init__(self):
        self._first_failure_time = {}
    def _check(self, key, now):
        if key not in self._first_failure_time:
            self._first_failure_time[key] = now
        elif now - self._first_failure_time[key] >= 3600:
            log_critical(key)
'''

SEEN_SET = '''
class Dedup:
    def __init__(self):
        self._seen = {}
    def first_time(self, key):
        if key in self._seen:
            return False
        self._seen[key] = True
        return True
'''

COUNTER = '''
class Counter:
    def __init__(self):
        self._hits = {}
    def bump(self, key):
        if key not in self._hits:
            self._hits[key] = 0
        self._hits[key] += 1
'''

QUERYSET_REAL = '''
class Query:
    def __init__(self, model):
        self._queryset = model.objects
    def filter(self, **kwargs):
        new = Query(self._model)
        new._queryset = self._queryset.filter(**kwargs)
        return new
    async def all(self):
        return await self._queryset.all()
'''

FLAG_IN_IF = '''
class A:
    def __init__(self):
        self._mode = "x"
    def run(self):
        if self._mode:
            return 1
        return 2
'''

NESTED_IN_TEST = '''
class A:
    def __init__(self):
        self._cfg = {}
    def run(self):
        if self._cfg.enabled:
            return 1
'''

CALL_IN_TEST = '''
class A:
    def __init__(self):
        self._items = []
    def run(self):
        if len(self._items) > 3:
            return 1
'''

COMPREHENSION_GUARD = '''
class A:
    def __init__(self):
        self._allow = set()
    def run(self, xs):
        return [x for x in xs if x in self._allow]
'''

WHILE_TEST = '''
class A:
    def __init__(self):
        self._pending = []
    def run(self):
        while self._pending:
            self._pending.pop()
'''

MATCH_SUBJECT = '''
class A:
    def __init__(self):
        self._kind = "a"
    def run(self):
        match self._kind:
            case "a":
                return 1
'''

CALLS_ONLY = '''
class A:
    def __init__(self):
        self._sink = Sink()
    def run(self, x):
        self._sink.write(x)
        return self._sink.total()
'''

CACHE_VECTORS = [
    ("_tokens memoization", TOKENS_REAL, "_tokens", True),
    ("_first_failure_time inspects value", FIRST_FAILURE_REAL, "_first_failure_time", False),
    ("seen-set: presence IS the answer", SEEN_SET, "_seen", False),
    ("counter: augmented assignment", COUNTER, "_hits", False),
    ("_queryset is not a cache", QUERYSET_REAL, "_queryset", False),
]

DECISION_VECTORS = [
    ("_queryset never tested", QUERYSET_REAL, "_queryset", False),
    ("calls and reads only", CALLS_ONLY, "_sink", False),
    ("bare if test", FLAG_IN_IF, "_mode", True),
    ("attribute member in test", NESTED_IN_TEST, "_cfg", True),
    ("inside a call in a test", CALL_IN_TEST, "_items", True),
    ("comprehension guard", COMPREHENSION_GUARD, "_allow", True),
    ("while test", WHILE_TEST, "_pending", True),
    ("match subject", MATCH_SUBJECT, "_kind", True),
    ("membership test is still a decision", TOKENS_REAL, "_tokens", True),
]


def main() -> int:
    failures = 0

    print("RULE A -- memoization cache (True = suppress the finding)")
    for name, src, attr, expected in CACHE_VECTORS:
        got = clears_as_cache(src, attr)
        ok = got == expected
        failures += not ok
        print(f"  {'ok  ' if ok else 'FAIL'} {name:42} -> {got} (want {expected})")

    print("\nRULE B -- drives a decision (True = genuinely branches)")
    for name, src, attr, expected in DECISION_VECTORS:
        got = drives_a_decision(ast.parse(src), attr)
        ok = got == expected
        failures += not ok
        print(f"  {'ok  ' if ok else 'FAIL'} {name:42} -> {got} (want {expected})")

    print(f"\n{'all vectors pass' if not failures else f'{failures} vector(s) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
