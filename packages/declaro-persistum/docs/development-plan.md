# Development plan

This is a plan, so unlike the architecture document it is allowed to go stale. It describes the order to build the declarative surface in, and why that order and not another. `HOW-IT-SHOULD-WORK.md` says what is being built. `docs/architecture.md` says what shape it has to take.

## How to read it

Each stage names what it is, what makes it done, and what it unblocks. "Done" is always a check somebody else can run, never a judgement. Most stages are done when a feature file that exists today goes from red to green and comes off the unbound list in `tests/unit/test_every_feature_is_bound.py`.

The specification is already written. `tests/bdd/features/asking/` holds one feature file per function, 80 scenarios, all failing on `NotImplementedError`. Building a function means making its file pass, and nothing here asks anyone to invent an expected value that isn't already written down.

## How to work

**One function, one commit.** A feature file goes green, and that is a commit. Not a stage, not an afternoon's work, not "the row questions". A commit holding three functions cannot be reverted without losing two that were fine, and a commit holding half of one cannot be reverted at all.

**Run the slop audit before every commit.** The audit reads the tree and reports what a scan could not clear: unexercised branches, loose parameters, silences. Run it on the work you are about to commit, not on a morning's worth.

    uv run slop-audit-l1 --honest-code-clauses

L1.21 is off unless you ask for it by name, so a plain run prints a complete-looking report with it silently absent.

**Before rather than after, because before you can still act on it.** Run it after and a number that got worse is already in the history, and fixing it is a second commit that exists only to undo the first. Run it before and the fix is part of the same change. You still know which commit moved the number, because it is the one you have not made yet.

**A worse number can still be right.** A new function has branches nothing exercises yet, and the test that clears them may honestly belong in the next commit. The rule is that you read the number and decide, not that the number never rises. What is forbidden is committing without looking.

## The order, and why

**One path before many.** Every question in the vocabulary is a variation on turning a declared model plus a narrowing into a statement, running it, and building instances from what comes back. Build that path once, for the simplest question, and the rest are rows in a table. Build several at once and you get several paths that have to agree.

**The vocabulary before the dialects.** A dialect is a grammar over the vocabulary. Four grammars over an empty vocabulary is four skins over nothing, and each would grow its own machinery while waiting.

**Reads before writes.** A wrong read shows you the wrong answer. A wrong write changes the database. The read path exercises the whole spine, so take the cheaper failure first.

**Retire first, then build against red.** The imperative surface goes in stage 1, before anything replaces it. That turns most of the suite red, and red is where the work starts: a failing test names what is missing, and a passing suite over a surface nobody wants names nothing. Keeping the old surface alive until its replacement lands means building beside it and comparing, which is how two paths that must agree get created.

## Stage 1: retire the imperative surface

`execute`, `execute_one`, `execute_scalar` and `execute_many` are deleted. `select` stops taking `where` as a string. `writing`, `reading` and `migrating` stop being public, because every imperative call site exists because they hand out a live connection.

They do not stop existing. The internals still open connections and the applier still writes SQL, and both are the library doing its own job. What ends is a caller reaching them.

**Expect the suite to go red**, including the three example apps, which make 24 calls between them. That is the point. Each red test names a job the declarative surface has to do, and the list of them is the specification for stages 2 onward.

**Done when** the gate in `the_test_for_any_function.feature` passes: no public function takes a live connection, hands one back, or takes a string that is neither a declared name nor a bound value.

**Why first:** a passing suite over a surface nobody wants tells you nothing. Deleting it first turns every place that depended on it into a question somebody has to answer, which is the only reliable way to find them all.

## Stage 2: one question, end to end

`all(db, Person)` returns instances of `Person`.

That means: read the table name and the columns off the model, build a statement, pick the parameter style from the dialect the `Database` already carries, open and close a connection, and turn rows into model instances. It is the whole spine for the simplest possible question.

**Done when** `tests/bdd/features/asking/all.feature` passes, including the scenario where a column the model does not declare is refused in the caller's own process before a statement exists.

**Unblocks** every other stage. Nothing else can start honestly until this runs, and it is what starts turning stage 1's red back to green.

## Stage 3: the rest of the rows

`one`, `first`, `page`, `ordered_by`, `values`, `distinct`, `exists`.

Each is a variation on stage 1 and should be a row in a table rather than a new function with its own path. Watch for the two that are not variations: `exists` must answer without reading rows, and `first` must refuse when no order is given, because "first" with no order is whichever row the engine reached.

**Done when** those seven feature files pass.

## Stage 4: narrowing, in full

The fifteen operators, `and`, `or`, `not`, and the case-insensitive companion for every text operator.

The operators already exist in `query/conditions.py`. This stage is making the declared surface reach all of them and refuse an operator nobody declared, by name, listing the ones that exist.

**Done when** `a_condition_is_data.feature` passes.

**Why here and not later:** every remaining stage narrows. Building writes first would mean building them twice.

## Stage 5: writes

`add`, `change`, `change_all`, `remove`, `remove_all`, `replace`.

Two rules from the specification decide the shape. A list passed to `add` lands together or not at all, and that is the only all-or-nothing on offer. Every one of them reports how many rows it touched, and zero is an answer a caller acts on rather than an error.

**Done when** those six feature files pass, including the group that cannot finish leaving the database unchanged.

## Stage 6: numbers and grouping

`count`, `sum`, `average`, `smallest`, `largest`, `count_distinct`, `grouped_by`, `having`.

Each returns the number, not a row carrying it. Over no rows a total is a named absence and not zero, because zero is a real total.

**Done when** those eight feature files pass.

## Stage 7: across models

`of`, and narrowing through a declared reference.

The foreign key already says two models are related. This stage works the join out from that rather than making the caller name it again.

**Done when** `of.feature` passes, including the scenario asserting the caller named no join and no key.

## Stage 8: the four dialects

Pseudo-SQL, Django, Prisma, SQLAlchemy, over the finished vocabulary.

A dialect borrows the other tool's grammar and nothing else: no session holding state, no manager attached to a class, no second place a column can be named. Each is a thin skin, and none of them accepts a string.

**Done when** `four_dialects_one_meaning.feature` passes, including the scenario where the same question asked four ways gives four equal answers.

## The schema axis, in parallel

These do not depend on the query stages and can run beside them.

**The capability table** (`declaro-hw6`). One row per capability, one column per engine, each cell saying native or naming what builds it. The three constraints on a cell are in the architecture document.

**Declared views and materialized views.** A view is a query with a name, declared against the models, with the library writing the SELECT. A materialized view adds one declaration: how fresh you need it.

**Declared constraints.** A constraint is a rule about a column. `field(check=...)` takes a SQL string today, which is the escape hatch in a different costume.

**Ranges** (`declaro-oyy`) are refused at the loader. **Hierarchies** (`declaro-7dk`) do not exist. Both were claimed as supported.

## Not in this plan

**The old refactor epic.** `declaro-75s` and its phases were written against a connection pool that no longer exists. Phases 0 and 1 are closed as already done. What remains of it should be re-read against this plan before anybody works it.

**The model check.** A lint refusing validators, methods, properties and foreign inheritance in a declared model. Written, uncommitted, eight tests failing because real models in this package carry those habits, and one rule in it was wrong and removed. It is a good idea half-built and it is not on this path.

**A CI gate** (`declaro-5gk`). Nothing runs on a push. The pre-commit and pre-push hooks cover this machine only.
