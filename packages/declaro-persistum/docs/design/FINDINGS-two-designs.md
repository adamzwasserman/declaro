# What is wrong with `with-a-declaration.hd` and `straight-to-sql.hd`

A working list, so it is allowed to go stale. Both files were drawn on 2026-09-01 to compare two designs for the query surface, and Adam found the faults below by reading the rendered map. They are recorded here rather than fixed, because fixing them one at a time is the wrong move: several say the drawing is of a library other than this one.

Neither file is built and neither describes what exists. `persistum.hd`, beside them, is the declaration of the package as it is.

## The design is of some other library

**The checking step is absent.** No card takes a model and a column name and refuses the columns the model does not declare. "Ask for a column no model declares and the library stops you in your own process" is the first thing `HOW-IT-SHOULD-WORK.md` promises, four feature files assert it, and neither drawing has a function for it.

**Nothing reaches four of the five orchestrators.** `migrate_views` has an entry, because `load_declarations` reads the schema file and hands it a `Schema`. The four `ask_in_*` have nothing to their left: no boundary, no chain step, no caller. The checker's uncalled rule covers `fn` and `boundary_out` and skips orchestrators, so four unreachable functions passed every check.

**The orchestrators own no sequence.** The tool's data edges show every hop fixed by the types: `declared_in_sqlalchemy → render → run`, with `answered` off the declaration. A pipeline whose steps are determined by types is a chain, not an orchestrator. The four are four copies of one pipe differing by which builder they start with.

**Six of the twenty-three words cannot be expressed.** `add`, `change`, `change_all`, `remove`, `remove_all` and `replace` have no path. `Declaration.values` exists and nothing produces or consumes it.

**Paging is gone.** No size, no number, anywhere. `page` is one of the twenty-three.

## The types are wrong

**`Declaration` is a tagged union wearing a record.** Nine fields whose meaning depends on `word`. `word="add"` with an `order` is expressible; so is `word="count"` with `values`. That manufactures untested input regions by construction, which is the fault the audit correlates with loose parameters at r = 0.749.

**The answer's type is decided at runtime by a string.** `Answer` has six keys and `FILLS[word]` picks one, so a caller cannot know from the types whether rows or a number come back.

**`Narrowing` cannot express the condition language.** A flat `list<Narrowing>` cannot hold `{"or": [...]}`, a `not`, or a nesting. Stage 4 is exactly that.

**`columns` does two jobs**: the projection for `values`, and the single column for `sum` and `average`.

**Every bounded vocabulary built this week is thrown away.** `engine: str` rather than `Engine`. `direction: str` rather than `Direction`. `failure: any` when `Failure` exists. `operator: str` for fifteen operators. `word: str` for twenty-three words. `freshness: str` when its own feature says plain words. `model: any`, six times, so the design cannot state its own first principle.

**`asked: dict<str, any>`** on all four grammars, an untyped bag in the one place the design should be precise.

**`Failure` is referenced and never declared.**

## Cards that should not exist, and cards that should

**`run` and `apply` are one card twice.** Same two arguments, same destination. One returns rows and one a count, and that is a property of the statement rather than of the function. Their declared side effects cannot hold either: send an INSERT through `run` and it writes.

**The real split is undrawn.** `reading(db)` waits for nothing, because a read never touches the primary. `writing(db)` scopes a transaction, commits on a clean exit, rolls back on an exception, and counts the write in flight so replication yields to it. Different behaviour on each side, and neither file has it.

**`views_in`, `declaration_of` and `statement_of` are field reads.** `schema["views"]`, `view["declaration"]`. They were added so the chain would have something to pass through.

**`run` is on the wrong side of the board.** It is declared `boundary_out` and it reads. `honest-persist.hd` declares its equivalent, `execute`, as `boundary_in ... reads "database"`.

**Four orchestrators differ by one invoked name**, which is the L1.21.1 shape.

**`migrate_views -> int` cannot fail.** The package's rule is that a failure is a value.

## The files themselves

**They redeclare names the package already declares, differently.** `Schema` and `Database` are in `persistum.hd` too: `Schema = dict<str, Table>` there and `{models, views}` here; `Database = {path, primary, engine, in_flight}` there and `{path, engine}` here. `Answer`, `Statement` and `View` are each declared twice more across the two files. Anything scanning this directory renders all three as equals, which is the drift the tool exists to prevent.

**`layer domain`**, where the framework's own persistence module says `layer data`.

## Fixed already

`persistum.hd` declared `serialize: any` on `Database`. The field was `serialise` until the write lock came out on 2026-09-01 and became `in_flight`; the American-spelling sweep then renamed the stale name rather than noticing it was stale. Corrected to `in_flight`.

## What to decide first

Whether these two files should exist at all. Several findings say the drawing is of another library, and the honest alternative is to declare the package as it is, in `persistum.hd`, and settle the query surface in `HOW-IT-SHOULD-WORK.md` where the vocabulary already lives.
