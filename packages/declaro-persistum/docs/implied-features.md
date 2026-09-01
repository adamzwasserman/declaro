# Features implied but never named

Both documents make claims that only hold if something exists to make them hold. This is that list: things the specification and the architecture promise by implication, which nobody has named, tracked or built.

It is a working list, so unlike the architecture it is allowed to go stale. Each entry says the sentence that implies it, and what would have to exist.

## From "the models are the truth"

**A model check.** "Everything reads those models" only holds if nothing else in the class carries behavior the library cannot see. A validator, a method or a property is a rule that applies in Python and never reaches the database. Half-built and uncommitted.

**Type coverage for every declarable column type.** "The library never asks the database what is there" means the loader must understand every type a model can declare. It refuses `tuple[int, int]` today. The set of types it accepts is not written down anywhere, so nobody can tell a gap from a decision.

**A reserved-name rule.** A model can declare a column called `id`, and it can presumably declare one called `select` or `_declaro_meta`. Nothing says what happens.

## From "the schema is declared, not migrated"

**Drift detection.** "The library reads the live database, compares it to your models, and applies it" implies you can ask what the difference *is* without applying it. A dry run exists in the applier. Nothing in the declared surface exposes it.

**A rename decision, asked and remembered.** The differ returns ambiguity rather than guessing, which means somebody has to answer. Where the answer is asked for, and whether it is remembered so the next run does not ask again, is not described.

**Destructive-change refusal.** Dropping a column loses data. Nothing says whether that needs confirmation, a flag, or nothing at all.

## From "the data is declared too"

**A `Query` that is not SQL.** Every grammar produces something the runner takes. If that thing carries `sql` text, the grammars are four ways of building a string and the surface is imperative underneath. The shape of what a grammar produces is unnamed and it decides whether any of this works.

**Statement caching.** The library builds a statement from a declaration on every call. Whether the same declaration builds it once or every time is a real decision and appears nowhere.

**Result shaping beyond model instances.** `values` returns mappings, `rows` returns instances. What a join across two models returns is not named.

## From "what the library owns"

**Transaction scope for a declared group.** "It decides whether several writes travel as one transaction" is only true for `add` with a list. Whether two `change` calls can be grouped, and how a caller says so, is not described.

**Connection lifetime under concurrency.** "It chooses the connection, opens it, and closes it" says nothing about how many, or what happens when N callers ask at once. The crew answers this for writes on a local database. Nothing answers it for reads.

**Retry as a library decision.** Conflicts fire at commit and retries move rather than disappear. Backoff with jitter is the library's job by the ownership test, and `retry.py` holds a policy, but nothing in the declared surface says a caller never writes a retry loop.

## From "everything gets declared"

**A declared view needs a query language that is not SQL.** "The library writes the SELECT" means a view has to be declared as something the library can render. That something does not exist, and it is the same object the grammars need.

**Freshness as a declared word.** "You say how fresh you need it in plain terms" implies a bounded vocabulary of freshness. The current `refresh` field takes five engine-flavored strings across three engines.

**Index declaration on more than one column.** `field` is per column. A composite index has no home.

**Migration of a declared view.** Changing a view's definition has to produce an operation. A recorded defect says changing a materialized view's refresh migrates nothing and reports success.

## From "absence is an answer"

**A named absence that survives the boundary.** `NOTHING` is a Python value. What a caller gets across a process boundary, or in a response body, is not described.

**Failure as a value everywhere, not just at the edges.** "A function that cannot do its job returns that fact" is the rule. Which functions return a failure rather than raising, and how a caller tells the two apart, is not written down.

## From "the test for any function"

**A gate that runs the test.** The rule is stated and there is a feature file describing it. Nothing enforces it, so the next function taking a connection lands unopposed.

**An import boundary.** The rule covers public functions. Nothing stops a caller importing `declaro_persistum.query.executor` and using what is inside.

## From the architecture

**A purity check.** "The middle is pure" is enforced for I/O through `@helper`, and not for anything else. A middle function that reads a clock, a random source or an environment variable is impure and passes.

**One logging function, enforced.** Information goes through one declared function so there is one edge. Nothing stops a new module importing `logging` directly, and thirteen modules did exactly that before.

**A no-defaults gate.** "Nothing takes a default" is a rule nobody checks. The Honest Code principles say this one is the most cited and the least defended, and a defaulted argument passes every gate here.

**Engine parity as a test.** Every dispatch table has a row per engine. Nothing asserts the tables agree on which engines exist, so an engine can be added to four tables and missed in three.

**The capability table** (`declaro-hw6`), which is the one item on this list already tracked.
