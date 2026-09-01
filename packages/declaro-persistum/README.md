# declaro-persistum

> ## READ THESE FIRST. NOT OPTIONAL.
>
> Nothing in this document may be reasoned about, quoted or acted on until you have read all three, word for word:
>
> 1. [Concurrent writes on Turso Cloud](https://turso.tech/blog/concurrent-writes-on-turso-cloud)
> 2. [Turso 0.7.0](https://turso.tech/blog/turso-0.7.0)
> 3. [Concurrent writes in practice](https://turso.tech/blog/concurrent-writes-in-practice)
>
> ### A SYNC CONNECTION AND A WRITE TO THE JOURNAL ARE TWO DIFFERENT THINGS
>
> Every wrong answer this project has produced about concurrency came from treating them as one thing. Read this table before you write a sentence containing the word "concurrency".
>
> | | **a write to the journal** | **a sync connection** |
> |---|---|---|
> | what it is | one transaction appending to the local database file | the channel between this copy and the primary |
> | how many at once | many, under MVCC with `BEGIN CONCURRENT` | exactly one |
> | what limits it | row-level write conflicts, at commit | the engine: a second one answers `database tape error: database is busy` |
> | when it happens | whenever a caller writes | on push and pull |
> | does it bound writers | no | **NO** |
>
> **The one sync connection is not a writer and does not make writers wait.** It carries a log to and from the primary. Local writes go to the journal, concurrently, and the sync connection ships them later. A measurement that a second sync connection fails is a fact about the channel and says NOTHING about how many writes can be in flight.
>
> So: "a replica permits one connection" is false. "A replicated database serialises writers" is false. Turso's sync engine syncs MVCC databases, so replicated and concurrent are not opposites. If a sentence concludes something about concurrency from a count of connections, it is wrong.


Your models say the `users` table has an `email` column. Your migration script says it has `email_address`. Both are checked in, both look right, and you find out which one production believes on a Friday.

That happens because you wrote the schema down twice: once as the thing you want, once as the steps to get there. Two descriptions of one fact will disagree eventually.

This library keeps one. You declare the models. It reads the live database, works out the difference, and applies it.

```python
from pydantic import BaseModel
from declaro_persistum import field, table

@table("people")
class Person(BaseModel):
    id: int = field(primary_key=True)
    name: str
    email: str
```

```python
from declaro_persistum import apply_migrations_async, open_sqlite
from declaro_persistum.types import NOTHING

db = await open_sqlite("app.db", shutdown="exit_immediately", busy_timeout_s=5.0)
await apply_migrations_async(db, "models.py", expand_enums=NOTHING, force=NOTHING)
```

That creates the table. Add a column to the model and run it again, and the column appears. You never write a migration, and you never say "add a column" — you declare that the column exists and reaching that state is the library's problem.

The same rule covers reading and writing. You name a model and the columns you care about, and the rows come back as instances of that model. No SQL text, no connection to hold, no engine name anywhere in your code.

Every question gives back an answer in one shape, and you read the key you asked for. `answer["rows"]` holds the instances, `answer["number"]` holds a count, `answer["row"]` holds one row or a named absence. `answer["failure"]` holds what went wrong, when something did. Every key a question did not fill is `NOTHING`, so a missing row and a failure are values you have to look at rather than things you can walk past.

## Why it is built this way

Every rule here removes a kind of bug rather than making something read nicer.

**One description, so nothing can drift.** The models are the only place a column is named. A rename is a type error rather than a discovery.

**A failure is returned, never logged.** A function that writes an error somewhere and carries on has reported it where you cannot reach it. Yours come back as values.

**Absence is an answer you have to handle.** Asking for a row that is not there gives you a named absence, not `None` and not an empty space that behaves like a row until it doesn't.

**Nothing has a silent default.** `expand_enums=NOTHING` above is not ceremony. A default absorbs your omission, so afterwards nothing can tell a caller who chose from a caller who forgot, and the path nobody chose is the path nobody tested.

## Databases

SQLite, PostgreSQL and Turso, local or replicated.

One line changes between them, the one that opens the database. Which engine you are on decides nothing else in your code: the parameter style, the transaction boundaries and every difference in the SQL each engine accepts are the library's business.

```python
db = await open_sqlite("app.db", shutdown="exit_immediately", busy_timeout_s=5.0)
db = await open_postgresql(dsn, shutdown="exit_immediately", busy_timeout_s=5.0)
db = await open_turso(path, primary=url, token=token, shutdown="exit_immediately")
```

## Install

```
uv add declaro_persistum
```

Python 3.11 or newer.

## Where the rest is written down

[HOW-IT-SHOULD-WORK.md](HOW-IT-SHOULD-WORK.md) is the specification: the four ways you can phrase a query, the whole vocabulary you can ask for, and how views, constraints and indexes are declared.

`tests/bdd/features/` holds it as scenarios, one file per function.

Documents with `DO NOT USE` in the name describe an older shape of this package. They are kept as a record and should not be followed.
