# What declaro-persistum is

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
> So: "a replica permits one connection" is false. "A replicated database serializes writers" is false. Turso's sync engine syncs MVCC databases, so replicated and concurrent are not opposites. If a sentence concludes something about concurrency from a count of connections, it is wrong.


You describe what you want. The library works out how to get it. The name says so: declaro means "I declare."

You never write SQL. You never hold a database connection. You never learn which engine you are on.

## The models are the truth

You write Pydantic models. That is your only description of the database.

```python
@table("people")
class Person(BaseModel):
    id: int = field(primary_key=True)
    name: str
```

Everything reads those models. They say which tables exist, which columns each has, and what each column means. The library never asks the database what is there, because the models already said.

That is what makes a mistake cheap. Ask for a column no model declares, and the library stops you in your own process. It names your model and lists the columns it does declare. You never see "no such column" from an engine, because the request never got that far.

`field` declares meaning, not spelling. You say a column holds money with a floor of zero. Whether that becomes `numeric(10,2)` or `REAL` is the library's business.

## The schema is declared, not migrated

You change a model. The library reads the live database, compares it to your models, works out the difference, and applies it.

You write no migration scripts. You never say "add a column." You declare that the column exists, and reaching that state is the library's problem. The models are the desired state, and everything else is derived from them.

## The data is declared too

Reading and writing follow the same rule as the schema. You name a model and the columns you care about, and you say which shape you want the rows back in: instances of that model, or plain dictionaries of column name to value.

You choose because neither one is right for every question. An instance is what you want when you asked for a whole row and are about to work with it. A dictionary is what you get when you asked for two columns out of five, because two fields of a five-field model is not that model and calling it one would be a lie.

There are four ways to say it. They are grammars: four ways of phrasing one meaning. A team arriving from Django writes what it already knows, and so does a team arriving from Prisma or SQLAlchemy.

**Pseudo-SQL**, the library's own, reading in SQL's word order without SQL's text:

```python
people.select("id", "name").where({"name": "bob"})
```

**Django**, for people who think in managers and lookups:

```python
Person.objects.filter(name="bob").values("id", "name")
```

**Prisma**, for people who think in one call carrying named parts:

```python
Person.find_many(where={"name": "bob"}, select=["id", "name"])
```

**SQLAlchemy**, for people who think in a query object:

```python
Person.query.filter_by(name="bob").with_only("id", "name")
```

All four read the same models, refuse the same undeclared columns, and produce the same rows. Pick the one your team already speaks.

A grammar borrows the other tool's phrasing and nothing else. It does not bring that tool's machinery: no session holding state, no manager attached to a class, no second place where a column can be named. Each is a thin skin over the same declaration, which is what keeps four surfaces from becoming four sets of rules.

And no grammar is a way back to SQL. If one of them could take a string, all four would eventually be judged by it.

A condition is data in every grammar, so it composes the same way everywhere:

```python
{"age": {"gt": 18}, "status": "active"}
{"or": [{"status": "active"}, {"status": "pending"}]}
```

Writing has the same shape. You declare that a row exists and the library makes it so. Declare several together and they arrive together or not at all.

## What you can ask for

The vocabulary is small and every word means one thing. Each grammar spells these its own way; the meanings do not change.

### Rows

| You want | You ask for |
| --- | --- |
| every matching row | `all` |
| exactly one, or a named absence | `one` |
| the first, in a stated order | `first` |
| a page of them | `page(size, number)`, or `limit` and `offset` |
| them in an order | `ordered_by({"name": "ascending"})` |
| only some columns | `select("id", "name")` |
| no duplicates | `distinct` |
| whether any exist at all | `exists`, which answers true or false and reads no rows |
| the rows as your own model | `as_objects` |
| the rows as plain dictionaries | `as_dictionaries` |

Asking for only some columns and asking for your own model at the same time is refused, and it names both words. Two columns out of five is not a Person, and the library will not hand you something that says it is.

`page` is the one worth having over raw limit and offset. Paging is what people actually mean, and doing it by hand is where the off-by-one lives.

An order is a mapping of declared columns to `"ascending"` or `"descending"`, and the keys break ties left to right: `{"age": "descending", "name": "ascending"}` sorts by age, then by name within each age. `first` requires one, because the first row in no order is whichever row the engine reached.

This used to be `ordered_by("name")` with `"-name"` to reverse, and the minus is the reason it changed. Apply the test below to `"-name"` and it is neither of the two things it is allowed to be. It is not a declared name, because `-name` is not a column any model declares and something has to cut it apart before it can be looked up. It is not a value, because nothing binds it as a parameter. It is a column name carrying an instruction, which is the same hole as a WHERE clause given as text, and small enough that it survived two readings of this document.

### Numbers over rows

| You want | You ask for |
| --- | --- |
| how many | `count`, over the rows or over one column |
| the total | `sum("total")` |
| the average | `average("total")` |
| the smallest, the largest | `smallest("total")`, `largest("total")` |
| how many different values | `count_distinct("status")` |

Each of these returns the number, not a row carrying the number. Asking how many people there are gives you an integer.

### Narrowing

Fifteen operators, named as keys because a condition is data:

`eq` `ne` `lt` `lte` `gt` `gte` `in` `not_in` `between` `like` `contains` `startswith` `endswith` `is_null` `is_not_null`

A bare value means `eq`, so `{"name": "bob"}` and `{"name": {"eq": "bob"}}` are the same thing. `and` is what two keys already mean. `or` and `not` are keys carrying a condition.

Text matching is case-sensitive unless you say otherwise, and every text operator takes an `ignore_case` companion. That difference is not a detail: the same `like` matched a name on one engine and nothing on another, silently, and it is the library's job to make the engines agree rather than the caller's job to learn which is which.

### Grouping

| You want | You ask for |
| --- | --- |
| a number per group | `grouped_by("status")` with any of the numbers above |
| only some groups | `having({"count": {"gt": 5}})` |

`having` narrows groups, `where` narrows rows, and asking for one when you meant the other is the mistake this vocabulary should make hard to write.

### Writing

| You want | You ask for |
| --- | --- |
| a row to exist | `add(person)` |
| several rows to exist, together or not at all | `add([a, b, c])` |
| a row to hold new values | `change(person, name="robert")` |
| every matching row changed | `change_all(where, name="robert")` |
| a row gone | `remove(person)` |
| every matching row gone | `remove_all(where)` |
| a row to exist, whatever was there before | `replace(person)` |

Every one of them says how many rows it touched. A `change_all` that matched nothing returns zero, and zero is an answer you can act on.

### Across models

A foreign key already says two models are related, so you do not repeat yourself:

| You want | You ask for |
| --- | --- |
| the rows on the other side | `Order.of(person)` |
| rows filtered by something on the other side | `where({"person": {"name": "bob"}})` |
| a question inside a question | a query used as a value |

The library works out the join. You declared the relationship once, in the model, and naming it again in every query is the duplication this library exists to remove.

## What the library owns

You own the description: which models exist, what their columns mean, and which rows you want.

The library owns the rest. It writes the statement and picks the parameter style. It chooses the connection, opens it, and closes it. It decides whether several writes travel as one transaction. It absorbs every difference between PostgreSQL, SQLite and Turso.

That gives a clean test. If a decision could go differently on a different engine, it belongs to the library. Moving a project from SQLite to PostgreSQL changes one line, the line that opens the database.

## Everything gets declared, not just tables

A view is a query with a name. You declare it against your models, the way you declare a table, and the library writes the SELECT.

A materialized view adds one declaration: how fresh you need it. You say that in plain terms. PostgreSQL has a real materialized view and SQLite does not, and bridging that is the library's job rather than yours.

A constraint is a rule about a column. You declare the rule.

An index is a promise about speed. You declare the promise.

None of these gets an escape hatch back to SQL. A feature you can only reach by writing a string is not supported. It is bypassed.

## Absence is an answer

A row that is not there is a real result, not a failure and not an empty space. The library hands back a named absence and you have to look at it. Nothing silently stands in for the row that was missing.

The same holds throughout. A function that cannot do its job returns that fact to you. It does not write it to a log nobody reads, and it does not carry on and report success.

## The test for any function

Follow each argument to what it becomes. **Every string you pass is either checked against your declaration or bound as a parameter. Nothing is interpolated.**

`select("id", "name")` passes two strings, and both are looked up in the model and refused if it does not declare them. `where({"name": "bob"})` passes two more: the key is looked up the same way, and the value is bound. None of the four is ever pasted into a statement.

`where("active = :x")` looks identical to a scanner and is the opposite. That string is neither a declared name nor a bound value, so the only thing left to do with it is paste it in.

That is the whole test, and it does not need anyone to recognize SQL. A parameter that reaches string concatenation on its way to a statement is a hole whatever it happens to contain. It also catches `field(check="total >= 0")`, which no amount of looking for the word `SELECT` would.

The same applies to a live connection and the name of an engine. Each is the caller doing the library's work, and that is the one thing this library exists to prevent.
