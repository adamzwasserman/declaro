"""The queue is a waiting room in front of the WAL.

A WAL is already the queue: a write is durable once it is in the log, and
the engine applies it later. So the only job left is to buffer callers who
arrive at the same instant and hand them to the log in order.

This does not serialise the database. Turso supports concurrent writers
through MVCC and BEGIN CONCURRENT, and a writer still opens a
connection per concurrent caller.

Nothing is stored here. The room is empty except during the microseconds
when callers overlap.

Deposit returns a ticket immediately. The caller awaits that ticket and gets
back the same ticket with a success or failure code. That is what makes it
asynchronous rather than a lock: a caller can deposit several writes, keep
working, and collect when it actually needs the answer.
"""





def _write(n: int):
    return {"sql": f"INSERT INTO t VALUES ({n})", "params": (n,)}










