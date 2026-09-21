"""An empty waiting room, built fresh for one database.

    fn room_make : () -> Room

`docs/architecture.md:140`: a Room holds the arrival order and the callers
waiting on tickets, and both are empty whenever nobody is mid-flight, which is
most of the time. A caller deposits a write, takes a ticket and leaves; a crew of
drainers empties the room one write at a time, in arrival order.

IT IS A WAITING ROOM, NOT A STORE. Nothing here survives the process, and there
is no pending list to recover after a crash, because every write in the room has
a caller holding its ticket and the write-ahead log is already the queue. So the
room starts empty every time, and starting empty is the whole of what this
function decides.

TWO CALLS HAND BACK TWO ROOMS. This is the one thing the function can get wrong.
Python evaluates a default argument once, at definition, so a room built up there
and handed out on every call would be one room shared by every database in the
process: two callers' writes would land in one arrival order and each drainer
would empty a queue it does not own. Building the lists inside the call is what
keeps them apart, and the feature's Then makes two rooms and deposits into one to
prove it.

Pure: nothing in, one dict out. No clock, no random source, no environment, no
file and no connection.
"""

from declaro_persistum.types import Room


def room_make() -> Room:
    """Build an empty waiting room."""
    return {"arrivals": [], "waiting": []}
