"""declaro-persistum, being rewritten from `docs/design/persistum.hd`.

TWO DOORS, AND THE DECLARATION SAYS WHICH TWO. `inputs` names `"caller"` as a
library call from the host program, and exactly two cards declare
`side_effect reads "caller"`: `boundary_in fn session_open` and
`boundary_in fn query_ask`. Every other door is reached by the platform, by a
drainer, or by an orchestrator, so a caller never names one. That is read off
the declaration rather than chosen here.

NOTHING IS EXPORTED UNTIL ITS FEATURE FILE IS BOUND AND GREEN, which is Adam's
rule from 2026-09-21, when the previous source was deleted: 67 modules and
30,013 lines written before the declaration existed and never reconciled with
it. Both doors below meet it. `session_open.feature` and `query_ask.feature` are
bound by their step files and pass.

THIS FILE WAS EMPTY UNTIL 2026-10-10 AND SAID SO FOR THREE WEEKS AFTER IT STOPPED
BEING TRUE. That cost a gate. `honest-check`'s HC-REF005 crosses a declaration
against the code and reports a declared name the module defines nowhere, and it
refuses to start on a module that assigns no `__all__`. With the list here it
runs over the whole package: 6 errors against `persistum.hd`, which are the cards
declared ahead of their code and recorded with their reasons, plus 16 against
`persistum-cli.hd` and 12 against `persistum-transfer.hd`.

THOSE TWENTY-EIGHT ARE A DESIGN WAITING FOR ITS CODE, NOT A DEAD ONE, and reading
them the other way is a mistake somebody has already made here. The command line's
code was deleted on 2026-09-25 while the declarations were reworked for the
rewrite, and Adam is building it back at a lower priority. Counting defined
against declared says 1 of 15 and 1 of 11 against this module's 70 of 75, and that
number cannot tell a parked design from a rotted one. Nothing can, from the
declaration alone.
"""

from declaro_persistum.query_ask import query_ask
from declaro_persistum.session_open import CouldNotOpen, session_open

__all__ = ["CouldNotOpen", "query_ask", "session_open"]
