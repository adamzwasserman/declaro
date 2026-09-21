# Why this suite is red, and what would be wrong with making it green

Decided 2026-09-21. The package is being rewritten from `docs/design/persistum.hd`, which declares 66 functions, and every one of them now has a feature file of its own name. Almost none of those functions exists yet.

On 2026-09-21 the suite ran 520 failed, 2,955 passed. Before the features were reconciled it ran 136 failed, 3,007 passed. The difference is about 390 scenarios that describe declared behavior nothing implements. They are not regressions and they are all one shape: not built yet.

There are two ways to make the number go down and both are worse than the number.

Unbinding the merged features would drop roughly 200 passing tests as well, and those cover the code being replaced. A green suite that tests the surface you are deleting is a more misleading signal than a red one that tells you how far the rewrite has to go.

Deleting the unbuilt scenarios would delete the specification. It was written from `HOW-IT-SHOULD-WORK.md`, `docs/architecture.md` and the feature files, one function at a time, and it is the only place the target is written down in the form the tests will take.

So the count comes down by building functions, not by changing what is asked of them. When a function exists, its feature goes green, and the number falls by itself.
