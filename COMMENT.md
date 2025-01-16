Q: Some of those cases are covered in tests, some are not. Which ones?

A1: Processing an update with filled greater than amount needed for order execution.

order: total 0.8

update with filled 0.5

update with filled 0.7

will result in order state of filled 0.7, since order filled is always updated to the filled amount of the update

This could be exposed in tests by "T4: process two updates resulting in two transactions" if 2nd update had filled 0.7
instead of 0.8.

A2: Transactions may have wrong amounts since calculation `updated.filled - state.filled` doesn't handle the case when
order's remaining amount is different than its total amount:

(1, 0.8, 0) ->
-> (2, 0.8, 0.5)
-> (3, 0.8, 0.8)

(literally the case from the README.md)

This could be exposed in tests by the same change as in A1.

A3: Currently "duplicate updates" are not tested nor deduplicated, as there is no identity of the updates which could be
used for deduplication. There is a test "T3: process a case where the same message is delivered twice", but it's passing
since it's not checking how many transactions were created as a result of processing two updates. 