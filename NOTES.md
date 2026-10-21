# PostgreSQL operations

Keep ledger entries immutable; enforce UNIQUE (payment_id, kind).
All writers must use these operations and the same locking protocol.
Use READ COMMITTED transactions and transaction-scoped advisory locks.
Use separate lock namespaces for payment IDs and member/month keys.
Award: BEGIN; lock payment_id, then read its existing award.
If an award exists, COMMIT without changing it.
Otherwise lock the member and UTC settlement month.
Read SUM(points) for awards in that bucket after acquiring the lock.
Insert min(requested_points, max(cap - used, 0)), including zero; COMMIT.
Reverse: BEGIN; lock payment_id, then read its original award and reversal.
If no award exists, COMMIT without writing and return unknown.
If already reversed, COMMIT without changing it.
Otherwise insert a reversal with the original member, month and negated points.
COMMIT; reversals never reduce the sum used to enforce the earning cap.
Use separate SQL statements for locking and subsequent reads.
Ack only after successful completion; Nack failures for broker redelivery.
A committed write followed by redelivery is safe because payment identity persists.
