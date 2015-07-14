# From CD to ACID: Adding Atomicity and Isolation to DynamoDB

Out of the box, DynamoDB provides two of the four ACID properties: Consistency and Durability. Within a single item, you also get Atomicity and Isolation, but when your application needs to involve multiple items you lose those properties.  Sometimes that's good enough, but many applications, especially distributed applications, would appreciate some of that Atomicity and Isolation as well. Fortunately, DynamoDB provides the tools (especially optimistic concurrency control) so that an application can achieve these properties and have full ACID transactions.

This document outlines a technique for achieving atomic and (optionally) isolated transactions using DynamoDB. The atomicity strategy is a multi-phase commit protocol. To avoid losing data on coordinator failure, the coordinator state is maintained in DynamoDB. To avoid the need for failure detection, the protocol is designed so that there can be many active coordinators working on the same transaction. Isolation is available at various isolation levels, which is described later.

The techniques described here are implemented and available for use as an extension of the AWS SDK for Java, and can be downloaded on GitHub.

## Part One: Atomic writes

An atomic write transaction includes a set of update commands, each applied to a different DynamoDB item. The guarantee is that, when the transaction is complete, either all of the commands are executed, or none of them are.

### TX Records

The state of an in-flight transaction is stored in a DynamoDB item called a TX record. It has these attributes:

* a primary key. Any unique key will do, so a UUID is used.
* a state. The state starts as pending, and is eventually updated to either committed or rolled-back.
* a list of DynamoDB items participating in the transaction. Each item is given an id that is unique within the transaction.
* a set of update commands. Each is an instruction for changing a DynamoDB item.
* a timestamp indicating approximately when the transaction was last worked on.
* a version number storing for detecting concurrent changes to a TX record.

### Locks

Every DynamoDB item which participates in the protocol needs to have a lock attribute. Most of the time, this is set to the null string, but when the item is participating in a transaction, the lock is set to the primary key of the transaction's TX record. Each item can only participate in one transaction at a time.  Additionally, each item participating in the transaction contains:

* an applied flag, indicating whether the transaction has performed the write to the item yet.
* a transient flag, indicating if the item was inserted in order to acquire the lock.
* a timestamp indicating approximately when the item was locked.

### Item Images

After each item is locked and before it is changed during the transaction, a complete previous copy of the item is kept in the transaction table.  The requests in the transaction can be invalid, and their validity is not known until they are applied to the item, so we need a way to roll back a partially applied (but not committed) transaction.  Examples of errors include an update request to add "1" to the string attribute "Foo".  Each item image contains:
* all attributes of the item before it was modified as a part of the transaction
* the primary key of the transaction
* the unique id of the request within the transaction

Item images are not saved:
* for items which are read lock requests
* if the item is "transient" in the request, meaning that the item did not exist before it was locked

### No contention

We start by considering the case of a transaction where none of the items are currently locked. In the next section, we'll add a dash of secret sauce to deal with contention. In this happy case, a coordinator executes a transaction by following these steps: 

* create. Insert a new TX record, ensuring that the primary key of the TX record is unique.
* add. Add the item to the TX record's item list and assign it a unique request id.  Also add the full update request for that item to the TX record.  Use concurrency control to ensure that the TX record is still pending, and that it has not been changed since you last read or updated the TX record.
* lock. Set the item's lock attribute to point to the TX record.  If there is no item to lock, insert a new one, with a lock, and marked as transient.  Use optimistic concurrency control to detect contention and to detect whether or not the item is transient.
* save. Save a copy of the item in the transactions table, using optimistic concurrency control to avoid overwriting it.  Skip this step if your copy is already marked as applied, if it is a read request, or the item is transient.
* verify. Re-read the TX record to ensure it is still in the pending state.
* apply. Perform the requested operation on the item. This changes the item before the transaction has committed, so clients who are reading the item without a read lock need to be aware that they are reading uncommitted writes. Delete requests are not actually performed at this step, since they would unlock the item, and the transaction has not committed yet. 
* commit. This is the key moment. The coordinator uses optimistic concurrency control to move the state of the TX record form pending to committed. For now, we ignore the case where some other coordinator rolls back the transaction due to contention.
* complete. Once the transaction commits, complete it by deleting the old item images, and unlocking each item. If an item was marked as transient, or if the request for that item was a delete request, delete the item.
* clean. Once the transaction is complete all locks are clear and the TX record is marked as complete.
* delete. Once the caller has noted that the request is complete, and enough time has passed where the caller is confident that no other coordinator could be working on the transaction, the caller can delete the TX record.

### Contention with other transactions

Contention happens when a lock attempt fails because that item is already part of some other transaction. To resolve the contention, the coordinator removes the lock. It does so by deciding the other transaction, and then completing it:

* decide. Follow the lock to the transaction's TX record. If the transaction is pending, decide it by moving it from pending to rolled-back (using optimistic concurrency control, of course).
* complete. If the transaction was committed, then use the same code as before to complete it, removing locks, and deleting transient items and all of the old item images. If the transaction was rolled back, then use the old item images to revert them, releasing their locks, and delete the old item images.
* clean. All locks are now clear and the contending TX record is marked as complete. The creator of the conflicting transaction can use the TX record to determine if the transaction completed or rolled back.
A problem with this aggressive approach is that coordinators can do battle, each rolling back the other other's transactions, and no one making much forward progress. This is a liveness issue, not a safety issue: the protocol as stated is correct. There are many techniques which can decrease contention. For example, a coordinator can pause before rolling back a transaction, giving the competing coordinator a chance to finish its work. This is especially useful if each coordinator acquires locks in the same order, so that deadlock is prevented. There are lots more techniques you can dream up: dreaming is left as an exercise to the reader.
A bigger problem with this approach is that uncommitted reads are visible to the rest of the application if they are not using read locks.  Read isolation is discussed later.

### Contention with other coordinators

The protocol supports multiple coordinators working on the same transaction at the same time, including resuming a pending transaction, adding requests, and committing. To support this, the coordinator uses optimistic concurrency control when updating the TX record to ensure it makes valid state changes.  Before applying an update to an item, the coordinator checks the state of the TX record to ensure it was not moved out of pending by another coordinator in between locking an item and applying the change.  This interaction is most easily described by how a coordinator resumes a pending transaction that it did not start, or when contention is encountered with another coordinator of the same transaction:

* read. Read the TX record
* verify. Ensure that the transaction is in pending.  If it is committed or rolled-back, drive the commit or rollback to completion.
* catch up. Read each update from the TX record, and verify that each item is locked and backed up, and each update is applied using the algorithm above.

### Cleaning up

Transaction items can be useful to leave around even when the transaction has been fully committed or rolled back.  If a different coordinator completed a transaction than initiated it, they may want to leave the transaction record around so that the initiator can determine if their transaction was rolled back or successfully committed.  If a competing transaction rolled back a transaction and then deleted the TX record, the caller would never be able to determine the fate of the transaction.  
One clean up approach is for transactions to be deleted only by the originator of the transaction.  This would leave TX records around only when the original coordinator dies before it can delete the TX record.

Another solution is to mark each transaction with the wall-clock time of last update, and delete transactions only once they have been completed, and have not been updated for some configured period of time.  The application must run a sweeper process to periodically scan the transaction record for stuck or completed transactions, and move them along by rolling back and eventually deleting transaction records deemed "old enough".  Clock skew and extreme delay in the application (such as persisting TX records outside of DynamoDB) can still cause confusion between coordinators, but is greatly reduced, and it doesn't affect correctness of the algorithm.

### Performance and scaling

As implemented, this protocol requires 7N+4 writes.  The 7N comes from: 3 for each item record for locking, making the change, and unlocking, 2 more saving and deleting each item's old image, and 2 more for each item to add each request to the TX record and later verify the TX record state.  The extra 4 are to create the TX record, one to commit, one to mark it as finalized, and one to clean up.  If desired, the transaction can be deleted instead of being marked as complete.  This analysis assumes that each request in the transaction is an update to existing items.  The algorithm is cheaper for obtaining read locks and inserting new items, since in these cases the old item images do not need to be saved.

The protocol will scale to any transaction rate, thanks to DynamoDB's behind-the-scenes partitioning. Two unrelated transactions do not interfere with one another. The table of TX records can be indexed using a hash key, which provides nearly unlimited scaling.

As defined, the protocol will not scale to transactions with a large number of update commands. That's because the TX record, which must hold all the update commands, is limited, like any DynamoDB item, to 64K. One possible fix is to use a hash plus range key for the TX table, where one of the records (say the record with range key zero) is the TX record itself, and the other records in the range represent the items and updates.

## Part Two: Isolation

So far, we have not discussed reads. There are several approaches for incorporating read isolation with this algorithm, including ones not covered here.  This library provides 3 different read isolation levels:

The simplest approach is just to ignore locks. The problem is that this does not provide isolation: read transactions can see partial writes, and even uncommitted writes which may be rolled back, since the algorithm has to apply changes before commit in order to see if the transaction even can commit.

A stronger form of isolation is similar to what DynamoDB offers today: where you are guaranteed to read only committed changes, but without a "consistent cut", meaning that you could read some items from before a transaction commits and other items from after it commits.  This is accomplished by taking advantage of the fact that the algorithm saves the old item image away before it applies changes.  The item is read directly, without taking a lock, and if the item is marked as locked by another transaction and "applied", then the old item image is read instead.  This approach doesn't guarantee that you are returned the latest committed version of the item since the transaction locking the item can be committed, but not unlocked yet.  However it avoids the pitfalls of the weakest read consistency style by returning only item states that were committed.

The strongest form of read isolation is use read locks. An easy implementation is to code a read transaction exactly like a write, except that at the conclusion of the transaction you always roll back. This provides full ACID semantics, at the cost of turning reads into relatively expensive writes. Still, it scales, it's simple, and DynamoDB is so fast that this approach will be suitable for many applications.

## Limitations

The protocol described here has some problems. These limitations include:

### Range queries

There is no provision for locking ranges, so transaction which include range queries are subject to phantom reads. This could be solved by storing locks for specific ranges in a new item with the same hash key value as the range. 

### Cost

There are cheaper approaches to performing transactions on DynamoDB, but they each come with their own set of limitations in terms of capability. Some approaches rely on a global clock, which is a scaling bottleneck acceptable to some applications, while other approaches do not have the ability to handle bad requests to DynamoDB, or can only be scoped to items in a "parent/child" relationship.
