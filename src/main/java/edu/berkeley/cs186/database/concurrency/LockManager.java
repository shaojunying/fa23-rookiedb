package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;
import java.util.stream.Collectors;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         */
        public boolean checkCompatible(LockType lockType, long except) {
            return locks.stream()
                    .filter(lock -> !lock.transactionNum.equals(except))
                    .allMatch(lock -> LockType.compatible(lock.lockType, lockType));
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
            for (Lock existingLock : locks) {
                if (existingLock.transactionNum.equals(lock.transactionNum)) {
                    existingLock.lockType = lock.lockType;
                    return;
                }
            }
            locks.add(lock);
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            locks.remove(lock);
            transactionLocks.get(lock.transactionNum).remove(lock);
            processQueue();
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            if (addFront) {
                waitingQueue.addFirst(request);
            }else {
                waitingQueue.addLast(request);
            }
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            Iterator<LockRequest> requests = waitingQueue.iterator();
            while (requests.hasNext()) {
                LockRequest lockRequest = requests.next();
                // 是否可以grant这个锁
                boolean canCompatible = checkCompatible(lockRequest.lock.lockType, lockRequest.transaction.getTransNum());
                if (!canCompatible) {
                    break;
                }

                requests.remove();

                lockRequest.releasedLocks.forEach(this::releaseLock);
                grantOrUpdateLock(lockRequest.lock);
                transactionLocks.computeIfAbsent(lockRequest.transaction.getTransNum(), v -> new ArrayList<>())
                                .add(lockRequest.lock);


                lockRequest.transaction.unblock();
            }
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            return getTransactionLock(transaction)
                    .map(v -> v.lockType)
                    .orElse(LockType.NL);
        }

        public Optional<Lock> getTransactionLock(long transaction) {
            return locks.stream()
                    .filter(lock -> lock.transactionNum.equals(transaction))
                    .findFirst();
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     *
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     * by `transaction` and isn't being released
     * @throws NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        boolean shouldBlock = false;
        synchronized (this) {
            List<Lock> locksForTransaction = transactionLocks
                    .computeIfAbsent(transaction.getTransNum(), v -> new ArrayList<>());
            ResourceEntry resourceEntry = resourceEntries.computeIfAbsent(name, v -> new ResourceEntry());

            // 判断是否需要可以申请锁
            this.checkIfShouldAcquireLock(locksForTransaction, resourceEntry, lockType,
                    transaction.getTransNum(), name);

            Lock lock = new Lock(name, lockType, transaction.getTransNum());

            // 判断是否需要阻塞
            if (this.shouldBlock1(resourceEntry, lockType,
                    transaction.getTransNum())) {
                // 开始阻塞
                shouldBlock = true;
                LockRequest lockRequest = new LockRequest(transaction, lock);
                this.prepareForBlock(resourceEntry, lockRequest, transaction);
            }else {
                // 不应该阻塞，直接申请锁即可
                releaseNames.forEach(resourceName -> {
                    ResourceEntry resourceEntryToRelease = resourceEntries.getOrDefault(resourceName, new ResourceEntry());
                    List<Lock> locksForTransactionToRelease = transactionLocks.getOrDefault(transaction.getTransNum(), Collections.emptyList());

                    checkIfShouldReleaseLock(resourceEntryToRelease, transaction.getTransNum(), resourceName);

                    doRelease(locksForTransactionToRelease, resourceEntryToRelease, resourceName, transaction.getTransNum());
                });

                doAcquireLock(locksForTransaction, resourceEntry, lock);
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    private void releaseLock(Lock lock) {
        // 1. 从transactionLocks中删除
        // 2. 从resourceEntries中释放锁
        boolean removed = this.transactionLocks
                .get(lock.transactionNum)
                .remove(lock);
        if (!removed) {
            throw new NoLockHeldException(String.format("%s事务上不存在%s锁", lock.transactionNum, lock));
        }
        this.resourceEntries.get(lock.name).releaseLock(lock);
    }

    private void grantLock(TransactionContext transaction, ResourceName name, Lock lock) {
        this.transactionLocks
                .computeIfAbsent(transaction.getTransNum(), v -> new ArrayList<>())
                .add(lock);
        this.resourceEntries
                .get(name)
                .grantOrUpdateLock(lock);
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     * `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        boolean shouldBlock = false;
        synchronized (this) {
            List<Lock> locksForTransaction = transactionLocks
                    .computeIfAbsent(transaction.getTransNum(), v -> new ArrayList<>());
            ResourceEntry resourceEntry = resourceEntries.computeIfAbsent(name, v -> new ResourceEntry());

            // 判断是否需要可以申请锁
            this.checkIfShouldAcquireLock(locksForTransaction, resourceEntry, lockType,
                    transaction.getTransNum(), name);

            Lock lock = new Lock(name, lockType, transaction.getTransNum());

            // 判断是否需要阻塞
            if (this.shouldBlock1(resourceEntry, lockType,
                    transaction.getTransNum())) {
                // 开始阻塞
                shouldBlock = true;
                LockRequest lockRequest = new LockRequest(transaction, lock);
                this.prepareForBlock(resourceEntry, lockRequest, transaction);
            }else {
                // 不应该阻塞，直接申请锁即可
                doAcquireLock(locksForTransaction, resourceEntry, lock);
            }
        }

        if (shouldBlock) {
            transaction.block();
        }
    }

    private void doAcquireLock(List<Lock> locksForTransaction, ResourceEntry resourceEntry, Lock lock) {
        locksForTransaction.add(lock);
        resourceEntry.grantOrUpdateLock(lock);
    }

    private void prepareForBlock(ResourceEntry resourceEntry, LockRequest lockRequest, TransactionContext transaction) {
        resourceEntry.addToQueue(lockRequest, false);
        transaction.prepareBlock();
    }

    private boolean shouldBlock1(ResourceEntry resourceEntry, LockType lockType, long transNum) {
        return !resourceEntry.checkCompatible(lockType, transNum) || !resourceEntry.waitingQueue.isEmpty();
    }

    /**
     * 判断是否应该申请锁：
     * 1. 不能重复加相同的锁
     * 2. 判断是否可以用新的锁替换旧的锁
     */
    private void checkIfShouldAcquireLock(List<Lock> locksForTransaction,
                                          ResourceEntry resourceEntry, LockType lockType,
                                          long transNum, ResourceName name) {
        resourceEntry.getTransactionLock(transNum).ifPresent(existingLock -> {
            // 不能重复加锁
            if (existingLock.lockType.equals(lockType)) {
                throw new DuplicateLockRequestException(
                        String.format("不允许对同一事务对同一资源添加相同的锁: " +
                                "事务ID(%s), 资源名称(%s), 锁类型(%s)", transNum, name, lockType));
            }
            // 需要判断新的锁能不能替换掉旧的锁
            if (!LockType.substitutable(lockType, existingLock.lockType)) {
                throw new InvalidLockException(
                        String.format("新的锁类型(%s)不能替换现有的锁类型(%s)：事务ID(%s), 资源名称(%s)",
                                lockType, existingLock.lockType, transNum, name));
            }
        });
    }

    /**
     * 是否应该阻塞：
     * 1. transaction对name资源重复加锁，throw Exception
     * 2. 新的锁无法替换掉旧的锁，throw Exception
     * 3. 不能和resource上的锁共存，应该阻塞
     */
    private boolean shouldBlock(TransactionContext transaction, ResourceName name, LockType lockType) {
        ResourceEntry resourceEntry = resourceEntries.get(name);
        if (!resourceEntry.waitingQueue.isEmpty()) {
            return true;
        }
        LockType existingLockTypeForThisTransaction =
                resourceEntry.getTransactionLockType(transaction.getTransNum());
        if (lockType.equals(existingLockTypeForThisTransaction)) {
            throw new DuplicateLockRequestException("");
        }

        if (!LockType.substitutable(lockType, existingLockTypeForThisTransaction)) {
            throw new InvalidLockException("");
        }

        return !resourceEntry.checkCompatible(lockType, transaction.getTransNum());
    }


    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     *
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * 这里的前提是，当前事务不在waitingQueue了，已经真正持有锁了，这时才能释放
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        synchronized (this) {
            ResourceEntry resourceEntry = resourceEntries.getOrDefault(name, new ResourceEntry());
            List<Lock> locksForTransaction = transactionLocks.getOrDefault(transaction.getTransNum(), Collections.emptyList());

            checkIfShouldReleaseLock(resourceEntry, transaction.getTransNum(), name);

            doRelease(locksForTransaction, resourceEntry, name, transaction.getTransNum());
        }
    }

    private void doRelease(List<Lock> locksForTransaction, ResourceEntry resourceEntry, ResourceName name, long transNum) {
        Lock lock = resourceEntry.getTransactionLock(transNum).orElseThrow(() -> new NoLockHeldException(
                String.format("尝试释放未持有的锁: 事务ID(%s), 资源ID(%s)。" +
                        "事务未在指定资源上持有锁。", transNum, name)));
        locksForTransaction.remove(lock);
        resourceEntry.releaseLock(lock);
    }

    private void checkIfShouldReleaseLock(ResourceEntry resourceEntry, long transNum, ResourceName name) {
        if (resourceEntry.getTransactionLockType(transNum).equals(LockType.NL)) {
            throw new NoLockHeldException(
                    String.format("尝试释放未持有的锁: 事务ID(%s), 资源ID(%s)。" +
                            "事务未在指定资源上持有锁。", transNum, name));
        }
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     *
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        boolean shouldBlock = false;
        synchronized (this) {
            ResourceEntry resourceEntry = resourceEntries.get(name);
            if (resourceEntry == null) {
                throw new NoLockHeldException("");
            }
            Lock lock = resourceEntry.getTransactionLock(transaction.getTransNum())
                    .orElseThrow(() -> new NoLockHeldException(""));
            Lock newLock = new Lock(lock.name, newLockType, lock.transactionNum);
            if (shouldBlock(transaction, name, newLockType)) {
                // 阻塞起来
                shouldBlock = true;
                transaction.prepareBlock();
                resourceEntry.addToQueue(new LockRequest(transaction, newLock,
                        Collections.singletonList(lock)), true);
            }else {
                lock.lockType = newLockType;
            }

        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        ResourceEntry resourceEntry = getResourceEntry(name);
        return resourceEntry.getTransactionLockType(transaction.getTransNum());
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("LockManager的状态：\n");

        sb.append("transactionLocks:\n");
        transactionLocks.forEach((transNum, locks) -> {
            sb.append("  Transaction ").append(transNum).append(": ");
            locks.forEach(lock -> sb.append(lock).append(", "));
            sb.setLength(sb.length() - 2);  // 移除最后一个逗号和空格
            sb.append("\n");
        });

        sb.append("resourceEntries:\n");
        resourceEntries.forEach((resourceName, resourceEntry) -> {
            sb.append("  Resource ").append(resourceName).append(": ");
            sb.append(resourceEntry).append("\n");
        });

        return sb.toString();
    }

}
