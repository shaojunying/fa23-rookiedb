package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {

        checkIfShouldAcquire(transaction, lockType);

        lockman.acquire(transaction, name, lockType);

        if (parent != null) {
            parent.numChildLocks.put(transaction.getTransNum(),
                    parent.numChildLocks.getOrDefault(transaction.getTransNum(), 0) + 1);
        }
    }

    private void checkIfShouldAcquire(TransactionContext transaction, LockType lockType) {
        if (this.readonly) {
            throw new UnsupportedOperationException("当前LockContext只读，不能acquire Lock");
        }

        if (lockType.equals(LockType.NL)) {
            throw new InvalidLockException("不能acquire NL Lock");
        }

        if (!ifParentSupports(transaction, lockType)) {
            throw new InvalidLockException("父节点不支持当前LockType");
        }
    }

    private boolean ifParentSupports(TransactionContext transaction, LockType lockType) {
        if (parent == null) {
            return true;
        }
        LockType requiredParentLockType = LockType.parentLock(lockType);
        return LockType.substitutable(parent.getEffectiveLockType(transaction), requiredParentLockType);
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        checkIfShouldRelease(transaction);

        lockman.release(transaction, name);
        if (parent != null) {
            parent.numChildLocks.put(transaction.getTransNum(),
                    parent.numChildLocks.getOrDefault(transaction.getTransNum(), 0) - 1);
        }
    }

    private void checkIfShouldRelease(TransactionContext transaction) {
        if (readonly) {
            throw new UnsupportedOperationException("当前LockContext只读，不能release Lock");
        }
        if (hasChildLocks(transaction)) {
            throw new InvalidLockException("当前LockContext有子节点，不能release Lock");
        }
    }

    private boolean hasChildLocks(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0) > 0;
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        if (readonly) {
            throw new UnsupportedOperationException("当前LockContext只读，不能promote Lock");
        }

        LockType thisLevelLock = getExplicitLockType(transaction);
        if (thisLevelLock.equals(LockType.NL)) {
            throw new NoLockHeldException("当前LockContext没有锁，不能promote Lock");
        }

        if (thisLevelLock.equals(newLockType)) {
            throw new DuplicateLockRequestException("当前LockContext已经有相同的锁，不能promote Lock");
        }

        if (!LockType.substitutable(newLockType, thisLevelLock)) {
            throw new InvalidLockException("不能promote Lock");
        }

        // promote之后，是否会和parent不兼容
        if (!ifParentSupports(transaction, newLockType)) {
            throw new InvalidLockException("不能promote Lock");
        }
        
        List<ResourceName> resourceNamesToRelease = this.sisDescendants(transaction);

        resourceNamesToRelease.forEach(resourceName -> {
            LockContext lockContext = fromResourceName(lockman, resourceName);
            if (lockContext.parent != null) {
                lockContext.parent.numChildLocks.put(transaction.getTransNum(),
                        lockContext.parent.numChildLocks.getOrDefault(transaction.getTransNum(), 0) - 1);
            }
        });

        resourceNamesToRelease.add(name);
        lockman.acquireAndRelease(transaction, name, newLockType, resourceNamesToRelease);

    }
    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {

        if (readonly) {
            throw new UnsupportedOperationException("当前LockContext只读，不能escalate");
        }

        LockType thisLevelLock = this.getExplicitLockType(transaction);

        if (thisLevelLock.equals(LockType.NL)) {
            throw new NoLockHeldException("当前LockContext没有锁，不能escalate");
        }

        List<LockContext> descendants = getDescendants(transaction);
        descendants.add(this);

        boolean shouldToX = shouldConvertToExclusiveLock(transaction, descendants, thisLevelLock);
        LockType newLockType = shouldToX ? LockType.X : LockType.S;

        if (newLockType.equals(getExplicitLockType(transaction))) {
            return;
        }

        updateChildLockNum(transaction, descendants);

        List<ResourceName> resourceNames = descendants.stream().map(LockContext::getResourceName)
                .collect(Collectors.toList());
        lockman.acquireAndRelease(transaction, name, newLockType, resourceNames);
    }

    private void updateChildLockNum(TransactionContext transaction, List<LockContext> descendants) {
        for (LockContext descendant : descendants) {
            descendant.numChildLocks.put(transaction.getTransNum(), 0);
        }
    }

    private boolean shouldConvertToExclusiveLock(TransactionContext transaction, List<LockContext> descendants, LockType thisLevelLock) {
        if (isExclusiveLock(thisLevelLock)) {
            return true;
        }
        for (LockContext descendant : descendants) {
            LockType descendantLockType = descendant.getExplicitLockType(transaction);
            if (isExclusiveLock(descendantLockType)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 针对当前锁，不考虑孩子，是否应该转为X锁
     */
    private boolean isExclusiveLock(LockType lockType) {
        return lockType.equals(LockType.X)
                || lockType.equals(LockType.SIX)
                || lockType.equals(LockType.IX);
    }
    

    private List<LockContext> getDescendants(TransactionContext transaction) {
        List<LockContext> resourceNames = new ArrayList<>();
        for (Lock lock : lockman.getLocks(transaction)) {
            LockContext lockContext = fromResourceName(lockman, lock.name);
            if (isDescendant(lockContext)) {
                resourceNames.add(lockContext);
            }
        }
        return resourceNames;
    }

    /**
     * lockContext 是 this 的子孙节点？
     */
    private boolean isDescendant(LockContext lockContext) {
        while (lockContext.parent != null) {
            if (this.equals(lockContext.parent)) {
                return true;
            }
            lockContext = lockContext.parent;
        }
        return false;
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        return lockman.getLockType(transaction, name);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        LockType explicitLockType = this.getExplicitLockType(transaction);
        LockType implicitLockType = parent != null ?
                parent.getEffectiveLockType(transaction) : LockType.NL;
        for (LockType lockType : Arrays.asList(LockType.X, LockType.SIX, LockType.S)) {
            if (implicitLockType.equals(lockType) || explicitLockType.equals(lockType)) {
                return lockType;
            }
        }
        return explicitLockType;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        return getDescendants(transaction)
                .stream()
                .filter(lockContext ->
                        lockContext.getExplicitLockType(transaction).equals(LockType.S)
                                || lockContext.getExplicitLockType(transaction).equals(LockType.IS))
                .map(LockContext::getResourceName)
                .collect(Collectors.toList());
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

