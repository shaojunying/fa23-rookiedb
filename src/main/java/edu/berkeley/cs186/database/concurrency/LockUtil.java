package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.Arrays;
import java.util.List;
import java.util.Stack;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        //
        if (LockType.substitutable(effectiveLockType, requestType)) {
            return;
        }
//
//        if (explicitLockType.equals(LockType.IS)) {
//            if (requestType.equals(LockType.S)) {
//                lockContext.escalate(transaction);
//            }else {
//                // requestType == X
//                LockContext context = parentContext;
//                Stack<LockContext> stack = new Stack<>();
//                while (context != null) {
//                    stack.push(context);
//                    context = context.parent;
//                }
//                while (!stack.isEmpty()) {
//                    stack.pop().promote(transaction, LockType.IX);
//                }
//
//
//                lockContext.promote(transaction, requestType);
//
//                freeDescendantsLocks(lockContext, transaction,
//                        Arrays.asList(LockType.S, LockType.IS, LockType.IX,
//                                LockType.SIX, LockType.X));
//
//            }
//        }else if (explicitLockType.equals(LockType.IX)) {
//            if (requestType.equals(LockType.S)) {
//                lockContext.promote(transaction, LockType.SIX);
//                freeDescendantsLocks(lockContext, transaction,
//                        Arrays.asList(LockType.S, LockType.IS, LockType.SIX));
//            }else {
//                // X
//                lockContext.escalate(transaction);
//            }
//        }else if (explicitLockType.equals(LockType.SIX)) {
//            if (requestType.equals(LockType.X)) {
//                lockContext.promote(transaction, LockType.X);
//                freeDescendantsLocks(lockContext, transaction,
//                        Arrays.asList(LockType.X, LockType.IX));
//            }
//        }else if (explicitLockType.equals(LockType.S)
//                || explicitLockType.equals(LockType.NL) && effectiveLockType.equals(LockType.S)) {
//            if (requestType.equals(LockType.X)) {
//                LockContext context = parentContext;
//                Stack<LockContext> stack = new Stack<>();
//                while (context != null) {
//                    stack.push(context);
//                    context = context.parent;
//                }
//                while (!stack.isEmpty()) {
//                    stack.pop().promote(transaction, LockType.IX);
//                }
//                lockContext.promote(transaction, LockType.X);
//            }
//        }else if (effectiveLockType.equals(LockType.NL)) {
//            if (requestType.equals(LockType.S)) {
//                LockContext context = parentContext;
//                Stack<LockContext> stack = new Stack<>();
//                while (context != null) {
//                    stack.push(context);
//                    context = context.parent;
//                }
//                while (!stack.isEmpty()) {
//                    stack.pop().acquire(transaction, LockType.IS);
//                }
//                lockContext.acquire(transaction, LockType.S);
//            }else {
//                // X
//                LockContext context = parentContext;
//                Stack<LockContext> stack = new Stack<>();
//                while (context != null) {
//                    stack.push(context);
//                    context = context.parent;
//                }
//                while (!stack.isEmpty()) {
//                    stack.pop().acquire(transaction, LockType.IX);
//                }
//                lockContext.acquire(transaction, LockType.X);
//            }
//        }

        // 特殊处理SIX的情况
        if (explicitLockType.equals(LockType.IX) && requestType.equals(LockType.S)) {
            lockContext.promote(transaction, LockType.SIX);
            return;
        }

        if (explicitLockType.isIntent()) {
            lockContext.escalate(transaction);
            explicitLockType = lockContext.getExplicitLockType(transaction);
            if (explicitLockType == LockType.X || explicitLockType == requestType) return;
        }

        // 这时只剩(S, X), (NL, S), (NL, X)的情况
        if (requestType.equals(LockType.S)) {
            enforceLock(transaction, parentContext, LockType.IS);
        }else {
            enforceLock(transaction, parentContext, LockType.IX);
        }
        if (explicitLockType.equals(LockType.NL)) {
            lockContext.acquire(transaction, requestType);
        }else {
            lockContext.promote(transaction, requestType);
        }
    }

    private static void enforceLock(TransactionContext transaction, LockContext context,
                                    LockType lockType) {
        if (context == null) return;
        enforceLock(transaction, context.parent, lockType);
        LockType explicitLockType = context.getExplicitLockType(transaction);
//        if (explicitLockType.equals(LockType.S) && lockType.equals(LockType.IX)) {
//            context.promote(transaction, LockType.SIX);
//        }else if (explicitLockType.equals(LockType.NL)) {
//            context.acquire(transaction, lockType);
//        }else {
//            context.promote(transaction, lockType);
//        }
        // 注意为什么需要这个检查
        if (!LockType.substitutable(explicitLockType, lockType)) {
            if (explicitLockType == LockType.NL) {
                context.acquire(transaction, lockType);
            }else if (explicitLockType == LockType.S && lockType == LockType.IX){
                context.promote(transaction, LockType.SIX);
            }else {
                context.promote(transaction, lockType);
            }
        }
    }

    /**
     * 释放后代中，lock type为 lockTypeListToBeFreed 的锁
     */
    private static void freeDescendantsLocks(LockContext lockContext,
                                             TransactionContext transaction,
                                             List<LockType> lockTypeListToBeFreed) {
        List<LockContext> lockContexts = lockContext.getDescendants(transaction);
        for (LockContext context : lockContexts) {
            if (lockTypeListToBeFreed.contains(context.getExplicitLockType(transaction))) {
                context.release(transaction);
            }
        }
    }
}
