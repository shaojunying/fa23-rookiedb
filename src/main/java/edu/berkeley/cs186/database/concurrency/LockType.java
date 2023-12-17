package edu.berkeley.cs186.database.concurrency;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    private static final boolean[][] compatibilityMatrix = new boolean[][] {
            // S,    X,     IS,   IX,    SIX,   NL
            { true, false, true, false, false, true}, // S
            { false, false, false, false, false, true}, // X
            { true, false, true, true, true, true}, // IS
            { false, false, true, true, false, true}, // IX
            { false, false, true, false, false, true}, // SIX
            { true, true, true, true, true, true} // NL
    };

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        return compatibilityMatrix[a.ordinal()][b.ordinal()];
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    private static final boolean[][] parentLockMatrix = new boolean[][] {
            // S,    X,     IS,   IX,    SIX,   NL
            { true, false, true, false, false, true}, // S
            { true, true, true, true, true, true}, // X
            { true, false, true, false, false, true}, // IS
            { true, true, true, true, true, true}, // IX
            { true, true, true, true, true, true}, // SIX
            { false, false, false, false, false, true} // NL
    };

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        return parentLockMatrix[parentLockType.ordinal()][childLockType.ordinal()];
    }

    private static final boolean[][] substitutableMatrix = new boolean[][] {
            // S,    X,    IS,    IX,    SIX,  NL 替换前
            { true, false, true, false, false, true}, // S 替换后
            { true, true, true, true, true, true}, // X
            { false, false, true, false, false, true}, // IS
            { false, false, true, true, false, true}, // IX
            { true, false, true, true, true, true}, // SIX
            { false, false, false, false, false, true} // NL
    };

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     * substitute: 替换后
     * required: 替换前
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        return substitutableMatrix[substitute.ordinal()][required.ordinal()];
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

