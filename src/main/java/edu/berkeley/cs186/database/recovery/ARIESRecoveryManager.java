package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.records.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given
    // transaction number.
    private Function<Long, Transaction> newTransaction;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();
    // true if redo phase of restart has terminated, false otherwise. Used
    // to prevent DPT entries from being flushed during restartRedo.
    boolean redoComplete;

    public ARIESRecoveryManager(Function<Long, Transaction> newTransaction) {
        this.newTransaction = newTransaction;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     * The master record should be added to the log, and a checkpoint should be
     * taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor
     * because of the cyclic dependency between the buffer manager and recovery
     * manager (the buffer manager must interface with the recovery manager to
     * block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and
     * redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManager(bufferManager);
    }

    // Forward Processing //////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be appended, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        TransactionTableEntry transactionTableEntry = getVaildTransactionTableEntry(transNum);

        long lastLSN = transactionTableEntry.lastLSN;

        // 1. append commit record
        CommitTransactionLogRecord commitTransactionLogRecord = new CommitTransactionLogRecord(transNum, lastLSN);
        // logManager will set LSN for record, so we don't need to set it here
        long LSN = logManager.appendToLog(commitTransactionLogRecord);

        // 2. flush log
        logManager.flushToLSN(LSN);

        // 3. update transactionTableEntry table
        transactionTableEntry.lastLSN = LSN;

        // 4. update transaction status
        Transaction transaction = transactionTableEntry.transaction;
        transaction.setStatus(Transaction.Status.COMMITTING);

        return LSN;
    }

    private TransactionTableEntry getVaildTransactionTableEntry(long transNum) {
        return Optional.of(transactionTable.get(transNum))
                .orElseThrow(() -> new RuntimeException("Transaction not found"));
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be appended, and the transaction table and
     * transaction status should be updated. Calling this function should not
     * perform any rollbacks.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        TransactionTableEntry transactionTableEntry = getVaildTransactionTableEntry(transNum);

        long lastLSN = transactionTableEntry.lastLSN;

        // 1. append abort record
        AbortTransactionLogRecord abortTransactionLogRecord = new AbortTransactionLogRecord(transNum, lastLSN);
        long LSN = logManager.appendToLog(abortTransactionLogRecord);

        // 2. update transactionTableEntry
        transactionTableEntry.lastLSN = LSN;

        // 3. update transaction status
        Transaction transaction = transactionTableEntry.transaction;
        transaction.setStatus(Transaction.Status.ABORTING);

        return LSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting (see the rollbackToLSN helper
     * function below).
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be appended,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // rollback all changes if the transaction is aborting
        TransactionTableEntry transactionTableEntry = getVaildTransactionTableEntry(transNum);
        Transaction transaction = transactionTableEntry.transaction;
        if (transaction.getStatus() != Transaction.Status.COMMITTING) {
            rollbackToLSN(transNum, 0);
        }

        transaction.cleanup();

        // remove transaction from transaction table
        transactionTable.remove(transNum);

        long lastLSN = transactionTableEntry.lastLSN;

        // append end log record
        LogRecord endTransactionLogRecord = new EndTransactionLogRecord(transNum, lastLSN);
        long LSN = logManager.appendToLog(endTransactionLogRecord);

        // update transactionTableEntry
        transactionTableEntry.lastLSN = LSN;

        // update transaction status
        transaction.setStatus(Transaction.Status.COMPLETE);

        return LSN;
    }


    /**
     * Recommended helper function: performs a rollback of all of a
     * transaction's actions, up to (but not including) a certain LSN.
     * Starting with the LSN of the most recent record that hasn't been undone:
     * - while the current LSN is greater than the LSN we're rolling back to:
     *    - if the record at the current LSN is undoable:
     *       - Get a compensation log record (CLR) by calling undo on the record
     *       - Append the CLR
     *       - Call redo on the CLR to perform the undo
     *    - update the current LSN to that of the next record to undo
     *
     * Note above that calling .undo() on a record does not perform the undo, it
     * just creates the compensation log record.
     *
     * @param transNum transaction to perform a rollback for
     * @param LSN LSN to which we should rollback
     */
    private void rollbackToLSN(long transNum, long LSN) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        long lastRecordLSN = lastRecord.getLSN();
        // Small optimization: if the last record is a CLR we can start rolling
        // back from the next record that hasn't yet been undone.
        long currentLSN = lastRecord.getUndoNextLSN().orElse(lastRecordLSN);
        while (currentLSN > LSN) {
            LogRecord logRecord = logManager.fetchLogRecord(currentLSN);
            if (logRecord.isUndoable()) {
                long lastLSN = transactionEntry.lastLSN;

                // append CLR(compensation log record)
                LogRecord undoLogRecord = logRecord.undo(lastLSN);
                long lsn = logManager.appendToLog(undoLogRecord);

                // we don't need to flush the log here because this will not lose any information

                // update transactionTableEntry
                transactionEntry.lastLSN = lsn;

                // redo the CLR to perform the undo
                undoLogRecord.redo(this, diskSpaceManager, bufferManager);
            }
            currentLSN = logRecord.getUndoNextLSN().orElse(logRecord.getPrevLSN().orElse(-1L));
        }
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        if (redoComplete) dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be appended, and the transaction table
     * and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);
        assert (before.length <= BufferManager.EFFECTIVE_PAGE_SIZE / 2);

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        long prevLSN = transactionEntry.lastLSN;

        // append log record
        LogRecord record = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, after);
        long LSN = logManager.appendToLog(record);

        // update transaction table
        transactionEntry.lastLSN = LSN;

        // update dirty page table
        dirtyPage(pageNum, LSN);

        return LSN;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long savepointLSN = transactionEntry.getSavepoint(name);

        // rollback
        rollbackToLSN(transNum, savepointLSN);
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible first
     * using recLSNs from the DPT, then status/lastLSNs from the transactions
     * table, and written when full (or when nothing is left to be written).
     * You may find the method EndCheckpointLogRecord#fitsInOneRecord here to
     * figure out when to write an end checkpoint record.
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public synchronized void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord();
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> chkptDPT = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = new HashMap<>();

        for (Map.Entry<Long, Long> entry : dirtyPageTable.entrySet()) {
            // 添加完之后，是否可以放在一个Page中
            if (!EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size() + 1, chkptTxnTable.size())) {
                // append
                LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
                logManager.appendToLog(endRecord);

                // clear
                chkptDPT.clear();
            }
            chkptDPT.put(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            if (!EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size(), chkptTxnTable.size() + 1)) {
                // append
                LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
                logManager.appendToLog(endRecord);

                chkptDPT.clear();
                chkptTxnTable.clear();
            }
            TransactionTableEntry transactionTableEntry = entry.getValue();
            Transaction transaction = transactionTableEntry.transaction;
            chkptTxnTable.put(entry.getKey(), new Pair<>(transaction.getStatus(), transactionTableEntry.lastLSN));
        }


        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
        logManager.appendToLog(endRecord);

        // Ensure checkpoint is fully flushed before updating the master record
        flushToLSN(endRecord.getLSN());

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    /**
     * Flushes the log to at least the specified record,
     * essentially flushing up to and including the page
     * that contains the record specified by the LSN.
     *
     * @param LSN LSN up to which the log should be flushed
     */
    @Override
    public void flushToLSN(long LSN) {
        this.logManager.flushToLSN(LSN);
    }

    @Override
    public void dirtyPage(long pageNum, long LSN) {
        dirtyPageTable.putIfAbsent(pageNum, LSN);
        // Handle race condition where earlier log is beaten to the insertion by
        // a later log.
        dirtyPageTable.computeIfPresent(pageNum, (k, v) -> Math.min(LSN,v));
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery ////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery.
     * Recovery is complete when the Runnable returned is run to termination.
     * New transactions may be started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the
     * dirty page table of non-dirty pages (pages that aren't dirty in the
     * buffer manager) between redo and undo, and perform a checkpoint after
     * undo.
     */
    @Override
    public void restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.redoComplete = true;
        this.cleanDPT();
        this.restartUndo();
        this.checkpoint();
    }

    /**
     * 1. 读取 Master Record 上次成功的 checkPoint 的 Last Sequence Number
     * 2.
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the beginning of the
     * last successful checkpoint.
     *
     * If the log record is for a transaction operation (getTransNum is present)
     * - update the transaction table
     *
     * If the log record is page-related (getPageNum is present), update the dpt
     *   - update/undoupdate page will dirty pages
     *   - free/undoalloc page always flush changes to disk
     *   - no action needed for alloc/undofree page
     *
     * If the log record is for a change in transaction status:
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     * - if END_TRANSACTION: clean up transaction (Transaction#cleanup), remove
     *   from txn table, and add to endedTransactions
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Skip txn table entries for transactions that have already ended
     * - Add to transaction table if not already present
     * - Update lastLSN to be the larger of the existing entry's (if any) and
     *   the checkpoint's
     * - The status's in the transaction table should be updated if it is possible
     *   to transition from the status in the table to the status in the
     *   checkpoint. For example, running -> aborting is a possible transition,
     *   but aborting -> running is not.
     *
     * After all records in the log are processed, for each ttable entry:
     *  - if COMMITTING: clean up the transaction, change status to COMPLETE,
     *    remove from the ttable, and append an end record
     *  - if RUNNING: change status to RECOVERY_ABORTING, and append an abort
     *    record
     *  - if RECOVERY_ABORTING: no action needed
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        // Type checking
        assert (record != null && record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;
        // Set of transactions that have completed
        Set<Long> endedTransactions = new HashSet<>();
        // TODO(proj5): implement
        Iterator<LogRecord> iterator = logManager.scanFrom(LSN);
        while (iterator.hasNext()) {
            // 挨个遍历从 LSN 开始的所有Log
            LogRecord logRecord = iterator.next();
            // 如果这是一次事务操作
            if (logRecord.getTransNum().isPresent()) {
                Long transNum = logRecord.getTransNum().get();
                // 如果是一个新事务，就将其添加到事务表里
                if (transactionTable.get(transNum) == null) {
                    Transaction transaction = newTransaction.apply(transNum);
                    startTransaction(transaction);
                }
                // 更新事务的LSN
                TransactionTableEntry tableEntry = transactionTable.get(transNum);
                tableEntry.lastLSN = logRecord.getLSN();
            }
            // 如果是数据页操作
            if (logRecord.getPageNum().isPresent()) {
                LogType type = logRecord.getType();
                // 对于更新涉及写入的更新操作，将该页加入脏页表中
                if (type.equals(LogType.UPDATE_PAGE) || type.equals(LogType.UNDO_UPDATE_PAGE)) {
                    dirtyPage(logRecord.getPageNum().get(), logRecord.getLSN());
                }
                // 对于内存管理，先将更改写入磁盘，再从脏页表中移除
                if (type.equals(LogType.FREE_PAGE) || type.equals(LogType.UNDO_ALLOC_PAGE)) {
                    Page page = bufferManager.fetchPage(new DummyLockContext(""), logRecord.getPageNum().get());
                    page.flush();
                    dirtyPageTable.remove(logRecord.getPageNum().get());
                }
                // 其余操作不需要管理
            }

            if (logRecord.getType().equals(LogType.COMMIT_TRANSACTION)) {
                Long transNum = logRecord.getTransNum().get();
                TransactionTableEntry tableEntry = transactionTable.get(transNum);
                tableEntry.transaction.setStatus(Transaction.Status.COMMITTING);
            }

            if (logRecord.getType().equals(LogType.ABORT_TRANSACTION)) {
                Long transNum = logRecord.getTransNum().get();
                TransactionTableEntry tableEntry = transactionTable.get(transNum);
                tableEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
            }

            // 如果是结束事务的日志，则需要先清除事务，从事务表移除，添加到endedTransactions,最后修改状态
            if (logRecord.getType().equals(LogType.END_TRANSACTION)) {
                Long transNum = logRecord.getTransNum().get();
                TransactionTableEntry tableEntry = transactionTable.get(transNum);
                tableEntry.transaction.cleanup();
                transactionTable.remove(transNum);
                endedTransactions.add(transNum);
                tableEntry.transaction.setStatus(Transaction.Status.COMPLETE);
            }

            if (logRecord.getType().equals(LogType.END_CHECKPOINT)) {
                // 对于检查点脏页表的数据，将其全部加入
                Map<Long, Long> dpt = logRecord.getDirtyPageTable();
                dirtyPageTable.putAll(dpt);
                Map<Long, Pair<Transaction.Status, Long>> tTable = logRecord.getTransactionTable();
                // 如果检查点的事务表中有事务不在恢复事务表中，就将其添加
                // 更新事务表的LSN
                for (Long transNum : tTable.keySet()) {
                    if (endedTransactions.contains(transNum)) continue;
                    if (!transactionTable.containsKey(transNum)) {
                        startTransaction(newTransaction.apply(transNum));
                    }
                    Pair<Transaction.Status, Long> pair = tTable.get(transNum);
                    TransactionTableEntry tableEntry = transactionTable.get(transNum);
                    Long lsn = pair.getSecond();
                    if (lsn >= tableEntry.lastLSN) {
                        tableEntry.lastLSN = lsn;
                    }
                    // 如果存档点中的事务状态领先，则要进行修改
                    if (judgeAdvance(pair.getFirst(), tableEntry.transaction.getStatus())) {
                        // 如果是aborting则需要修改为recovery_abort
                        if (pair.getFirst().equals(Transaction.Status.ABORTING)) {
                            tableEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                        } else {
                            tableEntry.transaction.setStatus(pair.getFirst());
                        }
                    }
                }
            }
        }
        for (Long transNum : transactionTable.keySet()) {
            TransactionTableEntry tableEntry = transactionTable.get(transNum);
            Transaction transaction = tableEntry.transaction;
            // 这里cleanup必须在end前调用，因为end会修改状态，导致cleanup报错
            if (transaction.getStatus().equals(Transaction.Status.COMMITTING)) {
                transaction.cleanup();
                end(transNum);
            }
            // 这里要注意以下顺序，abort()会将事务修改为abort状态，但我们这时候需要的是RECOVERY_ABORTING
            // 因此abort方法要在前面。不过日志也理应在事务操作之前添加
            if (transaction.getStatus().equals(Transaction.Status.RUNNING)) {
                abort(transNum);
                transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
            }

        }
        return;
    }

    /**
     * 判断状态1是否比状态2领先
     */
    private boolean judgeAdvance(Transaction.Status type1, Transaction.Status type2) {
        if (type1.equals(Transaction.Status.RUNNING)) return false;
        if (type1.equals(Transaction.Status.ABORTING) || type1.equals(Transaction.Status.COMMITTING)) {
            if (type2.equals(Transaction.Status.RUNNING)) return true;
            else return false;
        }
        if (type1.equals(Transaction.Status.COMPLETE)) return true;
        return true;
    }

//    private void processRemainingTransactions() {
//        // After all records in the log are processed, for each ttable entry:
//        //  - if COMMITTING: clean up the transaction, change status to COMPLETE,
//        //    remove from the ttable, and append an end record
//        //  - if RUNNING: change status to RECOVERY_ABORTING, and append an abort
//        //    record
//        //  - if RECOVERY_ABORTING: no action needed
//        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
//            TransactionTableEntry transactionTableEntry = entry.getValue();
//            Transaction transaction = transactionTableEntry.transaction;
//            switch (transaction.getStatus()) {
//                case COMMITTING:
//                    // clean up the transaction, change status to COMPLETE, remove from the ttable, and append an end record
//                    transaction.cleanup();
//                    transaction.setStatus(Transaction.Status.COMPLETE);
//                    transactionTable.remove(entry.getKey());
//                    EndTransactionLogRecord endTransactionLogRecord = new EndTransactionLogRecord(entry.getKey(), transactionTableEntry.lastLSN);
//                    logManager.appendToLog(endTransactionLogRecord);
//                    break;
//                case RUNNING:
//                    // change status to RECOVERY_ABORTING, and append an abort record
//                    transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
//                    AbortTransactionLogRecord abortTransactionLogRecord = new AbortTransactionLogRecord(entry.getKey(), transactionTableEntry.lastLSN);
//                    logManager.appendToLog(abortTransactionLogRecord);
//                    break;
//                case RECOVERY_ABORTING:
//                    // no action needed
//                    break;
//                default:
//                    break;
//            }
//        }
//    }
//
//    private void processLogRecords(long LSN, Set<Long> endedTransactions) {
//        Iterator<LogRecord> logRecordIterator = logManager.scanFrom(LSN);
//        while (logRecordIterator.hasNext()) {
//            LogRecord logRecord = logRecordIterator.next();
//
//            // 处理Transaction相关的LogRecord
//            processTransactionLogRecord(logRecord);
//
//            // 处理Page相关的LogRecord
//            processPageLogRecord(logRecord);
//
//            processSpecialLogRecords(logRecord, endedTransactions);
//        }
//    }
//
//    private void processSpecialLogRecords(LogRecord logRecord, Set<Long> endedTransactions) {
//        switch (logRecord.getType()) {
//            case COMMIT_TRANSACTION:
//                // update transaction status to COMMITTING
//                updateTransactionStatus(logRecord, Transaction.Status.COMMITTING);
//                break;
//            case ABORT_TRANSACTION:
//                // update transaction status to RECOVERY_ABORTING
//                updateTransactionStatus(logRecord, Transaction.Status.RECOVERY_ABORTING);
//                break;
//            case END_TRANSACTION:
//                endTransaction(logRecord, endedTransactions);
//                break;
//            case END_CHECKPOINT:
//                processEndCheckpointLogRecord((EndCheckpointLogRecord) logRecord, endedTransactions);
//                break;
//            default:
//                break;
//        }
//
//    }

//
//    private void processEndCheckpointLogRecord(EndCheckpointLogRecord endCheckpointLogRecord, Set<Long> endedTransactions) {
//        // endCheckpointLogRecord包含dirtyPageTable和transactionTable两个Map
//
//        // 将endCheckpointLogRecord中的dirtyPageTable拷贝到dirtyPageTable中
//        Map<Long, Long> chkptDPT = endCheckpointLogRecord.getDirtyPageTable();
//        dirtyPageTable.putAll(chkptDPT);
//
//        // 将endCheckpointLogRecord中的transactionTable拷贝到transactionTable中
//        Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = endCheckpointLogRecord.getTransactionTable();
//        for (Map.Entry<Long, Pair<Transaction.Status, Long>> entry : chkptTxnTable.entrySet()) {
//            // 跳过已经结束的transaction
//            if (endedTransactions.contains(entry.getKey())) {
//                continue;
//            }
//
//            // 如果transactionTable中没有该transaction，那么创建一个
//            TransactionTableEntry transactionTableEntry = transactionTable.get(entry.getKey());
//            if (transactionTableEntry == null) {
//                transactionTableEntry = new TransactionTableEntry(newTransaction.apply(entry.getKey()));
//                transactionTable.put(entry.getKey(), transactionTableEntry);
//            }
//
//            // 更新lastLSN
//            transactionTableEntry.lastLSN = Math.max(transactionTableEntry.lastLSN, entry.getValue().getSecond());
//
//            // 如果可以从transactionTable中的状态转换到checkpoint中的状态，那么更新状态
//            if (transactionTableEntry.transaction.getStatus().canTransitionTo(entry.getValue().getFirst())) {
//                transactionTableEntry.transaction.setStatus(entry.getValue().getFirst());
//            }
//        }
//    }
//
//    private void endTransaction(LogRecord logRecord, Set<Long> endedTransactions) {
//        // clean up transaction (Transaction#cleanup), remove from txn table, and add to endedTransactions
//        TransactionTableEntry transactionTableEntry = transactionTable.get(logRecord.getTransNum().get());
//        transactionTableEntry.transaction.cleanup();
//
//        transactionTable.remove(logRecord.getTransNum().get());
//
//        endedTransactions.add(logRecord.getTransNum().get());
//    }
//
//    private void updateTransactionStatus(LogRecord logRecord, Transaction.Status status) {
//        TransactionTableEntry transactionTableEntry = transactionTable
//                .get(logRecord.getTransNum().orElseThrow(
//                        () -> new RuntimeException("Transaction not found")));
//        transactionTableEntry.transaction.setStatus(status);
//    }
//
//    /**
//     * PageNum存在，page相关的log record
//     */
//    private void processPageLogRecord(LogRecord logRecord) {
//        logRecord.getPageNum().ifPresent(pageNum -> {
//            switch (logRecord.getType()) {
//                case UPDATE_PAGE:
//                case UNDO_UPDATE_PAGE:
//                    dirtyPage(pageNum, logRecord.getLSN());
//                    break;
//                case FREE_PAGE:
//                case UNDO_ALLOC_PAGE:
//                    // free/undoalloc page always flush changes to disk
//                    flushToLSN(logRecord.getLSN());
//                    break;
//                default:
//                    break;
//            }
//        });
//    }
//
//    /**
//     * 如果logRecord存在TransactionNum，那么为其创建TransactionTableEntry
//     */
//    private void processTransactionLogRecord(LogRecord logRecord) {
//        logRecord.getTransNum().ifPresent(transNum -> {
//            // TransactionNum存在，transaction相关的log record
//            transactionTable.computeIfAbsent(transNum,
//                    k -> new TransactionTableEntry(newTransaction.apply(transNum)));
//        });
//    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the dirty page table.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - partition-related (Alloc/Free/UndoAlloc/UndoFree..Part), always redo it
     * - allocates a page (AllocPage/UndoFreePage), always redo it
     * - modifies a page (Update/UndoUpdate/Free/UndoAlloc....Page) in
     *   the dirty page table with LSN >= recLSN, the page is fetched from disk,
     *   the pageLSN is checked, and the record is redone if needed.
     */
    void restartRedo() {
        // TODO(proj5): implement
        return;
    }

    /**
     * This method performs the undo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting
     * transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, and append the appropriate CLR
     * - replace the entry with a new one, using the undoNextLSN if available,
     *   if the prevLSN otherwise.
     * - if the new LSN is 0, clean up the transaction, set the status to complete,
     *   and remove from transaction table.
     */
    void restartUndo() {
        // TODO(proj5): implement
        return;
    }

    /**
     * Removes pages from the DPT that are not dirty in the buffer manager.
     * This is slow and should only be used during recovery.
     */
    void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) dirtyPages.add(pageNum);
        });
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

    // Helpers /////////////////////////////////////////////////////////////////
    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A),
     * in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
