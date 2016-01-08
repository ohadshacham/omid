package com.yahoo.omid.tsoclient;

import com.google.common.util.concurrent.SettableFuture;
import com.yahoo.omid.committable.CommitTable;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class MockTSOClient extends TSOClient {
    private final AtomicLong timestampGenerator = new AtomicLong();
    private final int CONFLICT_MAP_SIZE = 1 * 1000 * 1000;
    private final long[] conflictMap = new long[CONFLICT_MAP_SIZE];
    private final AtomicLong lwm = new AtomicLong();

    private final CommitTable.Writer commitTable;

    public MockTSOClient(CommitTable.Writer commitTable) {
        this.commitTable = commitTable;
    }

    @Override
    public TSOFuture<Long> getNewStartTimestamp() {
        synchronized (conflictMap) {
            SettableFuture<Long> f = SettableFuture.<Long>create();
            f.set(timestampGenerator.incrementAndGet());
            return new ForwardingTSOFuture<Long>(f);
        }
    }

    @Override
    public TSOFuture<Long> commit(long transactionId, Set<? extends CellId> cells) {
        synchronized (conflictMap) {
            SettableFuture<Long> f = SettableFuture.<Long>create();
            if (transactionId < lwm.get()) {
                f.setException(new AbortException());
                return new ForwardingTSOFuture<Long>(f);
            }

            boolean canCommit = true;
            for (CellId c : cells) {
                int index = Math.abs((int) (c.getCellId() % CONFLICT_MAP_SIZE));
                if (conflictMap[index] >= transactionId) {
                    canCommit = false;
                    break;
                }
            }

            if (canCommit) {
                long commitTimestamp = timestampGenerator.incrementAndGet();
                for (CellId c : cells) {
                    int index = Math.abs((int) (c.getCellId() % CONFLICT_MAP_SIZE));
                    long oldVal = conflictMap[index];
                    conflictMap[index] = commitTimestamp;
                    long curLwm = lwm.get();
                    while (oldVal > curLwm) {
                        if (lwm.compareAndSet(curLwm, oldVal)) {
                            break;
                        }
                        curLwm = lwm.get();
                    }
                }

                f.set(commitTimestamp);
                try {
                    commitTable.addCommittedTransaction(transactionId, commitTimestamp);
                    commitTable.updateLowWatermark(lwm.get());
                    commitTable.flush();
                } catch (IOException ioe) {
                    f.setException(ioe);
                }
            } else {
                f.setException(new AbortException());
            }
            return new ForwardingTSOFuture<Long>(f);
        }
    }

    @Override
    public TSOFuture<Void> close() {
        SettableFuture<Void> f = SettableFuture.<Void>create();
        f.set(null);
        return new ForwardingTSOFuture<Void>(f);
    }
}