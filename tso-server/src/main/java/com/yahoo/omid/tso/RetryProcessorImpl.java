package com.yahoo.omid.tso;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.inject.Inject;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;

import org.jboss.netty.channel.Channel;

import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.CommitTable.CommitTimestamp;
import com.yahoo.omid.metrics.MetricsRegistry;

import static com.codahale.metrics.MetricRegistry.name;

import com.yahoo.omid.metrics.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the retry requests that clients can send when they did
 * not received the response in the specified timeout
 */
class RetryProcessorImpl
    implements EventHandler<RetryProcessorImpl.RetryEvent>, RetryProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(RetryProcessor.class);

    // Disruptor chain stuff
    final ReplyProcessor replyProc;
    final RingBuffer<RetryEvent> retryRing;

    final CommitTable.Client commitTableClient;
    final CommitTable.Writer writer;

    // Metrics
    final Meter retriesMeter;

    @Inject
    RetryProcessorImpl(MetricsRegistry metrics, CommitTable commitTable,
                       ReplyProcessor replyProc, Panicker panicker)
        throws InterruptedException, ExecutionException {
        this.commitTableClient = commitTable.getClient().get();
        this.writer = commitTable.getWriter().get();
        this.replyProc = replyProc;

        WaitStrategy strategy = new YieldingWaitStrategy();

        retryRing = RingBuffer.<RetryEvent>createSingleProducer(
                RetryEvent.EVENT_FACTORY, 1<<12, strategy);
        SequenceBarrier retrySequenceBarrier = retryRing.newBarrier();
        BatchEventProcessor<RetryEvent> retryProcessor = new BatchEventProcessor<RetryEvent>(
                retryRing,
                retrySequenceBarrier,
                this);
        retryProcessor.setExceptionHandler(new FatalExceptionHandler(panicker));

        retryRing.addGatingSequences(retryProcessor.getSequence());

        ExecutorService retryExec = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("retry-%d").build());
        retryExec.submit(retryProcessor);

        // Metrics
        retriesMeter = metrics.meter(name("tso", "retries"));
    }

    @Override
    public void onEvent(final RetryEvent event, final long sequence, final boolean endOfBatch)
        throws Exception {

        switch (event.getType()) {
        case COMMIT:
            // TODO: What happens when the IOException is thrown?
            handleCommitRetry(event);
            break;
        default:
            assert(false);
            break;
        }

    }

    private void handleCommitRetry(RetryEvent event) throws InterruptedException, ExecutionException {

        long startTimestamp = event.getStartTimestamp();

        try {
            Optional<CommitTimestamp> commitTimestamp = commitTableClient.getCommitTimestamp(startTimestamp).get();
            if(commitTimestamp.isPresent()) {
                if (commitTimestamp.get().isValid()) {
                    LOG.trace("Valid commit TS found in Commit Table");
                    replyProc.commitResponse(false, startTimestamp, commitTimestamp.get().getValue(),
                            event.getChannel(), event.getMonCtx());
                } else {
                    LOG.trace("Invalid commit TS found in Commit Table");
                    replyProc.abortResponse(startTimestamp, event.getChannel(), event.getMonCtx());
                }
            } else {
                LOG.trace("No commit TS found in Commit Table");
                replyProc.abortResponse(startTimestamp, event.getChannel(), event.getMonCtx());
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted reading from commit table");
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOG.error("Error reading from commit table", e);
        }

        retriesMeter.mark();
    }

    @Override
    public void disambiguateRetryRequestHeuristically(long startTimestamp, Channel c, MonitoringContext monCtx) {
        long seq = retryRing.next();
        RetryEvent e = retryRing.get(seq);
        RetryEvent.makeCommitRetry(e, startTimestamp, c, monCtx);
        retryRing.publish(seq);
    }

    public final static class RetryEvent {

        enum Type {
            COMMIT
        }
        private Type type = null;

        private long startTimestamp = 0;
        private Channel channel = null;
        private MonitoringContext monCtx;

        static void makeCommitRetry(RetryEvent e, long startTimestamp, Channel c, MonitoringContext monCtx) {
            e.monCtx = monCtx;
            e.type = Type.COMMIT;
            e.startTimestamp = startTimestamp;
            e.channel = c;
        }

        MonitoringContext getMonCtx() { return monCtx; }

        Type getType() { return type; }
        Channel getChannel() { return channel; }
        long getStartTimestamp() { return startTimestamp; }

        public final static EventFactory<RetryEvent> EVENT_FACTORY
            = new EventFactory<RetryEvent>() {
            @Override
            public RetryEvent newInstance()
            {
                return new RetryEvent();
            }
        };
    }

}