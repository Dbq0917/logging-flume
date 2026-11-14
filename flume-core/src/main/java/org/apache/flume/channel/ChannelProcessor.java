/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.channel;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.LogPrivacyUtil;
import org.apache.flume.conf.TransactionCapacitySupported;
import org.apache.flume.event.Event;
import org.apache.flume.exception.ChannelException;
import org.apache.flume.exception.ChannelFullException;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.InterceptorBuilderFactory;
import org.apache.flume.interceptor.InterceptorChain;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A channel processor exposes operations to put {@link Event}s into
 * {@link Channel}s. These operations will propagate a {@link ChannelException}
 * if any errors occur while attempting to write to {@code required} channels.
 * <p>
 * Each channel processor instance is configured with a {@link ChannelSelector}
 * instance that specifies which channels are
 * {@linkplain ChannelSelector#getRequiredChannels(Event) required} and which
 * channels are
 * {@linkplain ChannelSelector#getOptionalChannels(Event) optional}.
 */
public class ChannelProcessor implements Configurable {

    private static final Logger logger = LogManager.getLogger();

    private static final long INITIAL_RETRY_BACKOFF_MS = 10L;
    private static final long MAX_RETRY_BACKOFF_MS = 100L;
    private static final long MIN_CHANNEL_BLOCK_MILLIS = 10L;
    private static final long CHANNEL_FULL_LOG_INTERVAL_MILLIS = 30000L;
    private static final int MAX_LOGGED_EVENTS = 100;
    private static final int MAX_RAW_EVENT_CHARS = 256;
    private static final String[] EVENT_ID_HEADERS = {
        "id", "eventId", "eventID", "messageId", "messageID", "msgId", "msgID", "uuid", "correlationId"
    };

    private final ChannelSelector selector;
    private final InterceptorChain interceptorChain;
    private final AtomicLong lastChannelFullLogMillis = new AtomicLong(Long.MIN_VALUE / 2);

    public ChannelProcessor(ChannelSelector selector) {
        this(selector, null);
    }

    public ChannelProcessor(ChannelSelector selector, List<Interceptor> interceptors) {
        this.selector = selector;
        this.interceptorChain = new InterceptorChain();
        if (interceptors != null) {
            interceptorChain.setInterceptors(interceptors);
        }
    }

    public void initialize() {
        interceptorChain.initialize();
    }

    public void close() {
        interceptorChain.close();
    }

    /**
     * The Context of the associated Source is passed.
     *
     * @param context
     */
    @Override
    public void configure(Context context) {
        configureInterceptors(context);
    }

    // WARNING: throws FlumeException (is that ok?)
    private void configureInterceptors(Context context) {

        List<Interceptor> interceptors = Lists.newLinkedList();

        String interceptorListStr = context.getString("interceptors", "");
        if (interceptorListStr.isEmpty()) {
            return;
        }
        String[] interceptorNames = interceptorListStr.split("\\s+");

        Context interceptorContexts = new Context(context.getSubProperties("interceptors."));

        // run through and instantiate all the interceptors specified in the Context
        InterceptorBuilderFactory factory = new InterceptorBuilderFactory();
        for (String interceptorName : interceptorNames) {
            Context interceptorContext = new Context(interceptorContexts.getSubProperties(interceptorName + "."));
            String type = interceptorContext.getString("type");
            if (type == null) {
                logger.error("Type not specified for interceptor " + interceptorName);
                throw new FlumeException("Interceptor.Type not specified for " + interceptorName);
            }
            try {
                Interceptor.Builder builder = factory.newInstance(type);
                builder.configure(interceptorContext);
                interceptors.add(builder.build());
            } catch (ClassNotFoundException e) {
                logger.error("Builder class not found. Exception follows.", e);
                throw new FlumeException("Interceptor.Builder not found.", e);
            } catch (InstantiationException e) {
                logger.error("Could not instantiate Builder. Exception follows.", e);
                throw new FlumeException("Interceptor.Builder not constructable.", e);
            } catch (IllegalAccessException e) {
                logger.error("Unable to access Builder. Exception follows.", e);
                throw new FlumeException("Unable to access Interceptor.Builder.", e);
            }
        }

        interceptorChain.setInterceptors(interceptors);
    }

    public ChannelSelector getSelector() {
        return selector;
    }

    /**
     * Attempts to {@linkplain Channel#put(Event) put} the given events into each
     * configured channel. If any {@code required} channel throws a
     * {@link ChannelException}, that exception will be propagated.
     * <p>
     * A {@link ChannelFullException} is treated as backpressure rather than as
     * a terminal error: the unwritten suffix of the batch is retried, using
     * progressively smaller transactions, until the channel accepts it. This
     * keeps all events and preserves the original order.
     * <p>
     * <p>Note that if multiple channels are configured, some {@link Transaction}s
     * may have already been committed while others may be rolled back in the
     * case of an exception.
     *
     * @param events A list of events to put into the configured channels.
     * @throws ChannelException when a write to a required channel fails.
     */
    public void processEventBatch(List<Event> events) {
        Preconditions.checkNotNull(events, "Event list must not be null");

        events = interceptorChain.intercept(events);

        Map<Channel, List<Event>> reqChannelQueue = new LinkedHashMap<Channel, List<Event>>();

        Map<Channel, List<Event>> optChannelQueue = new LinkedHashMap<Channel, List<Event>>();

        for (Event event : events) {
            List<Channel> reqChannels = selector.getRequiredChannels(event);

            for (Channel ch : reqChannels) {
                List<Event> eventQueue = reqChannelQueue.get(ch);
                if (eventQueue == null) {
                    eventQueue = new ArrayList<Event>();
                    reqChannelQueue.put(ch, eventQueue);
                }
                eventQueue.add(event);
            }

            List<Channel> optChannels = selector.getOptionalChannels(event);

            for (Channel ch : optChannels) {
                List<Event> eventQueue = optChannelQueue.get(ch);
                if (eventQueue == null) {
                    eventQueue = new ArrayList<Event>();
                    optChannelQueue.put(ch, eventQueue);
                }

                eventQueue.add(event);
            }
        }

        // Process required channels
        for (Channel reqChannel : reqChannelQueue.keySet()) {
            putLossless(reqChannel, reqChannelQueue.get(reqChannel), "batch", true);
        }

        // Process optional channels
        for (Channel optChannel : optChannelQueue.keySet()) {
            try {
                putLossless(optChannel, optChannelQueue.get(optChannel), "batch", false);
            } catch (Throwable t) {
                logger.error("Unable to put batch on optional channel: " + optChannel, t);
                if (t instanceof Error) {
                    throw (Error) t;
                }
            }
        }
    }

    /**
     * Attempts to {@linkplain Channel#put(Event) put} the given event into each
     * configured channel. If any {@code required} channel throws a
     * {@link ChannelException}, that exception will be propagated.
     * <p>
     * A {@link ChannelFullException} is treated as backpressure and the event
     * is retried until the channel accepts it.
     * <p>
     * <p>Note that if multiple channels are configured, some {@link Transaction}s
     * may have already been committed while others may be rolled back in the
     * case of an exception.
     *
     * @param event The event to put into the configured channels.
     * @throws ChannelException when a write to a required channel fails.
     */
    public void processEvent(Event event) {

        event = interceptorChain.intercept(event);
        if (event == null) {
            return;
        }

        // Process required channels
        List<Channel> requiredChannels = selector.getRequiredChannels(event);
        for (Channel reqChannel : requiredChannels) {
            putLossless(reqChannel, Collections.singletonList(event), "event", true);
        }

        // Process optional channels
        List<Channel> optionalChannels = selector.getOptionalChannels(event);
        for (Channel optChannel : optionalChannels) {
            try {
                putLossless(optChannel, Collections.singletonList(event), "event", false);
            } catch (Throwable t) {
                logger.error("Unable to put event on optional channel: " + optChannel, t);
                if (t instanceof Error) {
                    throw (Error) t;
                }
            }
        }
    }

    /**
     * Writes events with backpressure. A failing transaction is rolled back and
     * only its unwritten suffix is retried, so a full channel cannot cause the
     * events held by the caller to be discarded.
     */
    private void putLossless(Channel channel, List<Event> events, String component, boolean required) {
        int offset = 0;
        int maxBatchSize = maxBatchSize(channel, events.size());
        int batchSize = maxBatchSize;
        int retries = 0;
        int totalRetries = 0;
        long retryBackoffMillis = INITIAL_RETRY_BACKOFF_MS;
        long backpressureStartNanos = 0L;

        while (offset < events.size()) {
            int end = Math.min(events.size(), offset + batchSize);
            List<Event> batch = events.subList(offset, end);
            long attemptStartNanos = System.nanoTime();

            try {
                putTransaction(channel, batch, component, required);
                offset = end;
                retries = 0;
                retryBackoffMillis = INITIAL_RETRY_BACKOFF_MS;
                if (batchSize < maxBatchSize) {
                    batchSize = (int) Math.min((long) maxBatchSize, (long) batchSize * 2L);
                }
            } catch (ChannelFullException ex) {
                retries++;
                totalRetries++;
                if (backpressureStartNanos == 0L) {
                    backpressureStartNanos = attemptStartNanos;
                }
                logBackpressure(channel, events, offset, end, retries, ex);

                batchSize = Math.max(1, batchSize / 2);
                long attemptMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - attemptStartNanos);
                if (attemptMillis < MIN_CHANNEL_BLOCK_MILLIS) {
                    try {
                        Thread.sleep(retryBackoffMillis);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        logTerminalFailure(channel, events, offset, end, ie);
                        throw new ChannelException("Interrupted while applying backpressure to " + channel, ie);
                    }
                    retryBackoffMillis = Math.min(MAX_RETRY_BACKOFF_MS, retryBackoffMillis * 2L);
                }
            } catch (Throwable t) {
                logTerminalFailure(channel, events, offset, end, t);
                throw t;
            }

            if (offset == events.size() && totalRetries > 0) {
                long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - backpressureStartNanos);
                if (elapsedMillis >= MIN_CHANNEL_BLOCK_MILLIS) {
                    logger.info(
                            "Channel {} recovered after backpressure: accepted all {} events in {} retries over {} ms",
                            channel,
                            events.size(),
                            totalRetries,
                            elapsedMillis);
                } else {
                    logger.debug(
                            "Channel {} recovered after {} backpressure retries; all {} events accepted",
                            channel,
                            totalRetries,
                            events.size());
                }
            }
        }
    }

    private void putTransaction(Channel channel, List<Event> events, String component, boolean required) {
        Transaction tx = channel.getTransaction();
        Preconditions.checkNotNull(tx, "Transaction object must not be null");
        boolean begun = false;
        try {
            tx.begin();
            begun = true;
            for (Event event : events) {
                channel.put(event);
            }
            tx.commit();
        } catch (Throwable t) {
            if (begun) {
                Throwable rollbackFailure = null;
                try {
                    tx.rollback();
                } catch (Throwable rollbackException) {
                    rollbackFailure = rollbackException;
                    t.addSuppressed(rollbackException);
                }
                if (rollbackFailure != null) {
                    throw new ChannelException("Unable to roll back transaction on " + channel, t);
                }
            }
            if (t instanceof Error) {
                throw (Error) t;
            }
            if (t instanceof ChannelException) {
                throw (ChannelException) t;
            }
            throw new ChannelException(
                    "Unable to put " + component + " on " + (required ? "required" : "optional") + " channel: "
                            + channel,
                    t);
        } finally {
            tx.close();
        }
    }

    private int maxBatchSize(Channel channel, int eventCount) {
        if (channel instanceof TransactionCapacitySupported) {
            long transactionCapacity = ((TransactionCapacitySupported) channel).getTransactionCapacity();
            if (transactionCapacity > 0 && transactionCapacity < eventCount) {
                return (int) Math.min(Integer.MAX_VALUE, transactionCapacity);
            }
        }
        return eventCount;
    }

    private void logBackpressure(
            Channel channel, List<Event> events, int offset, int end, int retries, ChannelFullException cause) {
        long now = System.currentTimeMillis();
        long lastLog = lastChannelFullLogMillis.get();
        if (now - lastLog >= CHANNEL_FULL_LOG_INTERVAL_MILLIS && lastChannelFullLogMillis.compareAndSet(lastLog, now)) {
            logger.warn(
                    "Channel {} is full; applying backpressure to events {}..{} of {}. "
                            + "No events will be dropped. retries={}, event identities={}",
                    channel,
                    offset,
                    end - 1,
                    events.size(),
                    retries,
                    describeEvents(events, offset, end),
                    cause);
        } else {
            logger.debug(
                    "Channel {} is still full; retrying events {}..{} of {}. No events will be dropped. retries={}",
                    channel,
                    offset,
                    end - 1,
                    events.size(),
                    retries);
        }
    }

    private void logTerminalFailure(Channel channel, List<Event> events, int offset, int end, Throwable cause) {
        logger.error(
                "Channel {} rejected events {}..{} of {}. These events were not committed: {}",
                channel,
                offset,
                end - 1,
                events.size(),
                describeEvents(events, offset, end),
                cause);
    }

    private String describeEvents(List<Event> events, int offset, int end) {
        StringBuilder result = new StringBuilder();
        int eventCount = end - offset;
        int logCount = Math.min(eventCount, MAX_LOGGED_EVENTS);

        for (int i = 0; i < logCount; i++) {
            if (i > 0) {
                result.append(", ");
            }
            result.append(offset + i).append(':').append(describeEvent(events.get(offset + i)));
        }
        if (eventCount > logCount) {
            result.append(", ... (").append(eventCount - logCount).append(" more)");
        }
        if (!LogPrivacyUtil.allowLogRawData()) {
            result.append(" [raw headers/body require -D")
                    .append(LogPrivacyUtil.LOG_RAWDATA_PROP)
                    .append("=true]");
        }
        return result.toString();
    }

    private String describeEvent(Event event) {
        StringBuilder result = new StringBuilder();
        Map<String, String> headers = event.getHeaders();
        String eventId = findEventId(headers);
        result.append("id=").append(eventId == null ? "-" : sanitize(eventId));

        byte[] body = event.getBody();
        int bodyLength = body == null ? 0 : body.length;
        result.append(", size=").append(bodyLength);
        result.append(", sha256=").append(sha256(body));

        if (LogPrivacyUtil.allowLogRawData()) {
            result.append(", headers=").append(headers);
            result.append(", body=").append(body == null ? "null" : truncate(new String(body, StandardCharsets.UTF_8)));
        }
        return result.toString();
    }

    private String findEventId(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return null;
        }
        for (String headerName : EVENT_ID_HEADERS) {
            String value = headers.get(headerName);
            if (value != null && !value.isEmpty()) {
                return value;
            }
        }
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            for (String headerName : EVENT_ID_HEADERS) {
                if (headerName.equalsIgnoreCase(entry.getKey())
                        && entry.getValue() != null
                        && !entry.getValue().isEmpty()) {
                    return entry.getValue();
                }
            }
        }
        return null;
    }

    private String sha256(byte[] body) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(body == null ? new byte[0] : body);
            StringBuilder result = new StringBuilder(16);
            for (int i = 0; i < 8; i++) {
                result.append(String.format("%02x", hash[i]));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 is not available", ex);
        }
    }

    private String truncate(String value) {
        if (value.length() <= MAX_RAW_EVENT_CHARS) {
            return value;
        }
        return value.substring(0, MAX_RAW_EVENT_CHARS) + "...";
    }

    private String sanitize(String value) {
        return value.replace('\r', ' ').replace('\n', ' ');
    }
}
