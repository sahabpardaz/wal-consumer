package ir.sahab.walconsumer;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.google.common.annotations.VisibleForTesting;
import ir.sahab.dropwizardmetrics.LabelSupportedObjectNameFactory;
import java.io.Closeable;
import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.TimeUnit;
import javax.persistence.Table;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A consumer from a WAL table in a relational database. One of the main use cases of the WAL in a relational database,
 * is to synchronize another target (e.g., a relational database, a NoSQL database, a key-value store, a queue, ...)
 * based on the last changes applied to the relational database. This way, we can implement 'eventual consistency'
 * between different targets/repositories. So besides the other tables in a relational database, we have a WAL table
 * which is described by {@link WalEntity} class. It includes both the operation type (ADD, UPDATE or DELETE) and the
 * whole entity (in byte array) to which the operation is applied. This consumer reads sequentially from the WAL and
 * provides the head item to a callback which is responsible for the synchronization. Note that it is safe to create two
 * concurrent WAL consumer in a same WAL table because whenever a consumer is consuming a record, the other ones will be
 * blocked. This is useful for providing HA (high availability) by multiple instances.
 *
 * <p>To provide a synchronization mechanism and guarantee eventual consistency, you should do the followings:
 * <ul>
 *   <li> Register the WalEntity object to the list of your ORM entities.
 *   <li> Whenever you apply an operation in your database, also add a WalEntity in a same transaction that the
 *   operation is applied.
 *   <li> Create a new instance of this class and provide it with your desired synchronization callback.
 * </ul>
 */
public class WalConsumer implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(WalConsumer.class);

    private static int sleepMillisWhenWalIsEmpty = 1000;
    private static int sleepMillisOnIoFailure = 1000;

    private String metricPrefix = "wal";

    // Metric names
    private static final String STATE = "_state";
    private static final String NUMBER_OF_RECORDS = "_num_records";
    private static final String NUM_SYNCHRONIZED = "_num_synchronized";
    private static final String NUM_IGNORED_ALREADY_DONE = "_num_ignored_already_done";
    private static final String NOT_EMPTY_SECONDS = "_not_empty_seconds";

    // For reporting metrics
    private final MetricRegistry metricRegistry;
    private JmxReporter jmxReporter;
    private volatile WalState walState = WalState.NONE;
    private volatile long walNotEmptyStartedNano = -1L;

    private Thread thread;
    private volatile boolean stop = false;

    private final SessionFactory sessionFactory;
    private final Class<?> walEntityClass;
    private final String walTableName;
    private final WalEntityConsumerCallback walEntityConsumerCallback;

    @SuppressWarnings("java:S1905")
    public WalConsumer(Class<?> walEntityClass, WalEntityConsumerCallback walEntityConsumerCallback,
            SessionFactory sessionFactory, MetricRegistry metricRegistry) {
        this.walEntityClass = walEntityClass;
        this.walEntityConsumerCallback = walEntityConsumerCallback;
        this.sessionFactory = sessionFactory;
        this.metricRegistry = metricRegistry;
        this.walTableName = getTableName(walEntityClass);
        metricRegistry.register(getMetricName(NUMBER_OF_RECORDS),
                new CachedGauge<Long>(1, TimeUnit.MINUTES) {
                    @Override
                    protected Long loadValue() {
                        try {
                            return count();
                        } catch (IOException e) {
                            return null;
                        }
                    }
                });
        Gauge<Long> walNotEmptySecondsGauge = () -> {
            if (walNotEmptyStartedNano == -1L) {
                return 0L;
            } else {
                return (System.nanoTime() - walNotEmptyStartedNano) / 1_000_000_000L;
            }
        };
        metricRegistry.register(getMetricName(NOT_EMPTY_SECONDS), walNotEmptySecondsGauge);
        metricRegistry.register(getMetricName(STATE), ((Gauge<String>) walState::toString));
    }

    public WalConsumer(Class<?> walEntityClass, WalEntityConsumerCallback walEntityConsumerCallback,
            SessionFactory sessionFactory, MetricRegistry metricRegistry, String metricPrefix) {
        this(walEntityClass, walEntityConsumerCallback, sessionFactory, metricRegistry);
        this.metricPrefix = metricPrefix;
    }

    public WalConsumer(Class<?> walEntityClass, WalEntityConsumerCallback walEntityConsumerCallback,
            SessionFactory sessionFactory, String metricDomainName) {
        this(walEntityClass, walEntityConsumerCallback, sessionFactory, new MetricRegistry());
        jmxReporter = JmxReporter.forRegistry(metricRegistry)
                .createsObjectNamesWith(new LabelSupportedObjectNameFactory())
                .inDomain(metricDomainName)
                .build();
        jmxReporter.start();
    }

    @VisibleForTesting
    public static void setSleepMillisWhenWalIsEmpty(int sleepMillisWhenWalIsEmpty) {
        WalConsumer.sleepMillisWhenWalIsEmpty = sleepMillisWhenWalIsEmpty;
    }

    @VisibleForTesting
    public static void setSleepMillisOnIoFailure(int sleepMillisOnIoFailure) {
        WalConsumer.sleepMillisOnIoFailure = sleepMillisOnIoFailure;
    }

    @SuppressWarnings({"squid:S3776", "squid:S1141"})
    public void start() {
        thread = new Thread(() -> {
            logger.info("{} started", thread.getName());
            while (!stop) {
                try {
                    // Acquire the handle of the head item in the WAL.
                    WalHeadHandle headHandle = null;
                    do {
                        try {
                            headHandle = acquireWalHeadHandle();
                        } catch (IOException e) {
                            logger.warn("Failed to acquire wal head handle. Retrying...", e);
                            setWalState(WalState.INACCESSIBLE_IO_FAILURE);
                            Thread.sleep(sleepMillisOnIoFailure);
                            continue;
                        }
                        // Apparently Hibernate gobbles up InterruptedException without giving us any hints!
                        // The follow statement uses the 'stop' flag to break out of loop when thread has been
                        // interrupted.
                        if (stop) {
                            throw new InterruptedException("Hibernate gobbles up InterruptedException");
                        }

                        if (headHandle == null) {  // It means WAL is empty.
                            setWalState(WalState.EMPTY);
                            Thread.sleep(sleepMillisWhenWalIsEmpty);
                        }
                    } while (headHandle == null);
                    setWalState(WalState.NOT_EMPTY);

                    // Synchronize the entity using the provided callback.
                    WalEntity entity = headHandle.getHead();
                    synchronizeEntity(entity);
                    logger.debug("WAL record synchronized: {}", entity);
                    metricRegistry.meter(getMetricName(NUM_SYNCHRONIZED)).mark();

                    // Delete the head and release its handle.
                    try {
                        headHandle.deleteHeadAndReleaseHandle();
                    } catch (IOException e) {
                        logger.warn(
                                "Failed to apply transaction: deleting the WAL head and doing its synchronization logic"
                                        + " in a single transaction. But the transaction is rolled back and it will be "
                                        + "processed again.", e);
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    if (!stop) {
                        throw new AssertionError("Unexpected InterruptedException", ex);
                    }
                    break;
                }
            }
        }, walEntityClass.getSimpleName() + " WAL Consumer");
        thread.start();
    }

    /**
     * It returns a handle to the WAL head item and sets a lock for processing items from WAL, so that any concurrent
     * call will be blocked until the owner of the handle calls {@link WalHeadHandle#deleteHeadAndReleaseHandle()}. So
     * it is guaranteed that each item will be processed just once, and they will be processed sequentially (i.e., in
     * any point in time there is just one instance who is processing an item from WAL). If there is no item in the WAL
     * it returns null.
     *
     * @return A handle to the head item of the WAL. An instance who gets the handle, should first process the item and
     *      then release the handle by a call to {@link WalHeadHandle#deleteHeadAndReleaseHandle()}.
     */
    private WalHeadHandle acquireWalHeadHandle() throws IOException {
        while (true) {
            // Find id of the entity which is located in the head of the WAL and if there is no item
            // in the WAL, return null.
            Long headId = getWalHeadId();
            if (headId == null) {
                return null;
            }

            Session session = sessionFactory.openSession();
            Transaction transaction = null;
            try {
                transaction = session.beginTransaction();

                // Retrieve the head item and put a lock for processing the WAL.
                // Note that because we have used Sql command 'select ... for update', no one can
                // get the same record (i.e., the WAL head) for update until we commit/rollback this
                // transaction. So using this Sql structure, we can guarantee that the WAL items
                // will be processed just once, and they will be processed sequentially
                // (i.e., in any point in time there is just one instance who is processing an item
                // from WAL).
                WalEntity walEntity = (WalEntity) session.createNativeQuery(
                                "Select * from " + walTableName + " where id = :id for update")
                        .addEntity(walEntityClass).setParameter("id", headId).uniqueResult();

                // If there is no item in the head, it means someone is competing with us:
                // The competitor has gotten ID of the WAL head almost concurrently with us,
                // but it has acquired the lock before us. In this situation, we have lost this
                // competition, and we can just compete for the next item. So we will go to retry,
                // but we will be blocked until the current head is consumed and the lock is
                // released.
                if (walEntity == null) {
                    safeClose(session, transaction);
                    continue;
                }
                return new WalHeadHandle(walEntity, session);
            } catch (RuntimeException e) {
                safeClose(session, transaction);
                throw new IOException(e.getMessage(), e.getCause());
            }
        }
    }

    /**
     * Returns WAL head item's ID or returns null if there is no item in the WAL.
     */
    private Long getWalHeadId() throws IOException {
        Session session = sessionFactory.openSession();
        Transaction transaction = null;
        try {
            transaction = session.beginTransaction();
            BigInteger minId = (BigInteger)
                    session.createNativeQuery("select MIN(id) from " + walTableName).uniqueResult();
            if (minId != null) {
                return minId.longValue();
            } else {
                return null;
            }
        } catch (RuntimeException e) {
            throw new IOException(e.getMessage(), e.getCause());
        } finally {
            safeClose(session, transaction);
        }
    }

    private void synchronizeEntity(WalEntity entity) throws InterruptedException {
        boolean alreadyDone;
        do {
            try {
                alreadyDone = !walEntityConsumerCallback.syncEntity(entity);
                break;
            } catch (IOException ex) {
                logger.warn("Failed to synchronize the WAL entity. Retrying...", ex);
                Thread.sleep(sleepMillisOnIoFailure);
            }
        } while (true);

        if (alreadyDone) {
            logger.warn("Wants to {} the entity but it is already done. "
                    + "It can be valid in some rare cases, e.g., "
                    + "when in the previous iteration, we have failed to "
                    + "apply the head and it is not our first attempt."
                    + "WAL Entity: {}", entity.getOperation(), entity);
            metricRegistry.meter(getMetricName(NUM_IGNORED_ALREADY_DONE)).mark();
        }
    }

    private void setWalState(WalState currentWalState) {
        walState = currentWalState;
        if (walState == WalState.NOT_EMPTY) {
            if (walNotEmptyStartedNano == -1L) {
                walNotEmptyStartedNano = System.nanoTime();
            }
        } else {
            walNotEmptyStartedNano = -1L;
        }
    }

    private static void safeClose(Session session, Transaction transaction) {
        if (transaction != null && transaction.getStatus().canRollback()) {
            transaction.rollback();
        }
        session.close();
    }

    @Override
    public void close() throws IOException {
        this.stop = true;
        thread.interrupt();
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Unexpected interrupt.", e);
        }
        logger.info("{} stopped.", thread.getName());
        metricRegistry.remove(getMetricName(STATE));
        metricRegistry.remove(getMetricName(NUMBER_OF_RECORDS));
        metricRegistry.remove(getMetricName(NUM_SYNCHRONIZED));
        metricRegistry.remove(getMetricName(NUM_IGNORED_ALREADY_DONE));
        if (jmxReporter != null) {
            jmxReporter.close();
        }
    }

    /**
     * Returns the number of records in the WAL table which is not consumed yet.
     */
    private Long count() throws IOException {
        Session session = sessionFactory.openSession();
        try {
            BigInteger count = (BigInteger)
                    session.createNativeQuery("select COUNT(1) from " + walTableName).uniqueResult();
            return count.longValue();
        } catch (RuntimeException e) {
            throw new IOException(String.format("Failed to count walEntity of type %s.", walEntityClass), e.getCause());
        } finally {
            safeClose(session, null);
        }
    }

    private String getMetricName(String metric) {
        return metricPrefix + metric;
    }

    private static String getTableName(Class<?> walEntityClass) {
        Table annotation = walEntityClass.getAnnotation(Table.class);
        if (annotation == null) {
            throw new AssertionError("Wal Entity must be annotated with javax.persistence.Table!");
        }
        if ("".equalsIgnoreCase(annotation.name().trim())) {
            return walEntityClass.getSimpleName();
        }
        if (StringUtils.isNotEmpty(annotation.schema())) {
            return annotation.schema() + "." + annotation.name();
        } else {
            return annotation.name();
        }
    }

    private enum WalState {
        NONE,
        EMPTY,
        NOT_EMPTY,
        INACCESSIBLE_IO_FAILURE;
    }
}