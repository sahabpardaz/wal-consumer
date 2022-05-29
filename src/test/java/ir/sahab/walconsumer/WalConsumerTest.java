package ir.sahab.walconsumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import ir.sahab.uncaughtexceptionrule.UncaughtExceptionRule;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class WalConsumerTest {

    private volatile boolean returnByFalse = Boolean.FALSE;
    private volatile boolean throwIoException = Boolean.FALSE;
    private final Map<Long, String> model = new HashMap<>();

    @Rule
    public UncaughtExceptionRule uncaughtExceptionRule = new UncaughtExceptionRule();

    private SessionFactory sessionFactory;
    private TestWalEntityRepository walEntityRepository;

    @Before
    public void setUp() {
        // Read hibernate general properties from resource file.
        Configuration configuration = new Configuration();
        configuration.configure("hibernate.cfg.xml");
        Properties properties = configuration.getProperties();

        // Add the extra properties relating to JDBC connection string.
        properties.setProperty("hibernate.connection.url", "jdbc:h2:./db/repository;LOCK_TIMEOUT=20000;MVCC=TRUE");
        properties.setProperty("hibernate.connection.username", "root");
        properties.setProperty("hibernate.connection.password", "123");

        configuration.addAnnotatedClass(TestWalEntity.class);

        // Initialize hibernate session factory.
        sessionFactory = configuration.buildSessionFactory();
        walEntityRepository = new TestWalEntityRepository(sessionFactory);
    }

    @Test
    public void testWalConsumer() throws Exception {
        WalConsumer.setSleepMillisWhenWalIsEmpty(1);
        WalConsumer.setSleepMillisOnIoFailure(1);
        try (WalConsumer walConsumer =
                new WalConsumer(TestWalEntity.class, this::synchronizeModel, sessionFactory, "my_app")) {
            // Start the WAL consumer
            walConsumer.start();

            // Add a new entity and check it will be consumed correctly.
            addWalRecordAndCheck(1L, "name1", Operation.ADD);

            // Add another entity but configure the callback to say that it is a redundant entity (by returning false).
            // Then check that WAL consumer consumes this entity and goes forward.
            returnByFalse = Boolean.TRUE;
            addWalRecordAndCheck(2L, "name2", Operation.ADD);

            // Add a new entity of type "UPDATE" and check that it will be consumed correctly.
            addWalRecordAndCheck(1L, "updated-name", Operation.UPDATE);

            // Add another entity but configure the callback to throw IOException. Then check that WAL consumer retries
            // calling the callback until no exception is thrown and finally we can see it is synchronized.
            throwIoException = Boolean.TRUE;
            addWalRecordAndCheck(3L, "name3", Operation.ADD);

            // Delete an entity and check it will be consumed correctly.
            addWalRecordAndCheck(3L, "name3", Operation.DELETE);
        }
    }

    private void addWalRecordAndCheck(long id, String name, Operation operation) {
        saveTestWalEntity(id, name, operation);
        Awaitility.await()
                .atMost(Durations.ONE_SECOND)
                .with()
                .pollInterval(Durations.ONE_HUNDRED_MILLISECONDS)
                .until(() -> walEntityRepository.count() == 0);
        if (operation.equals(Operation.DELETE)) {
            assertNull(model.get(id));
        } else {
            assertEquals(name, model.get(id));
        }
    }

    private void saveTestWalEntity(Long id, String name, Operation operation) {
        TestWalEntity testWalEntity = new TestWalEntity();
        testWalEntity.setEntityId(id);
        testWalEntity.setOperation(operation);
        testWalEntity.setEntityBytes(name.getBytes(UTF_8));
        walEntityRepository.save(testWalEntity);
    }

    private boolean synchronizeModel(WalEntity entity) throws IOException {
        String name = new String(entity.getEntityBytes(), UTF_8);

        if (throwIoException) {
            throwIoException = false;
            throw new IOException("Dummy IO Exception!");
        }
        Operation operation = entity.getOperation();
        switch (operation) {
            case DELETE:
                model.remove(entity.getEntityId());
                break;
            case ADD:
                assertFalse(model.containsKey(entity.getEntityId()));
                model.put(entity.getEntityId(), name);
                break;
            case UPDATE:
                assertTrue(model.containsKey(entity.getEntityId()));
                model.put(entity.getEntityId(), name);
                break;
            default:
                throw new AssertionError("Invalid wal entity operation!");
        }
        if (returnByFalse) {
            returnByFalse = false;
            return false;
        }
        return true;
    }
}