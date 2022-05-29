package ir.sahab.walconsumer;


import java.math.BigInteger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

/**
 * A repository of type {@link TestWalEntity} defined just to be used in {@link WalConsumerTest}.
 */
public class TestWalEntityRepository {

    private final SessionFactory sessionFactory;

    public TestWalEntityRepository(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    void save(TestWalEntity testWalEntity) {
        Session session = sessionFactory.openSession();
        Transaction transaction = null;
        try {
            transaction = session.beginTransaction();
            session.persist(testWalEntity);
            transaction.commit();
        } finally {
            safeClose(session, transaction);
        }
    }

    public int count() {
        Session session = sessionFactory.openSession();
        Transaction transaction = null;
        try {
            transaction = session.beginTransaction();
            BigInteger count = (BigInteger) session.createNativeQuery("select count(1) from test_wal")
                    .uniqueResult();
            return count.intValue();
        } finally {
            safeClose(session, transaction);
        }
    }

    private void safeClose(Session session, Transaction transaction) {
        if (transaction != null && transaction.getStatus().canRollback()) {
            transaction.rollback();
        }
        session.close();
    }
}