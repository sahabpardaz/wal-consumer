package ir.sahab.walconsumer;

import java.io.IOException;
import org.hibernate.Session;
import org.hibernate.Transaction;

/**
 * Represents a handle to the WAL head item. Just the owner of the handle can process the WAL
 * item and other calls to get the handle will be blocked until the handle owner calls
 * {@link #deleteHeadAndReleaseHandle()}.
 */
class WalHeadHandle {
    // Wal head item, that this handle refers to.
    private final WalEntity head;

    // We need it when we want to delete the head, commit transaction and release the handle,
    // although it is hidden from the handle owner.
    private final Session session;

    public WalHeadHandle(WalEntity head, Session session) {
        this.session = session;
        this.head = head;
    }

    public WalEntity getHead() {
        return head;
    }

    public void deleteHeadAndReleaseHandle() throws IOException {
        try {
            session.delete(head);
            session.getTransaction().commit();
        } catch (RuntimeException e) {
            throw new IOException(e.getMessage(), e.getCause());
        } finally {
            Transaction transaction = session.getTransaction();
            if (transaction.getStatus().canRollback()) {
                transaction.rollback();
            }
            session.close();
        }
    }
}