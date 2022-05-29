package ir.sahab.walconsumer;

import java.io.IOException;

/**
 * A callback for synchronizing a {@link WalEntity} to its desired target (e.g., file, database, JMS, ...)
 *
 * @see WalConsumer
 */
public interface WalEntityConsumerCallback {

    /**
     * Synchronizes the {@link WalEntity} to the desired target. In case of returning false, it means the sync
     * operation is already done, otherwise indicates that the operation is done right now.
     */
    boolean syncEntity(WalEntity entity) throws InterruptedException, IOException;
}