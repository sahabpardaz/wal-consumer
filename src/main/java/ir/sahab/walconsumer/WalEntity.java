package ir.sahab.walconsumer;


import javax.persistence.Column;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;

/**
 * WAL stands for write ahead logging. In fact each item of the WAL indicates a manipulation (insert, update or delete)
 * of a entity in relational database which can be synchronized to another target (e.g., a relational database, a NoSQL
 * database, a queue, a file, ...) in an eventual consistent manner. Consuming from WAL and synchronizing the records
 * can be done by {@link WalConsumer}.
 */
@MappedSuperclass
public class WalEntity {

    // Note: It is very critical to use IDENTITY generation type here.
    //       When we were using ID generation type, we had encountered a deadlock:
    //       Each transaction, required 2 connection acquisition from connection pool
    //       (one for generating id using a separate sequence table) and after a while we had
    //       some threads which waits for their second connection without releasing their
    //       first connection -> DEADLOCK!
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY /*VERY CRITICAL, DO NOT CHANGE IT*/)
    private Long id;

    @Column(name = "entity_id")
    private long entityId;

    @Column
    @Enumerated(EnumType.STRING)
    private Operation operation;

    @Column(name = "entity_bytes", length = 20000)  // Default is 255 but it is not sufficient.
    private byte[] entityBytes;

    @Column(name = "entity_type")
    private String entityType;

    public WalEntity() {
        entityType = this.getClass().getSimpleName();
    }

    public long getEntityId() {
        return entityId;
    }

    public void setEntityId(long entityId) {
        this.entityId = entityId;
    }

    public Operation getOperation() {
        return operation;
    }

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    public byte[] getEntityBytes() {
        return entityBytes;
    }

    public void setEntityBytes(byte[] entityBytes) {
        this.entityBytes = entityBytes;
    }

    public String getEntityType() {
        return entityType;
    }

    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    @Override
    public String toString() {
        return String.format("Id: %s, Operation: %s, Entity id: %d, Type: %s", id, operation, entityId, entityType);
    }
}