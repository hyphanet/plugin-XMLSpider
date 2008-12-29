package plugins.XMLSpider.org.garret.perst;
import plugins.XMLSpider.org.garret.perst.impl.*;

/**
 * Storage factory
 */
public class StorageFactory {
    /**
     * Create new instance of the storage
     * @return new instance of the storage (unopened, you should explicitely invoke open method)
     */
    public Storage createStorage() {
        return new StorageImpl();
    }

    /**
     * Create new instance of the master node of replicated storage. There are two kinds of replication slave nodes:
     * statically defined and dynamically added. First one are specified by replicationSlaveNodes parameter.
     * When replication master is started it tries to eastablish connection with all of the specified nodes. 
     * It is expected that state of each such node is synchronized with state of the master node.
     * It is not possible to add or remove static replication slave node without stopping master node.
     * Dynamic slave nodes can be added at any moment of time. Replication master will send to such node complete 
     * snapshot of the database.
     * @param port socket port at which replication master will listen for dynamic slave nodes connections. If this parameter 
     * is -1, then no dynamic slave node conenctions are accepted.
     * @param replicationSlaveNodes addresses of static replicatin slave nodes, i.e. hosts to which replication 
     * will be performed. Address is specified as NAME:PORT
     * @param asyncBufSize if value of this parameter is greater than zero then replication will be 
     * asynchronous, done by separate thread and not blocking main application. 
     * Otherwise data is send to the slave nodes by the same thread which updates the database.
     * If space asynchronous buffer is exhausted, then main thread will be also blocked until the
     * data is send.     
     * @return new instance of the master storage (unopened, you should explicitely invoke open method)
     */
    public ReplicationMasterStorage createReplicationMasterStorage(int port, String[] replicationSlaveNodes, int asyncBufSize) {
        return new ReplicationMasterStorageImpl(port, replicationSlaveNodes, asyncBufSize);
    }

    /**
     * Create new instance of the static slave node of replicated storage.
     * The address of this host should be sepecified in the replicationSlaveNodes
     * parameter of createReplicationMasterStorage method. When replication master
     * is started it tries to eastablish connection with all of the specified nodes. 
     * @param slavePort  socket port at which connection from master will be established
     * @return new instance of the slave storage (unopened, you should explicitely invoke open method)
     */
    public ReplicationSlaveStorage createReplicationSlaveStorage(int slavePort) {
        return new ReplicationStaticSlaveStorageImpl(slavePort);
    }

    /**
     * Add new instance of the dynamic slave node of replicated storage. 
     * @param replicationMasterNode name of the host where replication master is running
     * @param masterPort replication master socket port to which connection should be established
     * @return new instance of the slave storage (unopened, you should explicitely invoke open method)
     */
    public ReplicationSlaveStorage addReplicationSlaveStorage(String replicationMasterNode, int masterPort) {
        return new ReplicationDynamicSlaveStorageImpl(replicationMasterNode, masterPort);
    }

    /**
     * Get instance of storage factory.
     * So new storages should be create in application in the following way:
     * <code>StorageFactory.getInstance().createStorage()</code>
     * @return instance of the storage factory
     */
    public static StorageFactory getInstance() { 
        return instance;
    }

    protected static final StorageFactory instance = new StorageFactory();
};
