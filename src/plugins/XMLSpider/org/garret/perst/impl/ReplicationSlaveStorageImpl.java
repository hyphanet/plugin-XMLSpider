package plugins.XMLSpider.org.garret.perst.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import plugins.XMLSpider.org.garret.perst.IFile;
import plugins.XMLSpider.org.garret.perst.IResource;
import plugins.XMLSpider.org.garret.perst.PersistentResource;
import plugins.XMLSpider.org.garret.perst.ReplicationSlaveStorage;
import plugins.XMLSpider.org.garret.perst.StorageError;


public abstract class ReplicationSlaveStorageImpl extends StorageImpl implements ReplicationSlaveStorage, Runnable
{ 
    static final int REPL_CLOSE = -1;
    static final int REPL_SYNC  = -2;
    
    public void open(IFile file, long pagePoolSize) {
        if (opened) {
            throw new StorageError(StorageError.STORAGE_ALREADY_OPENED);
        }
        initialize(file, pagePoolSize);
        lock = new PersistentResource();
        init = new Object();
        sync = new Object();
        done = new Object();
        commit = new Object();
        listening = true;
        connect();        
        thread = new Thread(this);
        thread.start();
        waitSynchronizationCompletion();
        waitInitializationCompletion(); 
        opened = true;
        beginThreadTransaction(REPLICATION_SLAVE_TRANSACTION);
        reloadScheme();
        endThreadTransaction();
    }


    /**
     * Check if socket is connected to the master host
     * @return <code>true</code> if connection between slave and master is sucessfully established
     */
    public boolean isConnected() {
        return socket != null;
    }
    
    public void beginThreadTransaction(int mode)
    {
        if (mode != REPLICATION_SLAVE_TRANSACTION) {
            throw new IllegalArgumentException("Illegal transaction mode");
        }
        lock.sharedLock();
        Page pg = pool.getPage(0);
        header.unpack(pg.data);
        pool.unfix(pg);
        currIndex = 1-header.curr;
        currIndexSize = header.root[1-currIndex].indexUsed;
        committedIndexSize = currIndexSize;
        usedSize = header.root[currIndex].size;
        objectCache.clear();
    }
     
    public void endThreadTransaction(int maxDelay)
    {
        lock.unlock();
    }

    protected void waitSynchronizationCompletion() {
        try { 
            synchronized (sync) { 
                while (outOfSync) { 
                    sync.wait();
                }
            }
        } catch (InterruptedException x) { 
        }
    }

    protected void waitInitializationCompletion() {
        try { 
            synchronized (init) { 
                while (!initialized) { 
                    init.wait();
                }
            }
        } catch (InterruptedException x) { 
        }
    }

    /**
     * Wait until database is modified by master
     * This method blocks current thread until master node commits trasanction and
     * this transanction is completely delivered to this slave node
     */
    public void waitForModification() { 
        try { 
            synchronized (commit) { 
                if (socket != null) { 
                    commit.wait();
                }
            }
        } catch (InterruptedException x) { 
        }
    }

    protected static final int DB_HDR_CURR_INDEX_OFFSET  = 0;
    protected static final int DB_HDR_DIRTY_OFFSET       = 1;
    protected static final int DB_HDR_INITIALIZED_OFFSET = 2;
    protected static final int PAGE_DATA_OFFSET          = 8;
    
    public static int LINGER_TIME = 10; // linger parameter for the socket

    /**
     * When overriden by base class this method perfroms socket error handling
     * @return <code>true</code> if host should be reconnected and attempt to send data to it should be 
     * repeated, <code>false</code> if no more attmpts to communicate with this host should be performed 
     */     
    public boolean handleError() 
    {
        return (listener != null) ? listener.replicationError(null) : false;
    }

    void connect()
    {
        try { 
            socket = getSocket();
            try {
                socket.setSoLinger(true, LINGER_TIME);
            } catch (NoSuchMethodError er) {}
            try { 
                socket.setTcpNoDelay(true);
            } catch (Exception x) {}
            in = socket.getInputStream();
            if (replicationAck) { 
                out = socket.getOutputStream();
            }
        } catch (IOException x) { 
            socket = null;
            in = null;
        }
    }

    abstract Socket getSocket() throws IOException;

    void cancelIO() {}

    public void run() { 
        byte[] buf = new byte[Page.pageSize+PAGE_DATA_OFFSET];

        while (listening) { 
            int offs = 0;
            do {
                int rc;
                try { 
                    rc = in.read(buf, offs, buf.length - offs);
                } catch (IOException x) { 
                    rc = -1;
                }
                synchronized(done) { 
                    if (!listening) { 
                        return;
                    }
                }
                if (rc < 0) { 
                    if (handleError()) { 
                        connect();
                    } else { 
                        return;
                    }
                } else { 
                    offs += rc;
                }
            } while (offs < buf.length);
            
            long pos = Bytes.unpack8(buf, 0);
            boolean transactionCommit = false;
            if (pos == 0) { 
                if (replicationAck) { 
                    try { 
                        out.write(buf, 0, 1);
                    } catch (IOException x) {
                        handleError();
                    }
                }
                if (buf[PAGE_DATA_OFFSET + DB_HDR_CURR_INDEX_OFFSET] != prevIndex) { 
                    prevIndex = buf[PAGE_DATA_OFFSET + DB_HDR_CURR_INDEX_OFFSET];
                    lock.exclusiveLock();
                    transactionCommit = true;
                }
            } else if (pos == REPL_SYNC) { 
                synchronized(sync) { 
                    outOfSync = false;
                    sync.notify();
                }
                continue;
            } else if (pos == REPL_CLOSE) { 
                synchronized(commit) { 
                    hangup();
                    commit.notifyAll();
                }     
                return;
            }
            
            Page pg = pool.putPage(pos);
            System.arraycopy(buf, PAGE_DATA_OFFSET, pg.data, 0, Page.pageSize);
            pool.unfix(pg);
            
            if (pos == 0) { 
                if (!initialized && buf[PAGE_DATA_OFFSET + DB_HDR_INITIALIZED_OFFSET] != 0) { 
                    synchronized(init) { 
                        initialized = true;
                        init.notify();
                    }
                }
                if (transactionCommit) { 
                    lock.unlock();
                    synchronized(commit) { 
                        commit.notifyAll();
                    }
                    pool.flush();
                }
            }
        }            
    }

    public void close() {
        synchronized (done) {
            listening = false;
        }
        cancelIO();
        try { 
            thread.interrupt();
            thread.join();
        } catch (InterruptedException x) {}

        hangup();

        pool.flush();
        super.close();
    }

    protected void hangup() { 
        if (socket != null) { 
            try { 
                in.close();
                if (out != null) { 
                    out.close();
                }
                socket.close();
            } catch (IOException x) {}
            in = null;
            socket = null;
        }
    }

    protected boolean isDirty() { 
        return false;
    }

    protected InputStream  in;
    protected OutputStream out;
    protected Socket       socket;
    protected boolean      outOfSync;
    protected boolean      initialized;
    protected boolean      listening;
    protected Object       sync;
    protected Object       init;
    protected Object       done;
    protected Object       commit;
    protected int          prevIndex;
    protected IResource    lock;
    protected Thread       thread;
}
