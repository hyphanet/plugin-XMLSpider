package plugins.XMLSpider.org.garret.perst.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import plugins.XMLSpider.org.garret.perst.IFile;
import plugins.XMLSpider.org.garret.perst.StorageError;


/**
 * File performing replication of changed pages to specified slave nodes.
 */
public class ReplicationMasterFile implements IFile, Runnable 
{ 
    /**
     * Constructor of replication master file
     * @param storage replication storage
     * @param file local file used to store data locally
     */
    public ReplicationMasterFile(ReplicationMasterStorageImpl storage, IFile file) { 
        this(storage, file, storage.port, storage.hosts, storage.replicationAck);
    }

    /**
     * Constructor of replication master file
     * @param file local file used to store data locally
     * @param hosts slave node hosts to which replicastion will be performed
     * @param ack whether master should wait acknowledgment from slave node during trasanction commit
     */
    public ReplicationMasterFile(IFile file, String[] hosts, boolean ack) {         
        this(null, file, -1, hosts, ack);
    }
    
    private ReplicationMasterFile(ReplicationMasterStorageImpl storage, IFile file, int port, String[] hosts, boolean ack) {
        this.storage = storage;
        this.file = file;
        this.hosts = hosts;
        this.ack = ack;
        this.port = port;
        mutex = new Object();
        sockets = new Socket[hosts.length];
        out = new OutputStream[hosts.length];
        if (ack) { 
            in = new InputStream[hosts.length];
            rcBuf = new byte[1];
        }
        txBuf = new byte[8 + Page.pageSize];
        nHosts = 0;
        for (int i = 0; i < hosts.length; i++) { 
            connect(i);
        }
        if (port >= 0) {
            try { 
                listenSocket = new ServerSocket(port);            
            } catch (IOException x) {
                throw new StorageError(StorageError.BAD_REPLICATION_PORT);
            }
            listening = true;
            listenThread = new Thread(this);
            listenThread.start();
        }
    }

    public void run() { 
        while (true) { 
            Socket s = null;
            try { 
                s = listenSocket.accept();
            } catch (IOException x) {
                x.printStackTrace();
            }
            synchronized (mutex) { 
                if (!listening) { 
                    return;
                }
            }
            if (s != null) { 
                try {
                    s.setSoLinger(true, LINGER_TIME);
                } catch (Exception x) {}
                try { 
                    s.setTcpNoDelay(true);
                } catch (Exception x) {}
                addConnection(s);
            }
        }
    }
         
    private void addConnection(Socket s) {
        OutputStream os = null;
        InputStream is = null;
        try { 
            os = s.getOutputStream();
            if (ack) { 
                is = s.getInputStream();
            }
        } catch (IOException x) { 
            x.printStackTrace();
            return;
        }
        synchronized (mutex) { 
            int n = hosts.length;
            String[] newHosts = new String[n+1];
            System.arraycopy(hosts, 0, newHosts, 0, n);
            newHosts[n] = s.getRemoteSocketAddress().toString();
            hosts = newHosts;
            OutputStream[] newOut = new OutputStream[n+1];
            System.arraycopy(out, 0, newOut, 0, n);            
            newOut[n] = os; 
            out = newOut;
            if (ack) { 
                InputStream[] newIn = new InputStream[n+1];
                System.arraycopy(in, 0, newIn, 0, n);            
                newIn[n] = is; 
                in = newIn;
            }
            Socket[] newSockets = new Socket[n+1];
            System.arraycopy(sockets, 0, newSockets, 0, n);
            newSockets[n] = s;
            sockets = newSockets;
            nHosts += 1;

            Thread syncThread = new SynchronizeThread(n);           
            // syncThread.run();
            syncThread.start();
        }
    }

    class SynchronizeThread extends Thread { 
        int i;

        SynchronizeThread(int i) { 
            this.i = i;
            //setPriority(Thread.NORM_PRIORITY-1);
        }

        public void run() { 
            long size = storage.getDatabaseSize();
            Socket s;
            OutputStream os = null;
            InputStream is = null;
            synchronized (mutex) { 
                s = sockets[i];
                if (s == null) { 
                    return;
                }
                os = out[i];
                if (ack) { 
                    is = in[i];
                }
            }
            for (long pos = 0; pos < size; pos += Page.pageSize) { 
                Page pg = storage.pool.getPage(pos);
                try {                    
                    synchronized (s) {
                        Bytes.pack8(txBuf, 0, pos);
                        System.arraycopy(pg.data, 0, txBuf, 8, Page.pageSize);
                        storage.pool.unfix(pg);
                        os.write(txBuf);
                        if (!ack || pos != 0 || is.read(rcBuf) == 1) { 
                            continue;
                        }
                    }
                } catch (IOException x) {
                    x.printStackTrace();
                }
                synchronized (mutex) { 
                    if (sockets[i] != null) { 
                        handleError(hosts[i]);
                        sockets[i] = null;
                        out[i] = null;
                        nHosts -= 1;
                    }
                    return;
                }
            }
            synchronized (s) {
                Bytes.pack8(txBuf, 0, ReplicationSlaveStorageImpl.REPL_SYNC);
                try {                    
                    os.write(txBuf); // end of sycnhronization
                } catch (IOException x) {
                    x.printStackTrace();
                }
            }   
        }
    }          
          
    public int getNumberOfAvailableHosts() { 
        return nHosts;
    }

    protected void connect(int i)
    {
        String host = hosts[i];
        int colon = host.indexOf(':');
        int port = Integer.parseInt(host.substring(colon+1));
        host = host.substring(0, colon);
        Socket socket = null; 
        try { 
            int maxAttempts = storage != null 
                ? storage.slaveConnectionTimeout : MAX_CONNECT_ATTEMPTS;
            for (int j = 0; j < maxAttempts; j++) { 
                try { 
                    socket = new Socket(InetAddress.getByName(host), port);
                    if (socket != null) { 
                        break;
                    }
                    Thread.sleep(CONNECTION_TIMEOUT);
                } catch (IOException x) {}
            }
        } catch (InterruptedException x) {}
            
        if (socket != null) { 
            try { 
                try {
                    socket.setSoLinger(true, LINGER_TIME);
                } catch (NoSuchMethodError er) {}
                try { 
                    socket.setTcpNoDelay(true);
                } catch (Exception x) {}
                sockets[i] = socket;
                out[i] = socket.getOutputStream();
                if (ack) { 
                    in[i] = socket.getInputStream();
                }
                nHosts += 1;
            } catch (IOException x) { 
                handleError(hosts[i]);
                sockets[i] = null;
                out[i] = null;
            }
        } 
    }

    /**
     * When overriden by base class this method perfroms socket error handling
     * @return <code>true</code> if host should be reconnected and attempt to send data to it should be 
     * repeated, <code>false</code> if no more attmpts to communicate with this host should be performed 
     */     
    public boolean handleError(String host) 
    {
        System.err.println("Failed to establish connection with host " + host);
        return (storage != null && storage.listener != null) 
            ? storage.listener.replicationError(host) 
            : false;
    }


    public void write(long pos, byte[] buf) {
        synchronized (mutex) { 
            for (int i = 0; i < out.length; i++) { 
                while (out[i] != null) {                 
                    try { 
                        synchronized (sockets[i]) { 
                            Bytes.pack8(txBuf, 0, pos);
                            System.arraycopy(buf, 0, txBuf, 8, buf.length);
                            out[i].write(txBuf);
                            if (!ack || pos != 0 || in[i].read(rcBuf) == 1) { 
                                break;
                            }
                        }
                    } catch (IOException x) {} 
                    
                    out[i] = null;
                    sockets[i] = null;
                    nHosts -= 1;
                    if (handleError(hosts[i])) { 
                        connect(i);
                    } else { 
                        break;
                    }
                }
            }
        }
        file.write(pos, buf);
    }

    public int read(long pos, byte[] buf) {
        return file.read(pos, buf);
    }

    public void sync() {
        file.sync();
    }

    public boolean tryLock(boolean shared) { 
        return file.tryLock(shared);
    }

    public void lock(boolean shared) { 
        file.lock(shared);
    }

    public void unlock() { 
        file.unlock();
    }

    public void close() {
        if (listenThread != null) { 
            synchronized (mutex) { 
                listening = false;
            }
            try { 
                Socket s = new Socket("localhost", port);
                s.close();
            } catch (IOException x) {}
            try {
                listenThread.join();
            } catch (InterruptedException x) {}
            try { 
                listenSocket.close();
            } catch (IOException x) {}
        }
                
        file.close();
        Bytes.pack8(txBuf, 0, ReplicationSlaveStorageImpl.REPL_CLOSE);
        for (int i = 0; i < out.length; i++) {  
            if (sockets[i] != null) { 
                try {  
                    out[i].write(txBuf);
                    out[i].close();
                    if (in != null) { 
                        in[i].close();
                    }
                    sockets[i].close();
                } catch (IOException x) {}
            }
        }
    }

    public long length() {
        return file.length();
    }

    public static int LINGER_TIME = 10; // linger parameter for the socket
    public static int MAX_CONNECT_ATTEMPTS = 10; // attempts to establish connection with slave node
    public static int CONNECTION_TIMEOUT = 1000; // timeout between attempts to conbbect to the slave

    Object         mutex;
    OutputStream[] out;
    InputStream[]  in;
    Socket[]       sockets;
    byte[]         txBuf;
    byte[]         rcBuf;
    IFile          file;
    String[]       hosts;
    int            nHosts;
    int            port;
    boolean        ack;
    boolean        listening;
    Thread         listenThread;
    ServerSocket   listenSocket;

    
    ReplicationMasterStorageImpl storage;
}
