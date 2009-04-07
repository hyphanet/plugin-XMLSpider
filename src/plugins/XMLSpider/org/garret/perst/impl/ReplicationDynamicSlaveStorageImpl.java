package plugins.XMLSpider.org.garret.perst.impl;

import java.io.IOException;
import java.net.Socket;

import plugins.XMLSpider.org.garret.perst.IFile;
import plugins.XMLSpider.org.garret.perst.StorageError;


public class ReplicationDynamicSlaveStorageImpl extends ReplicationSlaveStorageImpl
{
    public ReplicationDynamicSlaveStorageImpl(String host, int port) { 
        this.host = host;
        this.port = port;
    }

    public void open(IFile file, long pagePoolSize) {
        initialized = false;
        prevIndex = -1;
        outOfSync = true;
        super.open(file, pagePoolSize);
    }

    Socket getSocket() throws IOException { 
        if (opened) {
            throw new StorageError(StorageError.CONNECTION_FAILURE);
        }
        return new Socket(host, port);
    }

    protected String host;
    protected int    port;
}    

    
                                               