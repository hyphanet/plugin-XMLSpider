package plugins.XMLSpider.org.garret.perst.impl;

import java.io.*;
import java.net.*;

import plugins.XMLSpider.org.garret.perst.*;


public class ReplicationDynamicSlaveStorageImpl extends ReplicationSlaveStorageImpl
{
    public ReplicationDynamicSlaveStorageImpl(String host, int port) { 
        this.host = host;
        this.port = port;
    }

    public void open(IFile file, int pagePoolSize) {
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

    
                                               