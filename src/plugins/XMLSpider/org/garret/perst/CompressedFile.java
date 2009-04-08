package plugins.XMLSpider.org.garret.perst;

import java.io.*;
import java.util.*;
import java.util.zip.*;

/**
 * Compressed read-only database file. You should create database using normal file (OSFile).
 * Then use CompressDatabase utility to compress database file.
 * To work with compressed database file you should pass instance of this class in <code>Storage.open</code> method
 */
public class CompressedFile implements IFile 
{ 
    static final int SEGMENT_LENGTH = 128*1024;

    public void write(long pos, byte[] buf) {
        throw new UnsupportedOperationException("ZipFile.write");
    }

    public int read(long pos, byte[] buf) {
        try {
            int seg = (int)(pos / SEGMENT_LENGTH);
            ZipEntry e = entries[seg];
            int size = (int)e.getSize();
            if (e != currEntry) {
                InputStream in = file.getInputStream(e);
                int rc, offs = 0;
                while (offs < size && (rc = in.read(segment, offs, size - offs)) >= 0) { 
                    offs += rc;
                }
                if (offs != size) { 
                    throw new StorageError(StorageError.FILE_ACCESS_ERROR);
                }
                currEntry = e;
                
            }
            int offs = (int)(pos - (long)seg*SEGMENT_LENGTH);
            if (offs < size) { 
                int len = buf.length < size - offs ? buf.length : size - offs;
                System.arraycopy(segment, offs, buf, 0, len);
                return len;
            } 
            return 0;
        } catch (IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR);
        }
    }

    public void sync() {
    }

    public boolean tryLock(boolean shared) { 
        return true;
    }

    public void lock(boolean shared) { 
    }

    public void unlock() { 
    }

    public void close() {
        try {
            file.close();
        } catch (IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR);
        }
    }

    public long length() { 
        return (long)(entries.length-1)*SEGMENT_LENGTH + entries[entries.length-1].getSize();
    }

    /**
     * Constructor of comressed file
     * @param path path to the archieve previously prepared by CompressDatabase utility
     */
    public CompressedFile(String path) {
        try {
            file = new ZipFile(path);
            int nEntries = file.size();
            entries = new ZipEntry[nEntries];
            Enumeration ee = file.entries();
            for (int i = 0; ee.hasMoreElements(); i++) { 
                entries[i] = (ZipEntry)ee.nextElement();
            }
            segment = new byte[SEGMENT_LENGTH];
            currEntry = null;
        } catch (IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR);
        }
    }

    ZipFile    file;
    ZipEntry[] entries;
    byte[]     segment;
    ZipEntry   currEntry;
}
