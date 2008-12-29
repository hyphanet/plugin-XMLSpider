package plugins.XMLSpider.org.garret.perst;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;
import java.util.zip.*;

import plugins.XMLSpider.org.garret.perst.impl.Bitmap;
import plugins.XMLSpider.org.garret.perst.impl.Bytes;
import plugins.XMLSpider.org.garret.perst.impl.Page;

/**
 * Compressed read-write database file. 
 * To work with compressed database file you should pass instance of this class in <code>Storage.open</code> method
 */
public class CompressedReadWriteFile implements IFile 
{ 
    public void write(long pageAddr, byte[] buf) {
        try {
            long pageOffs = 0;
            int pageSize = buf.length;
            if (pageAddr != 0) { 
                Assert.that(pageSize == Page.pageSize);
                Assert.that((pageAddr & (Page.pageSize-1)) == 0);
                long pagePos = pageMap.get(pageAddr);
                boolean firstUpdate = false;
                if (pagePos == 0) { 
                    int bp = (int)(pageAddr >>> (Page.pageSizeLog - 3));
                    if (bp + 8 <= pageIndexSize) {
                        byte[] posBuf = new byte[8];
                        pageIndexBuffer.position(bp);
                        pageIndexBuffer.get(posBuf, 0, 8);
                        pagePos = Bytes.unpack8(posBuf, 0);
                    }
                    firstUpdate = true;
                }
                pageSize = ((int)pagePos & (Page.pageSize-1)) + 1;
                deflater.reset();
                deflater.setInput(buf, 0, buf.length);
                deflater.finish();
                int newPageSize = deflater.deflate(compressionBuf);
                if (newPageSize == Page.pageSize) { 
                    System.arraycopy(buf, 0, compressionBuf, 0, newPageSize);
                }
                buf = compressionBuf;
                int newPageBitSize = (newPageSize + ALLOCATION_QUANTUM - 1) >>> ALLOCATION_QUANTUM_LOG;
                int oldPageBitSize = (pageSize + ALLOCATION_QUANTUM - 1) >>> ALLOCATION_QUANTUM_LOG;
                if (firstUpdate || newPageBitSize != oldPageBitSize) { 
                    if (!firstUpdate) { 
                        Bitmap.free(bitmap, pagePos >>> (Page.pageSizeLog + ALLOCATION_QUANTUM_LOG), 
                                    oldPageBitSize);
                    }
                    pageOffs = allocate(newPageBitSize);
                } else {
                    pageOffs = pagePos >>> Page.pageSizeLog;
                }
                pageSize = newPageSize;
                pageMap.put(pageAddr, (pageOffs << Page.pageSizeLog) | (pageSize-1), pagePos);
                crypt(buf, pageSize);
            }
            dataFile.seek(pageOffs);
            dataFile.write(buf, 0, pageSize);
        } catch (IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    public int read(long pageAddr, byte[] buf) {
        try {
            if (pageAddr != 0) {
                Assert.that((pageAddr & (Page.pageSize-1)) == 0);
                long pagePos = 0;
                if (pageMap != null) { 
                    pagePos = pageMap.get(pageAddr);
                }
                if (pagePos == 0) { 
                    int bp = (int)(pageAddr >>> (Page.pageSizeLog - 3));
                    if (bp + 8 <= pageIndexSize) {
                        byte[] posBuf = new byte[8];
                        pageIndexBuffer.position(bp);
                        pageIndexBuffer.get(posBuf, 0, 8);
                        pagePos = Bytes.unpack8(posBuf, 0);
                    }
                    if (pagePos == 0) {
                        //System.out.println("pagePos=0 for address " + pageAddr);
                        return 0;
                    }                        
                }                
                dataFile.seek(pagePos >>> Page.pageSizeLog);
                int size = ((int)pagePos & (Page.pageSize-1)) + 1;
                //System.out.println("Read " + size + " bytes from " + (pagePos >>> Page.pageSizeLog));
                int rc = dataFile.read(compressionBuf, 0, size);
                if (rc != size) { 
                    throw new StorageError(StorageError.FILE_ACCESS_ERROR);
                }
                crypt(compressionBuf, size);
                if (size < Page.pageSize) { 
                    inflater.reset();
                    inflater.setInput(compressionBuf, 0, size);
                    rc = inflater.inflate(buf);
                    Assert.that(rc == Page.pageSize);
                } else { 
                    System.arraycopy(compressionBuf, 0, buf, 0, rc);
                }
                return rc;
            } else { 
                dataFile.seek(0);
                return dataFile.read(buf, 0, buf.length);
            }
        } catch (Exception x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    public void sync() 
    {
        try {   
            // Flush data to the main database file
            if (!noFlush) { 
                dataFile.getFD().sync();
            }

            int txSize = pageMap.size();
            if (txSize == 0) { 
                return;
            }

            // Make sure that new page mapping is saved in transaction log
            byte[] buf = new byte[4 + 16*txSize];
            Bytes.pack4(buf, 0, txSize);
            int pos = 4;
            for (PageMap.Entry e : pageMap) { 
                Bytes.pack8(buf, pos, e.addr);
                Bytes.pack8(buf, pos + 8, e.newPos);
                pos += 16;
            }
            pageIndexLogFile.write(buf, 0, pos);
            if (!noFlush) {  
                pageIndexLogFile.getFD().sync();
            }

            // Store new page mapping in the page index file
            for (PageMap.Entry e : pageMap) { 
                setPosition(e.addr);
                Bytes.pack8(buf, 0, e.newPos);
                pageIndexBuffer.put(buf, 0, 8);
                if (e.oldPos != 0) { 
                    Bitmap.free(bitmap, e.oldPos >>> (Page.pageSizeLog + ALLOCATION_QUANTUM_LOG), 
                                ((e.oldPos & (Page.pageSize-1)) + ALLOCATION_QUANTUM) >>> ALLOCATION_QUANTUM_LOG);
                }
            }
            pageMap.clear();

            // Truncate log if necessary
            if (pageIndexLogFile.length() > pageIndexCheckpointThreshold) { 
                pageIndexBuffer.force();
                pageIndexLogFile.setLength(0);
            }
        } catch(IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    public boolean tryLock(boolean shared) 
    { 
        try { 
            lck = dataChan.tryLock(0, Long.MAX_VALUE, shared);
            return lck != null;
        } catch (IOException x) { 
            return true;
        }
    }

    public void lock(boolean shared) 
    { 
        try { 
            lck = dataChan.lock(0, Long.MAX_VALUE, shared);
        } catch (IOException x) { 
            throw new StorageError(StorageError.LOCK_FAILED, x);
        }
    }

    public void unlock() 
    { 
        try { 
            lck.release();
        } catch (IOException x) { 
            throw new StorageError(StorageError.LOCK_FAILED, x);
        }
    }

    public void close() 
    {
        try {
            dataChan.close();
            dataFile.close();

            if (pageIndexLogFile != null) {
                Assert.that(pageMap.size() == 0);
                pageIndexBuffer.force();
                pageIndexLogFile.setLength(0);
                pageIndexLogFile.close();
            }
            pageIndexChan.close();
            pageIndexFile.close();
        } catch (IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    public long length() 
    { 
        try { 
            return dataChan.size();
        } catch (IOException x) { 
            return -1;
        }
    }

    /**
     * Constructor of compressed file with default parameter values
     * @param dataFilePath path to the data file 
     */
    public CompressedReadWriteFile(String dataFilePath) 
    { 
        this(dataFilePath, null);
    } 

    /**
     * Constructor of compressed file with default parameter values
     * @param dataFilePath path to the data file 
     * @param cipherKey cipher key (if null, then no encryption is performed)
     */
    public CompressedReadWriteFile(String dataFilePath, String cipherKey) 
    { 
        this(dataFilePath, dataFilePath + ".map",  dataFilePath + ".log", 8*1024*1024, 1024*1024, 1024*1024, false, false, cipherKey);
    } 

    /**
     * Constructor of compressed file
     * @param dataFilePath path to the data file 
     * @param pageIndexFilePath path to the page index file
     * @param pageIndexLogFilePath path to the transaction log file for page index 
     * @param dataFileExtensionQuantum quantum of extending data file
     * @param pageIndexCheckpointThreshold maximal size of page index log file, after reaching this size page index is flushed and log is truncsated
     * @param pageIndexInitSize initial size of page index
     * @param readOnly whether access to the file is read-only
     * @param noFlush whether synchronous write to the disk should be performed
     * @param cipherKey cipher key (if null, then no encryption is performed)
     */
    public CompressedReadWriteFile(String dataFilePath, 
                                   String pageIndexFilePath, 
                                   String pageIndexLogFilePath, 
                                   long dataFileExtensionQuantum, 
                                   long pageIndexCheckpointThreshold, 
                                   long pageIndexInitSize,
                                   boolean readOnly,
                                   boolean noFlush,
                                   String cipherKey) 
    {
        this.pageIndexCheckpointThreshold = pageIndexCheckpointThreshold;
        this.noFlush = noFlush;

        if (cipherKey != null) { 
            setKey(cipherKey.getBytes());
        }

        try { 
            dataFile = new RandomAccessFile(dataFilePath, readOnly ? "r" : "rw");
            dataChan = dataFile.getChannel();

            pageIndexFile = new RandomAccessFile(pageIndexFilePath, readOnly ? "r" : "rw");
            pageIndexChan = pageIndexFile.getChannel();
            long size = pageIndexChan.size();
            pageIndexSize = (readOnly || size > pageIndexInitSize) ? size : pageIndexInitSize;
            pageIndexBuffer = pageIndexChan.map(readOnly ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE,
                                                0, // position
                                                pageIndexSize);
            deflater = new Deflater();
            inflater = new Inflater();
            compressionBuf = new byte[Page.pageSize];
                
            if (!readOnly) {
                long pageMapSize = pageIndexInitSize / 8;
                if (pageMapSize > MAX_PAGE_MAP_SIZE) { 
                    pageMapSize = MAX_PAGE_MAP_SIZE;
                }
                pageMap = new PageMap((int)pageMapSize);            
                pageIndexLogFile = new RandomAccessFile(pageIndexLogFilePath, "rw");
                performRecovery();

                bitmapExtensionQuantum = (int)(dataFileExtensionQuantum >>> (ALLOCATION_QUANTUM_LOG + 3));
                bitmap = new byte[(int)(dataChan.size() >>> (ALLOCATION_QUANTUM_LOG + 3)) + bitmapExtensionQuantum];
                bitmapPos = bitmapStart = Page.pageSize >>> (ALLOCATION_QUANTUM_LOG + 3);

                byte[] buf = new byte[8];      
                size = pageIndexChan.size();
                pageIndexBuffer.position(0);

                while ((size -= 8) >= 0) {                 
                    pageIndexBuffer.get(buf, 0, 8);
                    long pagePos = Bytes.unpack8(buf, 0);
                    long pageBitOffs = pagePos >>> (Page.pageSizeLog + ALLOCATION_QUANTUM_LOG);
                    long pageBitSize = ((pagePos & (Page.pageSize - 1)) + ALLOCATION_QUANTUM) >>> ALLOCATION_QUANTUM_LOG;
                    Bitmap.reserve(bitmap, pageBitOffs, pageBitSize);
                }
            }
        } catch(IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    long allocate(int bitSize)
    {
        long pos = Bitmap.allocate(bitmap, bitmapPos, bitmap.length, bitSize);
        if (pos < 0) { 
            pos = Bitmap.allocate(bitmap, bitmapStart, Bitmap.locateHoleEnd(bitmap, bitmapPos), bitSize);
            if (pos < 0) { 
                byte[] newBitmap = new byte[bitmap.length + bitmapExtensionQuantum];
                System.arraycopy(bitmap, 0, newBitmap, 0, bitmap.length);
                pos = Bitmap.allocate(newBitmap, Bitmap.locateBitmapEnd(newBitmap, bitmap.length), newBitmap.length, bitSize);
                Assert.that(pos >= 0);
                bitmap = newBitmap;       
            }     
        }
        bitmapPos = (int)((pos + bitSize) >>> 3);
        return pos << ALLOCATION_QUANTUM_LOG;
    }


    void setPosition(long addr) throws IOException
    {
        long pos = addr >>> (Page.pageSizeLog-3);
        if (pos+8 > pageIndexSize) { 
            do {                
                pageIndexSize *= 2;
            } while (pos+8 > pageIndexSize);
            pageIndexBuffer = pageIndexChan.map(FileChannel.MapMode.READ_WRITE, 0, pageIndexSize);
        }
        pageIndexBuffer.position((int)pos);
    }        

    void performRecovery() throws IOException
    {
        int rc;
        byte[] hdr = new byte[4];
        while ((rc = pageIndexLogFile.read(hdr, 0, 4)) == 4) { 
            int nPages = Bytes.unpack4(hdr, 0);
            byte[] buf = new byte[nPages*16];
            if (pageIndexLogFile.read(buf, 0, buf.length) == buf.length) { 
                for (int i = 0; i < nPages; i++) { 
                    setPosition(Bytes.unpack8(buf, i*16));
                    pageIndexBuffer.put(buf, i*16 + 8, 8);
                }
            } else { 
                break;
            }
        }
    }


    static class PageMap implements Iterable<PageMap.Entry>
    { 
        static final float LOAD_FACTOR = 0.75f;

        static final int primeNumbers[] = {
            17,             /* 0 */
            37,             /* 1 */
            79,             /* 2 */
            163,            /* 3 */
            331,            /* 4 */
            673,            /* 5 */
            1361,           /* 6 */
            2729,           /* 7 */
            5471,           /* 8 */
            10949,          /* 9 */
            21911,          /* 10 */
            43853,          /* 11 */
            87719,          /* 12 */
            175447,         /* 13 */
            350899,         /* 14 */
            701819,         /* 15 */
            1403641,        /* 16 */
            2807303,        /* 17 */
            5614657,        /* 18 */
            11229331,       /* 19 */
            22458671,       /* 20 */
            44917381,       /* 21 */
            89834777,       /* 22 */
            179669557,      /* 23 */
            359339171,      /* 24 */
            718678369,      /* 25 */
            1437356741,     /* 26 */
            2147483647      /* 27 (largest signed int prime) */
        };


        Entry table[];
        int count;
        int tableSizePrime;
        int tableSize;
        int threshold;

        public PageMap(int initialCapacity) 
        {
            for (tableSizePrime = 0; primeNumbers[tableSizePrime] < initialCapacity; tableSizePrime++);
            tableSize = primeNumbers[tableSizePrime];
            threshold = (int)(tableSize * LOAD_FACTOR);
            table = new Entry[tableSize];
        }

        public void put(long addr, long newPos, long oldPos) 
        { 
            Entry tab[] = table;
            int index = (int)((addr >>> Page.pageSizeLog) % tableSize);
            for (Entry e = tab[index]; e != null; e = e.next) {
                if (e.addr == addr) {
                    e.newPos = newPos;
                    return;
                }
            }
            if (count >= threshold) {
                // Rehash the table if the threshold is exceeded
                rehash();
                tab = table;
                index = (int)((addr >>> Page.pageSizeLog) % tableSize);
            } 

            // Creates the new entry.
            tab[index] = new Entry(addr, newPos, oldPos, tab[index]);
            count += 1;
        }
    
        public long get(long addr) 
        {
            int index = (int)((addr >>> Page.pageSizeLog) % tableSize);
            for (Entry e = table[index]; e != null; e = e.next) {
                if (e.addr == addr) {
                    return e.newPos;
                }
            }
            return 0;
        }

        public void clear() 
        {
            Entry tab[] = table;
            int size = tableSize;
            for (int i = 0; i < size; i++) { 
                tab[i] = null;
            }
            count = 0;
        }

        void rehash() 
        {
            int oldCapacity = tableSize;
            int newCapacity = tableSize = primeNumbers[++tableSizePrime];
            Entry oldMap[] = table;
            Entry newMap[] = new Entry[newCapacity];

            threshold = (int)(newCapacity * LOAD_FACTOR);
            table = newMap;
            tableSize = newCapacity;

            for (int i = 0; i < oldCapacity; i++) {
                for (Entry old = oldMap[i]; old != null; ) {
                    Entry e = old;
                    old = old.next;
                    int index = (int)((e.addr >>> Page.pageSizeLog) % newCapacity);
                    e.next = newMap[index];
                    newMap[index] = e;
                }
            }
        }

        public Iterator<Entry> iterator() 
        { 
            return new PageMapIterator();
        }

        public int size() 
        { 
            return count;
        }

        class PageMapIterator implements Iterator<Entry> 
        {
            PageMapIterator() { 
                moveForward();
            }

            public boolean hasNext() { 
                return curr != null;
            }
            
            public Entry next() {
                Entry e = curr;
                if (e == null) { 
                    throw new NoSuchElementException();
                }
                moveForward();
                return e;
            }

            public void remove() { 
                throw new UnsupportedOperationException();
            }

            private void moveForward() { 
                if (curr != null) { 
                    curr = curr.next;
                }
                while (curr == null && i < tableSize) { 
                    curr = table[i++];
                }
            }

            Entry curr;
            int i;
        }

        static class Entry 
        { 
            Entry next;
            long  addr;
            long  newPos;
            long  oldPos;
        
            Entry(long addr, long newPos, long oldPos, Entry chain) 
            { 
                next = chain;
                this.addr = addr;
                this.newPos = newPos;
                this.oldPos = oldPos;
            }
        }
    }

    private void setKey(byte[] key)
    {
        initState = new byte[256];
        state = new byte[256];

	for (int counter = 0; counter < 256; ++counter) { 
	    initState[counter] = (byte)counter;
        }
	int index1 = 0;
	int index2 = 0;
	for (int counter = 0; counter < 256; ++counter) {
	    index2 = (key[index1] + initState[counter] + index2) & 0xff;
	    byte temp = initState[counter];
	    initState[counter] = initState[index2];
	    initState[index2] = temp;
	    index1 = (index1 + 1) % key.length;
        }
    }

    private final void crypt(byte[] buf, int len)
    {
        byte[] state = this.state;
        if (state != null) { 
            int x = 0, y = 0;
            System.arraycopy(initState, 0, state, 0, state.length);
            for (int i = 0; i < len; i++) {
                x = (x + 1) & 0xff;
                y = (y + state[x]) & 0xff;
                byte temp = state[x];
                state[x] = state[y];
                state[y] = temp;
                int nextState = (state[x] + state[y]) & 0xff;
                buf[i] ^= state[nextState];
            }
        }
    }

    static final int ALLOCATION_QUANTUM_LOG = 9;
    static final int ALLOCATION_QUANTUM = 1 << ALLOCATION_QUANTUM_LOG;
    static final long MAX_PAGE_MAP_SIZE = 1000000;
    

    byte[]           bitmap;
    int              bitmapPos;
    int              bitmapStart;
    int              bitmapExtensionQuantum;
    long             pageIndexSize;
    long             pageIndexCheckpointThreshold;

    Deflater         deflater;
    Inflater         inflater;
    byte[]           compressionBuf;

    RandomAccessFile dataFile;
    RandomAccessFile pageIndexFile;
    RandomAccessFile pageIndexLogFile;

    FileChannel      dataChan;
    FileChannel      pageIndexChan;

    MappedByteBuffer pageIndexBuffer;

    PageMap          pageMap;

    boolean          noFlush;
    FileLock         lck; 

    byte[]           initState;
    byte[]           state;
}
