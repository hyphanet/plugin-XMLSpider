package plugins.XMLSpider.org.garret.perst.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import plugins.XMLSpider.org.garret.perst.Assert;
import plugins.XMLSpider.org.garret.perst.BitIndex;
import plugins.XMLSpider.org.garret.perst.Blob;
import plugins.XMLSpider.org.garret.perst.CustomAllocator;
import plugins.XMLSpider.org.garret.perst.CustomSerializer;
import plugins.XMLSpider.org.garret.perst.FieldIndex;
import plugins.XMLSpider.org.garret.perst.IFile;
import plugins.XMLSpider.org.garret.perst.ILoadable;
import plugins.XMLSpider.org.garret.perst.INamedClassLoader;
import plugins.XMLSpider.org.garret.perst.IPersistent;
import plugins.XMLSpider.org.garret.perst.IPersistentList;
import plugins.XMLSpider.org.garret.perst.IPersistentMap;
import plugins.XMLSpider.org.garret.perst.IPersistentSet;
import plugins.XMLSpider.org.garret.perst.IResource;
import plugins.XMLSpider.org.garret.perst.IStoreable;
import plugins.XMLSpider.org.garret.perst.IValue;
import plugins.XMLSpider.org.garret.perst.Index;
import plugins.XMLSpider.org.garret.perst.Link;
import plugins.XMLSpider.org.garret.perst.MemoryUsage;
import plugins.XMLSpider.org.garret.perst.MultidimensionalComparator;
import plugins.XMLSpider.org.garret.perst.MultidimensionalIndex;
import plugins.XMLSpider.org.garret.perst.PatriciaTrie;
import plugins.XMLSpider.org.garret.perst.Persistent;
import plugins.XMLSpider.org.garret.perst.PersistentComparator;
import plugins.XMLSpider.org.garret.perst.PersistentIterator;
import plugins.XMLSpider.org.garret.perst.PersistentResource;
import plugins.XMLSpider.org.garret.perst.PerstInputStream;
import plugins.XMLSpider.org.garret.perst.Query;
import plugins.XMLSpider.org.garret.perst.Relation;
import plugins.XMLSpider.org.garret.perst.SelfSerializable;
import plugins.XMLSpider.org.garret.perst.SortedCollection;
import plugins.XMLSpider.org.garret.perst.SpatialIndex;
import plugins.XMLSpider.org.garret.perst.SpatialIndexR2;
import plugins.XMLSpider.org.garret.perst.Storage;
import plugins.XMLSpider.org.garret.perst.StorageError;
import plugins.XMLSpider.org.garret.perst.StorageListener;
import plugins.XMLSpider.org.garret.perst.TimeSeries;
import plugins.XMLSpider.org.garret.perst.XMLImportException;
import plugins.XMLSpider.org.garret.perst.fulltext.FullTextIndex;
import plugins.XMLSpider.org.garret.perst.fulltext.FullTextSearchHelper;

public class StorageImpl implements Storage { 
    /**
     * Initialial database index size - increasing it reduce number of index reallocation but increase
     * initial database size. Should be set before openning connection.
     */
    static final int  dbDefaultInitIndexSize = 1024;

    /**
     * Initial capacity of object hash
     */
    static final int  dbDefaultObjectCacheInitSize = 1319;

    /**
     * Database extension quantum. Memory is allocate by scanning bitmap. If there is no
     * large enough hole, then database is extended by the value of dbDefaultExtensionQuantum 
     * This parameter should not be smaller than dbFirstUserId
     */
    static final long dbDefaultExtensionQuantum = 1024*1024;

    static final long dbDefaultPagePoolLruLimit = 1L << 60;

    static final int  dbDatabaseOidBits = 31;          // up to 2 milliards of objects
    static final int  dbDatabaseOffsetBits = 32;       // up to 4 gigabyte
    static final int  dbLargeDatabaseOffsetBits = 40;  // up to 1 terabyte
    static final int  dbMaxObjectOid = (1 << dbDatabaseOidBits) - 1;

    static final int  dbAllocationQuantumBits = 5;
    static final int  dbAllocationQuantum = 1 << dbAllocationQuantumBits;
    static final int  dbBitmapSegmentBits = Page.pageSizeLog + 3 + dbAllocationQuantumBits;
    static final int  dbBitmapSegmentSize = 1 << dbBitmapSegmentBits;
    static final int  dbBitmapPages = 1 << (dbDatabaseOffsetBits-dbBitmapSegmentBits);
    static final int  dbLargeBitmapPages = 1 << (dbLargeDatabaseOffsetBits-dbBitmapSegmentBits);
    static final int  dbHandlesPerPageBits = Page.pageSizeLog - 3;
    static final int  dbHandlesPerPage = 1 << dbHandlesPerPageBits;
    static final int  dbDirtyPageBitmapSize = 1 << (dbDatabaseOidBits-dbHandlesPerPageBits-3);

    static final int  dbInvalidId   = 0;
    static final int  dbBitmapId    = 1;
    static final int  dbFirstUserId = dbBitmapId + dbBitmapPages;
    
    static final int  dbPageObjectFlag = 1;
    static final int  dbModifiedFlag   = 2;
    static final int  dbFreeHandleFlag = 4;
    static final int  dbFlagsMask      = 7;
    static final int  dbFlagsBits      = 3;

    /**
     * Current version of database format. 0 means that database is not initilized.
     * Used to provide backward compatibility of Perst releases.
     */
    static final byte dbDatabaseFormatVersion = (byte)3;

    final int getBitmapPageId(int i) { 
        return i < dbBitmapPages ? dbBitmapId + i : header.root[1-currIndex].bitmapExtent + i - bitmapExtentBase;
    }

    final long getPos(int oid) { 
        synchronized (objectCache) {
            if (oid == 0 || oid >= currIndexSize) { 
                throw new StorageError(StorageError.INVALID_OID);
            }
            Page pg = pool.getPage(header.root[1-currIndex].index 
                                   + ((long)(oid >>> dbHandlesPerPageBits) << Page.pageSizeLog));
            long pos = Bytes.unpack8(pg.data, (oid & (dbHandlesPerPage-1)) << 3);
            pool.unfix(pg);
            return pos;
        }
    }
    
    final void setPos(int oid, long pos) { 
        synchronized (objectCache) {
            dirtyPagesMap[oid >>> (dbHandlesPerPageBits+5)] 
                |= 1 << ((oid >>> dbHandlesPerPageBits) & 31);
            Page pg = pool.putPage(header.root[1-currIndex].index 
                                   + ((long)(oid >>> dbHandlesPerPageBits) << Page.pageSizeLog));
            Bytes.pack8(pg.data, (oid & (dbHandlesPerPage-1)) << 3, pos);
            pool.unfix(pg);
        }
    }

    final byte[] get(int oid) { 
        long pos = getPos(oid);
        if ((pos & (dbFreeHandleFlag|dbPageObjectFlag)) != 0) { 
            throw new StorageError(StorageError.INVALID_OID);
        }
        return pool.get(pos & ~dbFlagsMask);
    }
    
    final Page getPage(int oid) {  
        long pos = getPos(oid);
        if ((pos & (dbFreeHandleFlag|dbPageObjectFlag)) != dbPageObjectFlag) { 
            throw new StorageError(StorageError.DELETED_OBJECT);
        }
        return pool.getPage(pos & ~dbFlagsMask);
    }

    final Page putPage(int oid) {  
        synchronized (objectCache) {
            long pos = getPos(oid);
            if ((pos & (dbFreeHandleFlag|dbPageObjectFlag)) != dbPageObjectFlag) { 
                throw new StorageError(StorageError.DELETED_OBJECT);
            }
            if ((pos & dbModifiedFlag) == 0) { 
                dirtyPagesMap[oid >>> (dbHandlesPerPageBits+5)] 
                    |= 1 << ((oid >>> dbHandlesPerPageBits) & 31);
                allocate(Page.pageSize, oid);
                cloneBitmap(pos & ~dbFlagsMask, Page.pageSize);
                pos = getPos(oid);
            }
            modified = true;
            return pool.putPage(pos & ~dbFlagsMask);
        }
    }


    int allocatePage() { 
        int oid = allocateId();
        setPos(oid, allocate(Page.pageSize, 0) | dbPageObjectFlag | dbModifiedFlag);
        return oid;
    }

    public/*protected*/ synchronized void deallocateObject(Object obj) 
    {
        synchronized (objectCache) {
            if (getOid(obj) == 0) { 
                return;
            }
            if (useSerializableTransactions) { 
                ThreadTransactionContext ctx = getTransactionContext();
                if (ctx.nested != 0) { // serializable transaction
                    ctx.deleted.add(obj);  
                    return;
                }
            }
            deallocateObject0(obj);
        }
    }

    public void throwObject(Object obj) 
    {
        objectCache.remove(getOid(obj));
    }

    private void deallocateObject0(Object obj)
    {
        int oid = getOid(obj);
        long pos = getPos(oid);
        objectCache.remove(oid);
        int offs = (int)pos & (Page.pageSize-1);
        if ((offs & (dbFreeHandleFlag|dbPageObjectFlag)) != 0) { 
            throw new StorageError(StorageError.DELETED_OBJECT);
        }
        Page pg = pool.getPage(pos - offs);
        offs &= ~dbFlagsMask;
        int size = ObjectHeader.getSize(pg.data, offs);
        pool.unfix(pg);
        freeId(oid);
        CustomAllocator allocator = (customAllocatorMap != null) 
            ? getCustomAllocator(obj.getClass()) : null;
        if (allocator != null) { 
            allocator.free(pos & ~dbFlagsMask, size);
        } else { 
            if ((pos & dbModifiedFlag) != 0) { 
                free(pos & ~dbFlagsMask, size);
            } else { 
                cloneBitmap(pos, size);
            }
        }
        unassignOid(obj);
    }
    

    final void freePage(int oid) {
        long pos = getPos(oid);
        Assert.that((pos & (dbFreeHandleFlag|dbPageObjectFlag)) == dbPageObjectFlag);
        if ((pos & dbModifiedFlag) != 0) { 
            free(pos & ~dbFlagsMask, Page.pageSize);
        } else { 
            cloneBitmap(pos & ~dbFlagsMask, Page.pageSize);
        } 
        freeId(oid);
    }

    int allocateId() {
        synchronized (objectCache) { 
            int oid;
            int curr = 1-currIndex;
            setDirty();
            if ((oid = header.root[curr].freeList) != 0) { 
                header.root[curr].freeList = (int)(getPos(oid) >> dbFlagsBits);
                Assert.that(header.root[curr].freeList >= 0);
                dirtyPagesMap[oid >>> (dbHandlesPerPageBits+5)] 
                    |= 1 << ((oid >>> dbHandlesPerPageBits) & 31);
                return oid;
            }

            if (currIndexSize > dbMaxObjectOid) { 
                throw new StorageError(StorageError.TOO_MUCH_OBJECTS);
            }
            if (currIndexSize >= header.root[curr].indexSize) {
                int oldIndexSize = header.root[curr].indexSize;
                int newIndexSize = oldIndexSize << 1;
                if (newIndexSize < oldIndexSize) { 
                    newIndexSize = Integer.MAX_VALUE & ~(dbHandlesPerPage-1);
                    if (newIndexSize <= oldIndexSize) { 
                        throw new StorageError(StorageError.NOT_ENOUGH_SPACE);
                    }
                }
                long newIndex = allocate(newIndexSize*8L, 0);
                if (currIndexSize >= header.root[curr].indexSize) {
                    long oldIndex = header.root[curr].index;
                    pool.copy(newIndex, oldIndex, currIndexSize*8L);
                    header.root[curr].index = newIndex;
                    header.root[curr].indexSize = newIndexSize;
                    free(oldIndex, oldIndexSize*8L);
                } else { 
                    // index was already reallocated
                    free(newIndex, newIndexSize*8L);
                }
            }
            oid = currIndexSize;
            header.root[curr].indexUsed = ++currIndexSize;
            return oid;
        }
    }
    
    void freeId(int oid)
    {
        synchronized (objectCache) { 
            setPos(oid, ((long)(header.root[1-currIndex].freeList) << dbFlagsBits)
                   | dbFreeHandleFlag);
            header.root[1-currIndex].freeList = oid;
        }
    }
    
    static final int pageBits = Page.pageSize*8;
    static final int inc = Page.pageSize/dbAllocationQuantum/8;

    static final void memset(Page pg, int offs, int pattern, int len) { 
        byte[] arr = pg.data;
        byte pat = (byte)pattern;
        while (--len >= 0) { 
            arr[offs++] = pat;
        }
    }

    final void extend(long size)
    {
        if (size > header.root[1-currIndex].size) { 
            header.root[1-currIndex].size = size;
        }
    }

    public long getUsedSize() { 
        return usedSize;
    }

    public long getDatabaseSize() { 
        return header.root[1-currIndex].size;
    }

    static class Location { 
        long     pos;
        long     size;
        Location next;
    }

    final boolean wasReserved(long pos, long size) 
    {
        for (Location location = reservedChain; location != null; location = location.next) { 
            if ((pos >= location.pos && pos - location.pos < location.size) 
                || (pos <= location.pos && location.pos - pos < size)) 
            {
                return true;
            }
        }
        return false;
    }

    final void reserveLocation(long pos, long size)
    {
        Location location = new Location();
        location.pos = pos;
        location.size = size;
        location.next = reservedChain;
        reservedChain = location;
    }

    final void commitLocation()
    {
        reservedChain = reservedChain.next;
    }


    final void setDirty() 
    {
        modified = true;
        if (!header.dirty) { 
            header.dirty = true;
            Page pg = pool.putPage(0);
            header.pack(pg.data);
            pool.flush();
            pool.unfix(pg);
        }
    }

    protected boolean isDirty() { 
        return header.dirty;
    }

    final Page putBitmapPage(int i) { 
        return putPage(getBitmapPageId(i));
    }

    final Page getBitmapPage(int i) { 
        return getPage(getBitmapPageId(i));
    }


    final long allocate(long size, int oid)
    {
        synchronized (objectCache) {
            setDirty();
            size = (size + dbAllocationQuantum-1) & ~(dbAllocationQuantum-1);
            Assert.that(size != 0);
            allocatedDelta += size;
            if (allocatedDelta > gcThreshold) {
                gc0();
            }
            int  objBitSize = (int)(size >> dbAllocationQuantumBits);
            Assert.that(objBitSize == (size >> dbAllocationQuantumBits));
            long pos;    
            int  holeBitSize = 0;
            int  alignment = (int)size & (Page.pageSize-1);
            int  offs, firstPage, lastPage, i, j;
            int  holeBeforeFreePage  = 0;
            int  freeBitmapPage = 0;
            int  curr = 1 - currIndex;
            Page pg;


            lastPage = header.root[curr].bitmapEnd - dbBitmapId;
            usedSize += size;

            if (alignment == 0) { 
                firstPage = currPBitmapPage;
                offs = (currPBitmapOffs+inc-1) & ~(inc-1);
            } else { 
                firstPage = currRBitmapPage;
                offs = currRBitmapOffs;
            }
        
            while (true) { 
                if (alignment == 0) { 
                    // allocate page object 
                    for (i = firstPage; i < lastPage; i++){
                        int spaceNeeded = objBitSize - holeBitSize < pageBits 
                            ? objBitSize - holeBitSize : pageBits;
                        if (bitmapPageAvailableSpace[i] <= spaceNeeded) {
                            holeBitSize = 0;
                            offs = 0;
                            continue;
                        }
                        pg = getBitmapPage(i);
                        int startOffs = offs;   
                        while (offs < Page.pageSize) { 
                            if (pg.data[offs++] != 0) { 
                                offs = (offs + inc - 1) & ~(inc-1);
                                holeBitSize = 0;
                            } else if ((holeBitSize += 8) == objBitSize) { 
                                pos = (((long)i*Page.pageSize + offs)*8 - holeBitSize) 
                                    << dbAllocationQuantumBits;
                                if (wasReserved(pos, size)) { 
                                    startOffs = offs = (offs + inc - 1) & ~(inc-1);
                                    holeBitSize = 0;
                                    continue;
                                }       
                                reserveLocation(pos, size);
                                currPBitmapPage = i;
                                currPBitmapOffs = offs;
                                extend(pos + size);
                                if (oid != 0) { 
                                    long prev = getPos(oid);
                                    int marker = (int)prev & dbFlagsMask;
                                    pool.copy(pos, prev - marker, size);
                                    setPos(oid, pos | marker | dbModifiedFlag);
                                }
                                pool.unfix(pg);
                                pg = putBitmapPage(i);
                                int holeBytes = holeBitSize >> 3;
                                if (holeBytes > offs) { 
                                    memset(pg, 0, 0xFF, offs);
                                    holeBytes -= offs;
                                    pool.unfix(pg);
                                    pg = putBitmapPage(--i);
                                    offs = Page.pageSize;
                                }
                                while (holeBytes > Page.pageSize) { 
                                    memset(pg, 0, 0xFF, Page.pageSize);
                                    holeBytes -= Page.pageSize;
                                    bitmapPageAvailableSpace[i] = 0;
                                    pool.unfix(pg);
                                    pg = putBitmapPage(--i);
                                }
                                memset(pg, offs-holeBytes, 0xFF, holeBytes);
                                commitLocation();
                                pool.unfix(pg);
                                return pos;
                            }
                        }
                        if (startOffs == 0 && holeBitSize == 0
                            && spaceNeeded < bitmapPageAvailableSpace[i]) 
                        { 
                            bitmapPageAvailableSpace[i] = spaceNeeded;
                        }
                        offs = 0;
                        pool.unfix(pg);
                    }
                } else { 
                    for (i = firstPage; i < lastPage; i++){
                        int spaceNeeded = objBitSize - holeBitSize < pageBits 
                            ? objBitSize - holeBitSize : pageBits;
                        if (bitmapPageAvailableSpace[i] <= spaceNeeded) {
                            holeBitSize = 0;
                            offs = 0;
                            continue;
                        }
                        pg = getBitmapPage(i);
                        int startOffs = offs;
                        while (offs < Page.pageSize) { 
                            int mask = pg.data[offs] & 0xFF; 
                            if (holeBitSize + Bitmap.firstHoleSize[mask] >= objBitSize) { 
                                pos = (((long)i*Page.pageSize + offs)*8 
                                       - holeBitSize) << dbAllocationQuantumBits;
                                if (wasReserved(pos, size)) {                       
                                    startOffs = offs += 1;
                                    holeBitSize = 0;
                                    continue;
                                }       
                                reserveLocation(pos, size);
                                currRBitmapPage = i;
                                currRBitmapOffs = offs;
                                extend(pos + size);
                                if (oid != 0) { 
                                    long prev = getPos(oid);
                                    int marker = (int)prev & dbFlagsMask;
                                    pool.copy(pos, prev - marker, size);
                                    setPos(oid, pos | marker | dbModifiedFlag);
                                }
                                pool.unfix(pg);
                                pg = putBitmapPage(i);
                                pg.data[offs] |= (byte)((1 << (objBitSize - holeBitSize)) - 1); 
                                if (holeBitSize != 0) { 
                                    if (holeBitSize > offs*8) { 
                                        memset(pg, 0, 0xFF, offs);
                                        holeBitSize -= offs*8;
                                        pool.unfix(pg);
                                        pg = putBitmapPage(--i);
                                        offs = Page.pageSize;
                                    }
                                    while (holeBitSize > pageBits) { 
                                        memset(pg, 0, 0xFF, Page.pageSize);
                                        holeBitSize -= pageBits;
                                        bitmapPageAvailableSpace[i] = 0;
                                        pool.unfix(pg);
                                        pg = putBitmapPage(--i);
                                    }
                                    while ((holeBitSize -= 8) > 0) { 
                                        pg.data[--offs] = (byte)0xFF; 
                                    }
                                    pg.data[offs-1] |= (byte)~((1 << -holeBitSize) - 1);
                                }
                                pool.unfix(pg);
                                commitLocation();
                                return pos;
                            } else if (Bitmap.maxHoleSize[mask] >= objBitSize) { 
                                int holeBitOffset = Bitmap.maxHoleOffset[mask];
                                pos = (((long)i*Page.pageSize + offs)*8 + holeBitOffset) << dbAllocationQuantumBits;
                                if (wasReserved(pos, size)) { 
                                    startOffs = offs += 1;
                                    holeBitSize = 0;
                                    continue;
                                }       
                                reserveLocation(pos, size);
                                currRBitmapPage = i;
                                currRBitmapOffs = offs;
                                extend(pos + size);
                                if (oid != 0) { 
                                    long prev = getPos(oid);
                                    int marker = (int)prev & dbFlagsMask;
                                    pool.copy(pos, prev - marker, size);
                                    setPos(oid, pos | marker | dbModifiedFlag);
                                }
                                pool.unfix(pg);
                                pg = putBitmapPage(i);
                                pg.data[offs] |= (byte)(((1<<objBitSize) - 1) << holeBitOffset);
                                pool.unfix(pg);
                                commitLocation();
                                return pos;
                            }
                            offs += 1;
                            if (Bitmap.lastHoleSize[mask] == 8) { 
                                holeBitSize += 8;
                            } else { 
                                holeBitSize = Bitmap.lastHoleSize[mask];
                            }
                        }
                        if (startOffs == 0 && holeBitSize == 0
                            && spaceNeeded < bitmapPageAvailableSpace[i]) 
                        {
                            bitmapPageAvailableSpace[i] = spaceNeeded;
                        }
                        offs = 0;
                        pool.unfix(pg);
                    }
                }
                if (firstPage == 0) { 
                    if (freeBitmapPage > i) { 
                        i = freeBitmapPage;
                        holeBitSize = holeBeforeFreePage;
                    }
                    objBitSize -= holeBitSize;
                    // number of bits reserved for the object and aligned on page boundary
                    int skip = (objBitSize + Page.pageSize/dbAllocationQuantum - 1) 
                        & ~(Page.pageSize/dbAllocationQuantum - 1);
                    // page aligned position after allocated object
                    pos = ((long)i << dbBitmapSegmentBits) + ((long)skip << dbAllocationQuantumBits);

                    long extension = (size > extensionQuantum) ? size : extensionQuantum;
                    int oldIndexSize = 0;
                    long oldIndex = 0;
                    int morePages = (int)((extension + Page.pageSize*(dbAllocationQuantum*8-1) - 1)
                                          / (Page.pageSize*(dbAllocationQuantum*8-1)));
                    if (i + morePages > dbLargeBitmapPages) { 
                        throw new StorageError(StorageError.NOT_ENOUGH_SPACE);
                    }
                    if (i <= dbBitmapPages && i + morePages > dbBitmapPages) {   
                        // We are out of space mapped by memory default allocation bitmap
                        oldIndexSize = header.root[curr].indexSize;
                        if (oldIndexSize <= currIndexSize + dbLargeBitmapPages - dbBitmapPages) {
                            int newIndexSize = oldIndexSize;
                            oldIndex = header.root[curr].index;
                            do { 
                                newIndexSize <<= 1;                    
                                if (newIndexSize < 0) { 
                                    newIndexSize = Integer.MAX_VALUE & ~(dbHandlesPerPage-1);
                                    if (newIndexSize < currIndexSize + dbLargeBitmapPages - dbBitmapPages) {
                                        throw new StorageError(StorageError.NOT_ENOUGH_SPACE);
                                    }
                                    break;
                                }
                            } while (newIndexSize <= currIndexSize + dbLargeBitmapPages - dbBitmapPages);

                            if (size + newIndexSize*8L > extensionQuantum) { 
                                extension = size + newIndexSize*8L;
                                morePages = (int)((extension + Page.pageSize*(dbAllocationQuantum*8-1) - 1)
                                                  / (Page.pageSize*(dbAllocationQuantum*8-1)));
                            }
                            extend(pos + (long)morePages*Page.pageSize + newIndexSize*8L);
                            long newIndex = pos + (long)morePages*Page.pageSize;                        
                            fillBitmap(pos + (skip>>3) + (long)morePages * (Page.pageSize/dbAllocationQuantum/8),
                                       newIndexSize >>> dbAllocationQuantumBits);
                            pool.copy(newIndex, oldIndex, oldIndexSize*8L);
                            header.root[curr].index = newIndex;
                            header.root[curr].indexSize = newIndexSize;
                        }
                        int[] newBitmapPageAvailableSpace = new int[dbLargeBitmapPages];
                        System.arraycopy(bitmapPageAvailableSpace, 0, newBitmapPageAvailableSpace, 0, dbBitmapPages);
                        for (j = dbBitmapPages; j < dbLargeBitmapPages; j++) { 
                            newBitmapPageAvailableSpace[j] = Integer.MAX_VALUE;
                        }
                        bitmapPageAvailableSpace = newBitmapPageAvailableSpace;
                        
                        for (j = 0; j < dbLargeBitmapPages - dbBitmapPages; j++) { 
                            setPos(currIndexSize + j, dbFreeHandleFlag);
                        }

                        header.root[curr].bitmapExtent = currIndexSize;
                        header.root[curr].indexUsed = currIndexSize += dbLargeBitmapPages - dbBitmapPages;
                    }
                    extend(pos + (long)morePages*Page.pageSize);
                    long adr = pos;
                    int len = objBitSize >> 3;
                    // fill bitmap pages used for allocation of object space with 0xFF 
                    while (len >= Page.pageSize) { 
                        pg = pool.putPage(adr);
                        memset(pg, 0, 0xFF, Page.pageSize);
                        pool.unfix(pg);
                        adr += Page.pageSize;
                        len -= Page.pageSize;
                    }
                    // fill part of last page responsible for allocation of object space
                    pg = pool.putPage(adr);
                    memset(pg, 0, 0xFF, len);
                    pg.data[len] = (byte)((1 << (objBitSize&7))-1);
                    pool.unfix(pg);

                    // mark in bitmap newly allocated object
                    fillBitmap(pos + (skip>>3), morePages * (Page.pageSize/dbAllocationQuantum/8));
                    
                    j = i;
                    while (--morePages >= 0) { 
                        setPos(getBitmapPageId(j++), pos | dbPageObjectFlag | dbModifiedFlag);
                        pos += Page.pageSize;
                    }
                    header.root[curr].bitmapEnd = j + dbBitmapId;
                    j = i + objBitSize / pageBits; 
                    if (alignment != 0) { 
                        currRBitmapPage = j;
                        currRBitmapOffs = 0;
                    } else { 
                        currPBitmapPage = j;
                        currPBitmapOffs = 0;
                    }
                    while (j > i) { 
                        bitmapPageAvailableSpace[--j] = 0;
                    }
                
                    pos = ((long)i*Page.pageSize*8 - holeBitSize)  << dbAllocationQuantumBits;
                    if (oid != 0) { 
                        long prev = getPos(oid);
                        int marker = (int)prev & dbFlagsMask;
                        pool.copy(pos, prev - marker, size);
                        setPos(oid, pos | marker | dbModifiedFlag);
                    }
                
                    if (holeBitSize != 0) { 
                        reserveLocation(pos, size);
                        while (holeBitSize > pageBits) { 
                            holeBitSize -= pageBits;
                            pg = putBitmapPage(--i);
                            memset(pg, 0, 0xFF, Page.pageSize);
                            bitmapPageAvailableSpace[i] = 0;
                            pool.unfix(pg);
                        }
                        pg = putBitmapPage(--i);
                        offs = Page.pageSize;
                        while ((holeBitSize -= 8) > 0) { 
                            pg.data[--offs] = (byte)0xFF; 
                        }
                        pg.data[offs-1] |= (byte)~((1 << -holeBitSize) - 1);
                        pool.unfix(pg);
                        commitLocation();
                    }
                    if (oldIndex != 0) { 
                        free(oldIndex, oldIndexSize*8L);
                    }
                    return pos;
                } 
                if (gcThreshold != Long.MAX_VALUE && !gcDone && !gcActive) {
                    allocatedDelta -= size;
                    usedSize -= size;
                    gc0();
                    currRBitmapPage = currPBitmapPage = 0;
                    currRBitmapOffs = currPBitmapOffs = 0;                
                    return allocate(size, oid);
                }
                freeBitmapPage = i;
                holeBeforeFreePage = holeBitSize;
                holeBitSize = 0;
                lastPage = firstPage + 1;
                firstPage = 0;
                offs = 0;
            }
        }
    } 


    final void fillBitmap(long adr, int len) { 
        while (true) { 
            int off = (int)adr & (Page.pageSize-1);
            Page pg = pool.putPage(adr - off);
            if (Page.pageSize - off >= len) { 
                memset(pg, off, 0xFF, len);
                pool.unfix(pg);
                break;
            } else { 
                memset(pg, off, 0xFF, Page.pageSize - off);
                pool.unfix(pg);
                adr += Page.pageSize - off;
                len -= Page.pageSize - off;
            }
        }
    }

    final void free(long pos, long size)
    {
        synchronized (objectCache) {
            Assert.that(pos != 0 && (pos & (dbAllocationQuantum-1)) == 0);
            long quantNo = pos >>> dbAllocationQuantumBits;
            int  objBitSize = (int)((size+dbAllocationQuantum-1) >>> dbAllocationQuantumBits);
            int  pageId = (int)(quantNo >>> (Page.pageSizeLog+3));
            int  offs = (int)(quantNo & (Page.pageSize*8-1)) >> 3;
            Page pg = putBitmapPage(pageId);
            int  bitOffs = (int)quantNo & 7;

            allocatedDelta -= (long)objBitSize << dbAllocationQuantumBits;
            usedSize -= (long)objBitSize << dbAllocationQuantumBits;

            if ((pos & (Page.pageSize-1)) == 0 && size >= Page.pageSize) { 
                if (pageId == currPBitmapPage && offs < currPBitmapOffs) { 
                    currPBitmapOffs = offs;
                }
            }
            if (pageId == currRBitmapPage && offs < currRBitmapOffs) { 
                currRBitmapOffs = offs;
            }
            bitmapPageAvailableSpace[pageId] = Integer.MAX_VALUE;
        
            if (objBitSize > 8 - bitOffs) { 
                objBitSize -= 8 - bitOffs;
                pg.data[offs++] &= (1 << bitOffs) - 1;
                while (objBitSize + offs*8 > Page.pageSize*8) { 
                    memset(pg, offs, 0, Page.pageSize - offs);
                    pool.unfix(pg);
                    pg = putBitmapPage(++pageId);
                    bitmapPageAvailableSpace[pageId] = Integer.MAX_VALUE;
                    objBitSize -= (Page.pageSize - offs)*8;
                    offs = 0;
                }
                while ((objBitSize -= 8) > 0) { 
                    pg.data[offs++] = (byte)0;
                }
                pg.data[offs] &= (byte)~((1 << (objBitSize + 8)) - 1);
            } else { 
                pg.data[offs] &= (byte)~(((1 << objBitSize) - 1) << bitOffs); 
            }
            pool.unfix(pg);
        }
    }

    static class CloneNode {
        long      pos;
        CloneNode next;

        CloneNode(long pos, CloneNode list) { 
            this.pos = pos;
            this.next = list;
        }
    }

    final void cloneBitmap(long pos, long size)
    {
        synchronized (objectCache) {
            if (insideCloneBitmap) { 
                Assert.that(size == Page.pageSize);
                cloneList = new CloneNode(pos, cloneList);
            } else { 
                insideCloneBitmap = true;
                while (true) { 
                    long quantNo = pos >>> dbAllocationQuantumBits;
                    int  objBitSize = (int)((size+dbAllocationQuantum-1) >>> dbAllocationQuantumBits);
                    int  pageId = (int)(quantNo >>> (Page.pageSizeLog + 3));
                    int  offs = (int)(quantNo & (Page.pageSize*8-1)) >> 3;
                    int  bitOffs = (int)quantNo & 7;
                    int  oid = getBitmapPageId(pageId);
                    pos = getPos(oid);
                    if ((pos & dbModifiedFlag) == 0) { 
                        dirtyPagesMap[oid >>> (dbHandlesPerPageBits+5)] 
                            |= 1 << ((oid >>> dbHandlesPerPageBits) & 31);
                        allocate(Page.pageSize, oid);
                        cloneBitmap(pos & ~dbFlagsMask, Page.pageSize);
                    }
                    
                    if (objBitSize > 8 - bitOffs) { 
                        objBitSize -= 8 - bitOffs;
                        offs += 1;
                        while (objBitSize + offs*8 > Page.pageSize*8) { 
                            oid = getBitmapPageId(++pageId);
                            pos = getPos(oid);
                            if ((pos & dbModifiedFlag) == 0) { 
                                dirtyPagesMap[oid >>> (dbHandlesPerPageBits+5)] 
                                    |= 1 << ((oid >>> dbHandlesPerPageBits) & 31);
                                allocate(Page.pageSize, oid);
                                cloneBitmap(pos & ~dbFlagsMask, Page.pageSize);
                            }
                            objBitSize -= (Page.pageSize - offs)*8;
                            offs = 0;
                        }
                    }
                    if (cloneList == null) { 
                        break;
                    }
                    pos = cloneList.pos;
                    size = Page.pageSize;
                    cloneList = cloneList.next;
                }
                insideCloneBitmap = false;
            }
        }
    }

    public void open(String filePath) {
        open(filePath, DEFAULT_PAGE_POOL_SIZE);
    }

    public void open(IFile file) {
        open(file, DEFAULT_PAGE_POOL_SIZE);
    }

    public synchronized void open(String filePath, long pagePoolSize) {
        IFile file = filePath.startsWith("@") 
            ? (IFile)new MultiFile(filePath.substring(1), readOnly, noFlush)
            : (IFile)new OSFile(filePath, readOnly, noFlush);      
        try {
            open(file, pagePoolSize);
        } catch (StorageError ex) {
            file.close();            
            throw ex;
        }
    }

    public synchronized void open(String filePath, long pagePoolSize, String cryptKey) {
        Rc4File file = new Rc4File(filePath, readOnly, noFlush, cryptKey);      
        try {
            open(file, pagePoolSize);
        } catch (StorageError ex) {
            file.close();            
            throw ex;
        }
    }

        
    public void clearObjectCache() { 
        objectCache.clear();
    }

     protected OidHashTable createObjectCache(String kind, long pagePoolSize, int objectCacheSize) 
    { 
        if (pagePoolSize == INFINITE_PAGE_POOL || "strong".equals(kind)) {
            return new StrongHashTable(this, objectCacheSize);
        }
        if ("soft".equals(kind)) { 
            return new SoftHashTable(this, objectCacheSize);
        }
        if ("weak".equals(kind)) { 
            return new WeakHashTable(this, objectCacheSize);
        }
        if ("pinned".equals(kind)) { 
            return new PinWeakHashTable(this, objectCacheSize);
        }
        return new LruObjectCache(this, objectCacheSize);
    }
        

    protected void initialize(IFile file, long pagePoolSize) { 
        this.file = file;
        if (lockFile && !multiclientSupport) { 
            if (!file.tryLock(readOnly)) { 
                throw new StorageError(StorageError.STORAGE_IS_USED);
            }
        }
        dirtyPagesMap = new int[dbDirtyPageBitmapSize/4+1];
        gcThreshold = Long.MAX_VALUE;
        backgroundGcMonitor = new Object();
        backgroundGcStartMonitor = new Object();
        gcThread = null;
        gcActive = false;
        gcDone = false;
        allocatedDelta = 0;

        reservedChain = null;
        cloneList = null;
        insideCloneBitmap = false;

        nNestedTransactions = 0;
        nBlockedTransactions = 0;
        nCommittedTransactions = 0;
        scheduledCommitTime = Long.MAX_VALUE;
        transactionMonitor = new Object();
        transactionLock = new PersistentResource();

        modified = false; 

        objectCache = createObjectCache(cacheKind, pagePoolSize, objectCacheInitSize);

        objMap = new ObjectMap(objectCacheInitSize);

        classDescMap = new HashMap();
        descList = null;
        
        header = new Header();
        pool = new PagePool((int)(pagePoolSize/Page.pageSize), pagePoolLruLimit);
        pool.open(file);
    }        

    public synchronized void open(IFile file, long pagePoolSize) {
        Page pg;
        int i;

        if (opened) {
            throw new StorageError(StorageError.STORAGE_ALREADY_OPENED);
        }
        initialize(file, pagePoolSize);

        if (multiclientSupport) { 
            beginThreadTransaction(READ_WRITE_TRANSACTION);
        }            
        byte[] buf = new byte[Header.sizeof];
        int rc = file.read(0, buf);
        if (rc > 0 && rc < Header.sizeof) { 
            throw new StorageError(StorageError.DATABASE_CORRUPTED);
        }
        header.unpack(buf);
        if (header.curr < 0 || header.curr > 1) { 
            throw new StorageError(StorageError.DATABASE_CORRUPTED);
        }
        transactionId = header.transactionId;
        if (header.databaseFormatVersion == 0) { // database not initialized
            int indexSize = initIndexSize;
            if (indexSize < dbFirstUserId) { 
                indexSize = dbFirstUserId;
            }
            indexSize = (indexSize + dbHandlesPerPage - 1) & ~(dbHandlesPerPage-1);

            bitmapExtentBase = dbBitmapPages;

            header.curr = currIndex = 0;
            long used = Page.pageSize;
            header.root[0].index = used;
            header.root[0].indexSize = indexSize;
            header.root[0].indexUsed = dbFirstUserId;
            header.root[0].freeList = 0;
            used += indexSize*8L;
            header.root[1].index = used;
            header.root[1].indexSize = indexSize;
            header.root[1].indexUsed = dbFirstUserId;
            header.root[1].freeList = 0;
            used += indexSize*8L;
        
            header.root[0].shadowIndex = header.root[1].index;
            header.root[1].shadowIndex = header.root[0].index;
            header.root[0].shadowIndexSize = indexSize;
            header.root[1].shadowIndexSize = indexSize;
            
            int bitmapPages = 
                (int)((used + Page.pageSize*(dbAllocationQuantum*8-1) - 1)
                      / (Page.pageSize*(dbAllocationQuantum*8-1)));
            long bitmapSize = (long)bitmapPages*Page.pageSize;
            int usedBitmapSize = (int)((used + bitmapSize) >>> (dbAllocationQuantumBits + 3));

            for (i = 0; i < bitmapPages; i++) { 
                pg = pool.putPage(used + (long)i*Page.pageSize);
                byte[] bitmap = pg.data;
                int n = usedBitmapSize > Page.pageSize ? Page.pageSize : usedBitmapSize;
                for (int j = 0; j < n; j++) { 
                    bitmap[j] = (byte)0xFF;
                }
                usedBitmapSize -= Page.pageSize;
                pool.unfix(pg);
            }
            int bitmapIndexSize = 
                ((dbBitmapId + dbBitmapPages)*8 + Page.pageSize - 1)
                & ~(Page.pageSize - 1);
            byte[] index = new byte[bitmapIndexSize];
            Bytes.pack8(index, dbInvalidId*8, dbFreeHandleFlag);
            for (i = 0; i < bitmapPages; i++) { 
                Bytes.pack8(index, (dbBitmapId+i)*8, used | dbPageObjectFlag);
                used += Page.pageSize;
            }
            header.root[0].bitmapEnd = dbBitmapId + i;
            header.root[1].bitmapEnd = dbBitmapId + i;
            while (i < dbBitmapPages) { 
                Bytes.pack8(index, (dbBitmapId+i)*8, dbFreeHandleFlag);
                i += 1;
            }
            header.root[0].size = used;
            header.root[1].size = used;
            usedSize = used;
            committedIndexSize = currIndexSize = dbFirstUserId;

            pool.write(header.root[1].index, index);
            pool.write(header.root[0].index, index);

            modified = true;
            header.dirty = true;
            header.root[0].size = header.root[1].size;
            pg = pool.putPage(0);
            header.pack(pg.data);
            pool.flush();
            pool.modify(pg);
            header.databaseFormatVersion = dbDatabaseFormatVersion;
            header.pack(pg.data);
            pool.unfix(pg);
            pool.flush();
        } else {
            int curr = header.curr;
            currIndex = curr;
            if (header.root[curr].indexSize != header.root[curr].shadowIndexSize) {
                throw new StorageError(StorageError.DATABASE_CORRUPTED);
            }           
            bitmapExtentBase = (header.databaseFormatVersion < 2) ? 0 : dbBitmapPages;

            if (isDirty()) { 
                if (listener != null) {
                    listener.databaseCorrupted();
                }
                System.err.println("Database was not normally closed: start recovery");
                header.root[1-curr].size = header.root[curr].size;
                header.root[1-curr].indexUsed = header.root[curr].indexUsed; 
                header.root[1-curr].freeList = header.root[curr].freeList; 
                header.root[1-curr].index = header.root[curr].shadowIndex;
                header.root[1-curr].indexSize = header.root[curr].shadowIndexSize;
                header.root[1-curr].shadowIndex = header.root[curr].index;
                header.root[1-curr].shadowIndexSize = header.root[curr].indexSize;
                header.root[1-curr].bitmapEnd = header.root[curr].bitmapEnd;
                header.root[1-curr].rootObject = header.root[curr].rootObject;
                header.root[1-curr].classDescList = header.root[curr].classDescList;
                header.root[1-curr].bitmapExtent = header.root[curr].bitmapExtent;

                modified = true;
                pg = pool.putPage(0);
                header.pack(pg.data);
                pool.unfix(pg);

                pool.copy(header.root[1-curr].index, header.root[curr].index, 
                          (header.root[curr].indexUsed*8L + Page.pageSize - 1) & ~(Page.pageSize-1));
                if (listener != null) {
                    listener.recoveryCompleted();
                }
                System.err.println("Recovery completed");
            } 
            currIndexSize = header.root[1-curr].indexUsed;
            committedIndexSize = currIndexSize;
            usedSize = header.root[curr].size;
        }
        int bitmapSize = header.root[1-currIndex].bitmapExtent == 0 ? dbBitmapPages : dbLargeBitmapPages;
        bitmapPageAvailableSpace = new int[bitmapSize];
        for (i = 0; i < bitmapPageAvailableSpace.length; i++) { 
            bitmapPageAvailableSpace[i] = Integer.MAX_VALUE;
        }        
        currRBitmapPage = currPBitmapPage = 0;
        currRBitmapOffs = currPBitmapOffs = 0;

        opened = true;
        reloadScheme();

        if (multiclientSupport) { 
            modified = true;
            endThreadTransaction();
        }            
    }

    public boolean isOpened() { 
        return opened;
    }

    static void checkIfFinal(ClassDescriptor desc) {
        Class cls = desc.cls;
        if (cls != null) { 
            for (ClassDescriptor next = desc.next; next != null; next = next.next) { 
                next.load();
                if (cls.isAssignableFrom(next.cls)) { 
                    desc.hasSubclasses = true;
                } else if (next.cls.isAssignableFrom(cls)) { 
                    next.hasSubclasses = true;
                }
            }
        }
    }
        


    void reloadScheme() {
        classDescMap.clear();
        customAllocatorMap = null;
        customAllocatorList = null;
        defaultAllocator = new DefaultAllocator(this);
        int descListOid = header.root[1-currIndex].classDescList;
        classDescMap.put(ClassDescriptor.class, 
                         new ClassDescriptor(this, ClassDescriptor.class));
        classDescMap.put(ClassDescriptor.FieldDescriptor.class, 
                         new ClassDescriptor(this, ClassDescriptor.FieldDescriptor.class));
        if (descListOid != 0) {             
            ClassDescriptor desc;
            descList = findClassDescriptor(descListOid);
            for (desc = descList; desc != null; desc = desc.next) { 
                desc.load();
            }
            for (desc = descList; desc != null; desc = desc.next) { 
                if (findClassDescriptor(desc.cls) == desc) { 
                    desc.resolve();
                }
                if (desc.allocator != null) {
                    if (customAllocatorMap == null) { 
                        customAllocatorMap = new HashMap();
                        customAllocatorList = new ArrayList();
                    }
                    desc.allocator.load();
                    customAllocatorMap.put(desc.cls, desc.allocator);
                    customAllocatorList.add(desc.allocator);
                }
                checkIfFinal(desc);
            }
        } else { 
            descList = null;
        }
    }

    final void registerClassDescriptor(ClassDescriptor desc) { 
        classDescMap.put(desc.cls, desc);
        desc.next = descList;
        descList = desc;
        checkIfFinal(desc);
        storeObject0(desc, false);
        header.root[1-currIndex].classDescList = desc.getOid();
        modified = true;
    }        

    final ClassDescriptor findClassDescriptor(Class cls) { 
        return (ClassDescriptor)classDescMap.get(cls);
    }

    final ClassDescriptor getClassDescriptor(Class cls) { 
        ClassDescriptor desc = findClassDescriptor(cls);
        if (desc == null) { 
            desc = new ClassDescriptor(this, cls);
            registerClassDescriptor(desc);
        }
        return desc;
    }


    public synchronized Object getRoot() {
        if (!opened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        int rootOid = header.root[1-currIndex].rootObject;
        return (rootOid == 0) ? null : lookupObject(rootOid, null);
    }
    
    public synchronized void setRoot(Object root) {
        if (!opened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        if (root == null) { 
            header.root[1-currIndex].rootObject = 0;
        } else { 
            if (!isPersistent(root)) { 
                storeObject0(root, false);
            }
            header.root[1-currIndex].rootObject = getOid(root);
        }
        modified = true;
    }
 
    public void commit() {
        if (useSerializableTransactions && getTransactionContext().nested != 0) { 
            // Store should not be used in serializable transaction mode
            throw new StorageError(StorageError.INVALID_OPERATION, "commit");
        }
        synchronized (backgroundGcMonitor) { 
            synchronized (this) { 
                if (!opened) {
                    throw new StorageError(StorageError.STORAGE_NOT_OPENED);
                }
                objectCache.flush();
                if (customAllocatorList != null) { 
                    Iterator iterator = customAllocatorList.iterator();
                    while (iterator.hasNext()) {
                        CustomAllocator alloc = (CustomAllocator)iterator.next();
                        if (alloc.isModified()) {                             
                            alloc.store();
                        }
                        alloc.commit();
                    }
                }
                if (!modified) { 
                    return;
                }
                commit0();
                modified = false;
            }
        }
    }

    private final void commit0() 
    {
        int i, j, n;
        int curr = currIndex;
        int[] map = dirtyPagesMap;
        int oldIndexSize = header.root[curr].indexSize;
        int newIndexSize = header.root[1-curr].indexSize;
        int nPages = committedIndexSize >>> dbHandlesPerPageBits;
        Page pg;

        if (newIndexSize > oldIndexSize) { 
            cloneBitmap(header.root[curr].index, oldIndexSize*8L);
            long newIndex;
            while (true) { 
                newIndex = allocate(newIndexSize*8L, 0);
                if (newIndexSize == header.root[1-curr].indexSize) { 
                    break;
                }
                free(newIndex, newIndexSize*8L);
                newIndexSize = header.root[1-curr].indexSize;
            }
            header.root[1-curr].shadowIndex = newIndex;
            header.root[1-curr].shadowIndexSize = newIndexSize;
            free(header.root[curr].index, oldIndexSize*8L);
        }
        long currSize = header.root[1-curr].size;
        for (i = 0; i < nPages; i++) { 
            if ((map[i >> 5] & (1 << (i & 31))) != 0) { 
                Page srcIndex = pool.getPage(header.root[1-curr].index + (long)i*Page.pageSize);
                Page dstIndex = pool.getPage(header.root[curr].index + (long)i*Page.pageSize);
                for (j = 0; j < Page.pageSize; j += 8) {
                    long pos = Bytes.unpack8(dstIndex.data, j);
                    if (Bytes.unpack8(srcIndex.data, j) != pos && pos < currSize) { 
                        if ((pos & dbFreeHandleFlag) == 0) {
                            if ((pos & dbPageObjectFlag) != 0) {  
                                free(pos & ~dbFlagsMask, Page.pageSize);
                            } else if (pos != 0) { 
                                int offs = (int)pos & (Page.pageSize-1);
                                pg = pool.getPage(pos-offs);
                                free(pos, ObjectHeader.getSize(pg.data, offs));
                                pool.unfix(pg);
                            }
                        }
                    }
                }
                pool.unfix(srcIndex);
                pool.unfix(dstIndex);
            }
        }
        n = committedIndexSize & (dbHandlesPerPage-1);
        if (n != 0 && (map[i >> 5] & (1 << (i & 31))) != 0) { 
            Page srcIndex = pool.getPage(header.root[1-curr].index + (long)i*Page.pageSize);
            Page dstIndex = pool.getPage(header.root[curr].index + (long)i*Page.pageSize);
            j = 0;
            do { 
                long pos = Bytes.unpack8(dstIndex.data, j);
                if (Bytes.unpack8(srcIndex.data, j) != pos && pos < currSize) { 
                    if ((pos & dbFreeHandleFlag) == 0) {
                        if ((pos & dbPageObjectFlag) != 0) { 
                            free(pos & ~dbFlagsMask, Page.pageSize);
                        } else if (pos != 0) { 
                            int offs = (int)pos & (Page.pageSize-1);
                            pg = pool.getPage(pos - offs);
                            free(pos, ObjectHeader.getSize(pg.data, offs));
                            pool.unfix(pg);
                        }
                    }
                }
                j += 8;
            } while (--n != 0);
            pool.unfix(srcIndex);
            pool.unfix(dstIndex);
        }
        for (i = 0; i <= nPages; i++) { 
            if ((map[i >> 5] & (1 << (i & 31))) != 0) { 
                pg = pool.putPage(header.root[1-curr].index + (long)i*Page.pageSize);
                for (j = 0; j < Page.pageSize; j += 8) {
                    Bytes.pack8(pg.data, j, Bytes.unpack8(pg.data, j) & ~dbModifiedFlag);
                }
                pool.unfix(pg);
            }
        }
        if (currIndexSize > committedIndexSize) { 
            long page = (header.root[1-curr].index 
                         + committedIndexSize*8L) & ~(Page.pageSize-1);
            long end = (header.root[1-curr].index + Page.pageSize - 1
                        + currIndexSize*8L) & ~(Page.pageSize-1);
            while (page < end) { 
                pg = pool.putPage(page);
                for (j = 0; j < Page.pageSize; j += 8) {
                    Bytes.pack8(pg.data, j, Bytes.unpack8(pg.data, j) & ~dbModifiedFlag);
                }
                pool.unfix(pg);
                page += Page.pageSize;
            }
        }
        header.root[1-curr].usedSize = usedSize;
        pg = pool.putPage(0);
        header.pack(pg.data);
        pool.flush();
        pool.modify(pg);
        Assert.that(header.transactionId == transactionId);
        header.transactionId = ++transactionId;
        header.curr = curr ^= 1;
        header.dirty = true;
        header.pack(pg.data);
        pool.unfix(pg);
        pool.flush();
        header.root[1-curr].size = header.root[curr].size;
        header.root[1-curr].indexUsed = currIndexSize; 
        header.root[1-curr].freeList  = header.root[curr].freeList; 
        header.root[1-curr].bitmapEnd = header.root[curr].bitmapEnd; 
        header.root[1-curr].rootObject = header.root[curr].rootObject; 
        header.root[1-curr].classDescList = header.root[curr].classDescList; 
        header.root[1-curr].bitmapExtent = header.root[curr].bitmapExtent; 
        if (currIndexSize == 0 || newIndexSize != oldIndexSize) {
            header.root[1-curr].index = header.root[curr].shadowIndex;
            header.root[1-curr].indexSize = header.root[curr].shadowIndexSize;
            header.root[1-curr].shadowIndex = header.root[curr].index;
            header.root[1-curr].shadowIndexSize = header.root[curr].indexSize;
            pool.copy(header.root[1-curr].index, header.root[curr].index,
                      currIndexSize*8L);
            i = (currIndexSize+dbHandlesPerPage*32-1) >>> (dbHandlesPerPageBits+5);
            while (--i >= 0) { 
                map[i] = 0;
            }
        } else { 
            for (i = 0; i < nPages; i++) { 
                if ((map[i >> 5] & (1 << (i & 31))) != 0) { 
                    map[i >> 5] -= (1 << (i & 31));
                    pool.copy(header.root[1-curr].index + (long)i*Page.pageSize,
                              header.root[curr].index + (long)i*Page.pageSize,
                              Page.pageSize);
                }
            }
            if (currIndexSize > i*dbHandlesPerPage &&
                ((map[i >> 5] & (1 << (i & 31))) != 0
                 || currIndexSize != committedIndexSize))
            {
                pool.copy(header.root[1-curr].index + (long)i*Page.pageSize,
                          header.root[curr].index + (long)i*Page.pageSize,
                          8L*currIndexSize - (long)i*Page.pageSize);
                j = i>>>5;
                n = (currIndexSize + dbHandlesPerPage*32 - 1) >>> (dbHandlesPerPageBits+5); 
                while (j < n) { 
                    map[j++] = 0;
                }
            }
        }
        gcDone = false;
        currIndex = curr;
        committedIndexSize = currIndexSize;
        if (multiclientSupport) { 
            pool.flush();
            pg = pool.putPage(0);
            header.dirty = false;
            header.pack(pg.data);
            pool.unfix(pg);
            pool.flush();
        }
    }

    public synchronized void rollback() {
        if (!opened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        if (useSerializableTransactions && getTransactionContext().nested != 0) { 
            // Store should not be used in serializable transaction mode
            throw new StorageError(StorageError.INVALID_OPERATION, "rollback");
        }
        objectCache.invalidate();
        synchronized (objectCache){
            if (!modified) { 
                return;
            } 
            rollback0();
            modified = false;
        }
    }

    private final void rollback0() {
        int curr = currIndex;
        int[] map = dirtyPagesMap;
        if (header.root[1-curr].index != header.root[curr].shadowIndex) { 
            pool.copy(header.root[curr].shadowIndex, header.root[curr].index, 8L*committedIndexSize);
        } else { 
            int nPages = (committedIndexSize + dbHandlesPerPage - 1) >>> dbHandlesPerPageBits;
            for (int i = 0; i < nPages; i++) { 
                if ((map[i >> 5] & (1 << (i & 31))) != 0) { 
                    pool.copy(header.root[curr].shadowIndex + (long)i*Page.pageSize,
                              header.root[curr].index + (long)i*Page.pageSize,
                              Page.pageSize);
                }
            }
        }
        for (int j = (currIndexSize+dbHandlesPerPage*32-1) >>> (dbHandlesPerPageBits+5);
             --j >= 0;
             map[j] = 0);
        header.root[1-curr].index = header.root[curr].shadowIndex;
        header.root[1-curr].indexSize = header.root[curr].shadowIndexSize;
        header.root[1-curr].indexUsed = committedIndexSize;
        header.root[1-curr].freeList  = header.root[curr].freeList; 
        header.root[1-curr].bitmapEnd = header.root[curr].bitmapEnd; 
        header.root[1-curr].size = header.root[curr].size;
        header.root[1-curr].rootObject = header.root[curr].rootObject;
        header.root[1-curr].classDescList = header.root[curr].classDescList;
        header.root[1-curr].bitmapExtent = header.root[curr].bitmapExtent;
        usedSize = header.root[curr].size;
        currIndexSize = committedIndexSize;
        currRBitmapPage = currPBitmapPage = 0;
        currRBitmapOffs = currPBitmapOffs = 0;
        reloadScheme();
    }

    public synchronized void backup(OutputStream out) throws java.io.IOException
    {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        objectCache.flush();

        int   curr = 1-currIndex;
        final int nObjects = header.root[curr].indexUsed;
        long  indexOffs = header.root[curr].index;
        int   i, j, k;
        int   nUsedIndexPages = (nObjects + dbHandlesPerPage - 1) / dbHandlesPerPage;
        int   nIndexPages = (int)((header.root[curr].indexSize + dbHandlesPerPage - 1) / dbHandlesPerPage);
        long  totalRecordsSize = 0;
        long  nPagedObjects = 0;
        int   bitmapExtent = header.root[curr].bitmapExtent;
        final long[] index = new long[nObjects];
        final int[]  oids = new int[nObjects];
            
        if (bitmapExtent == 0) { 
            bitmapExtent = Integer.MAX_VALUE;
        }
        for (i = 0, j = 0; i < nUsedIndexPages; i++) {
            Page pg = pool.getPage(indexOffs + (long)i*Page.pageSize);
            for (k = 0; k < dbHandlesPerPage && j < nObjects; k++, j++) { 
                long pos = Bytes.unpack8(pg.data, k*8);
                index[j] = pos;
                oids[j] = j;
                if ((pos & dbFreeHandleFlag) == 0) { 
                    if ((pos & dbPageObjectFlag) != 0) {
                        nPagedObjects += 1;
                    } else if (pos != 0) { 
                        int offs = (int)pos & (Page.pageSize-1);
                        Page op = pool.getPage(pos - offs);
                        int size = ObjectHeader.getSize(op.data, offs & ~dbFlagsMask);
                        size = (size + dbAllocationQuantum-1) & ~(dbAllocationQuantum-1);
                        totalRecordsSize += size; 
                        pool.unfix(op);
                    }
                }
            }
            pool.unfix(pg);
        
        } 
        Header newHeader = new Header();
        newHeader.curr = 0;
        newHeader.dirty = false;
        newHeader.databaseFormatVersion = header.databaseFormatVersion;
        long newFileSize = (long)(nPagedObjects + nIndexPages*2 + 1)*Page.pageSize + totalRecordsSize;
        newFileSize = (newFileSize + Page.pageSize-1) & ~(Page.pageSize-1);     
        newHeader.root = new RootPage[2];
        newHeader.root[0] = new RootPage();
        newHeader.root[1] = new RootPage();
        newHeader.root[0].size = newHeader.root[1].size = newFileSize;
        newHeader.root[0].index = newHeader.root[1].shadowIndex = Page.pageSize;
        newHeader.root[0].shadowIndex = newHeader.root[1].index = Page.pageSize + (long)nIndexPages*Page.pageSize;
        newHeader.root[0].shadowIndexSize = newHeader.root[0].indexSize = 
            newHeader.root[1].shadowIndexSize = newHeader.root[1].indexSize = nIndexPages*dbHandlesPerPage;
        newHeader.root[0].indexUsed = newHeader.root[1].indexUsed = nObjects;
        newHeader.root[0].freeList = newHeader.root[1].freeList = header.root[curr].freeList;
        newHeader.root[0].bitmapEnd = newHeader.root[1].bitmapEnd = header.root[curr].bitmapEnd;

        newHeader.root[0].rootObject = newHeader.root[1].rootObject = header.root[curr].rootObject;
        newHeader.root[0].classDescList = newHeader.root[1].classDescList = header.root[curr].classDescList;
        newHeader.root[0].bitmapExtent = newHeader.root[1].bitmapExtent = header.root[curr].bitmapExtent;
        byte[] page = new byte[Page.pageSize];
        newHeader.pack(page);
        out.write(page);
        
        long pageOffs = (long)(nIndexPages*2 + 1)*Page.pageSize;
        long recOffs = (long)(nPagedObjects + nIndexPages*2 + 1)*Page.pageSize;
        GenericSort.sort(new GenericSortArray() { 
                public int size() { 
                    return nObjects;
                }
                public int compare(int i, int j) { 
                    return index[i] < index[j] ? -1 : index[i] == index[j] ? 0 : 1;
                }
                public void swap(int i, int j) { 
                    long t1 = index[i];
                    index[i] = index[j];
                    index[j] = t1;
                    int t2 = oids[i];
                    oids[i] = oids[j];
                    oids[j] = t2;
                }
            }
        );
        byte[] newIndex = new byte[nIndexPages*dbHandlesPerPage*8];
        for (i = 0; i < nObjects; i++) {
            long pos = index[i];
            int oid = oids[i];
            if ((pos & dbFreeHandleFlag) == 0) { 
                if ((pos & dbPageObjectFlag) != 0) {
                    Bytes.pack8(newIndex, oid*8, pageOffs | dbPageObjectFlag);
                    pageOffs += Page.pageSize;
                } else if (pos != 0) { 
                    Bytes.pack8(newIndex, oid*8, recOffs);
                    int offs = (int)pos & (Page.pageSize-1);
                    Page op = pool.getPage(pos - offs);
                    int size = ObjectHeader.getSize(op.data, offs & ~dbFlagsMask);
                    size = (size + dbAllocationQuantum-1) & ~(dbAllocationQuantum-1);
                    recOffs += size; 
                    pool.unfix(op);
                }
            } else { 
                Bytes.pack8(newIndex, oid*8, pos);
            }
        }
        out.write(newIndex);
        out.write(newIndex);

        for (i = 0; i < nObjects; i++) {
            long pos = index[i];
            if (((int)pos & (dbFreeHandleFlag|dbPageObjectFlag)) == dbPageObjectFlag) { 
                if (oids[i] < dbBitmapId + dbBitmapPages 
                    || (oids[i] >= bitmapExtent && oids[i] < bitmapExtent + dbLargeBitmapPages - dbBitmapPages))
                { 
                    int pageId = oids[i] < dbBitmapId + dbBitmapPages 
                        ? oids[i] - dbBitmapId : oids[i] - bitmapExtent + bitmapExtentBase;
                    long mappedSpace = (long)pageId*Page.pageSize*8*dbAllocationQuantum;
                    if (mappedSpace >= newFileSize) { 
                        Arrays.fill(page, (byte)0);
                    } else if (mappedSpace + Page.pageSize*8*dbAllocationQuantum <= newFileSize) { 
                        Arrays.fill(page, (byte)-1);
                    } else { 
                        int nBits = (int)((newFileSize - mappedSpace) >> dbAllocationQuantumBits);
                        Arrays.fill(page, 0, nBits >> 3, (byte)-1);
                        page[nBits >> 3] = (byte)((1 << (nBits & 7)) - 1);
                        Arrays.fill(page, (nBits >> 3) + 1, Page.pageSize, (byte)0);
                    }
                    out.write(page);
                } else {                        
                    Page pg = pool.getPage(pos & ~dbFlagsMask);
                    out.write(pg.data);
                    pool.unfix(pg);
                }
            }
        }
        for (i = 0; i < nObjects; i++) {
            long pos = index[i];
            if (pos != 0 && ((int)pos & (dbFreeHandleFlag|dbPageObjectFlag)) == 0) { 
                pos &= ~dbFlagsMask;
                int offs = (int)pos & (Page.pageSize-1);
                Page pg = pool.getPage(pos - offs);
                int size = ObjectHeader.getSize(pg.data, offs);
                size = (size + dbAllocationQuantum-1) & ~(dbAllocationQuantum-1);

                while (true) { 
                    if (Page.pageSize - offs >= size) { 
                        out.write(pg.data, offs, size);
                        break;
                    }
                    out.write(pg.data, offs, Page.pageSize - offs);
                    size -= Page.pageSize - offs;
                    pos += Page.pageSize - offs;
                    offs = 0;
                    pool.unfix(pg); 
                    pg = pool.getPage(pos);
                }
                pool.unfix(pg);
            }
        }
        if (recOffs != newFileSize) {       
            Assert.that(newFileSize - recOffs < Page.pageSize);
            int align = (int)(newFileSize - recOffs);
            Arrays.fill(page, 0, align, (byte)0);
            out.write(page, 0, align);
        }        
    }

    public <T> Query<T> createQuery() { 
        return new QueryImpl<T>(this);
    }

    public synchronized <T> IPersistentSet<T> createScalableSet() {
        return createScalableSet(8);
    }

    public synchronized <T> IPersistentSet<T> createScalableSet(int initialSize) {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        return new ScalableSet(this, initialSize);
    }

    public <T> IPersistentList<T> createList() {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        return new PersistentListImpl<T>(this);
    }

    public <T> IPersistentList<T> createScalableList() {
        return createScalableList(8);
    }

    public <T> IPersistentList<T> createScalableList(int initialSize) {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        return new ScalableList<T>(this, initialSize);
    }

    public <K extends Comparable, V> IPersistentMap<K, V> createMap(Class keyType) {
        return createMap(keyType, 4);
    }

    public <K extends Comparable, V> IPersistentMap<K, V> createMap(Class keyType, int initialSize) {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        return new PersistentMapImpl<K,V>(this, keyType, initialSize);
    }

    public synchronized <T> IPersistentSet<T> createSet() {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        IPersistentSet<T> set = alternativeBtree 
            ? (IPersistentSet<T>)new AltPersistentSet<T>()
            : (IPersistentSet<T>)new PersistentSet<T>();
        set.assignOid(this, 0, false);
        return set;
    }
        
    public synchronized <T> BitIndex<T> createBitIndex() {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        BitIndex index = new BitIndexImpl<T>();
        index.assignOid(this, 0, false);
        return index;
    }

    public synchronized <T> Index<T> createIndex(Class keyType, boolean unique) {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        Index<T> index = alternativeBtree 
            ? (Index<T>)new AltBtree<T>(keyType, unique)
            : (Index<T>)new Btree<T>(keyType, unique);
        index.assignOid(this, 0, false);
        return index;
    }

    public synchronized <T> Index<T> createIndex(Class[] keyTypes, boolean unique) {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        Index<T> index = alternativeBtree 
            ? (Index<T>)new AltBtreeCompoundIndex<T>(keyTypes, unique)
            : (Index<T>)new BtreeCompoundIndex<T>(keyTypes, unique);
        index.assignOid(this, 0, false);
        return index;
    }

    public synchronized <T> MultidimensionalIndex<T> createMultidimensionalIndex(MultidimensionalComparator<T> comparator)
    {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        return new KDTree<T>(this, comparator);
    }      

    public synchronized <T> MultidimensionalIndex<T> createMultidimensionalIndex(Class type, String[] fieldNames, boolean treateZeroAsUndefinedValue)
    {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        return new KDTree<T>(this, type, fieldNames, treateZeroAsUndefinedValue);
    }      

    public synchronized <T> Index<T> createThickIndex(Class keyType) {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        return new ThickIndex<T>(keyType, this);
    }      

    public synchronized <T> SpatialIndex<T> createSpatialIndex() {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        return new Rtree<T>();
    }

    public synchronized <T> SpatialIndexR2<T> createSpatialIndexR2() {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        return new RtreeR2<T>(this);
    }

    public <T> FieldIndex<T> createFieldIndex(Class type, String fieldName, boolean unique) {
        return this.<T>createFieldIndex(type, fieldName, unique, false);
    }

    public synchronized <T> FieldIndex<T> createFieldIndex(Class type, String fieldName, boolean unique, boolean caseInsensitive) {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }        
        FieldIndex<T> index = caseInsensitive
            ? alternativeBtree
                 ? (FieldIndex<T>)new AltBtreeCaseInsensitiveFieldIndex<T>(type, fieldName, unique)
                 : (FieldIndex<T>)new BtreeCaseInsensitiveFieldIndex<T>(type, fieldName, unique)
            : alternativeBtree
                 ? (FieldIndex<T>)new AltBtreeFieldIndex<T>(type, fieldName, unique)
                 : (FieldIndex<T>)new BtreeFieldIndex<T>(type, fieldName, unique);
        index.assignOid(this, 0, false);
        return index;
    }

    public <T> FieldIndex<T> createFieldIndex(Class type, String[] fieldNames, boolean unique) {
        return this.<T>createFieldIndex(type, fieldNames, unique, false);
    }

    public synchronized <T> FieldIndex<T> createFieldIndex(Class type, String[] fieldNames, boolean unique, boolean caseInsensitive) {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }        
        FieldIndex<T> index = caseInsensitive
            ? alternativeBtree
                ? (FieldIndex<T>)new AltBtreeCaseInsensitiveMultiFieldIndex(type, fieldNames, unique)
                : (FieldIndex<T>)new BtreeCaseInsensitiveMultiFieldIndex(type, fieldNames, unique)
            : alternativeBtree
                ? (FieldIndex<T>)new AltBtreeMultiFieldIndex(type, fieldNames, unique)
                : (FieldIndex<T>)new BtreeMultiFieldIndex(type, fieldNames, unique);
        index.assignOid(this, 0, false);
        return index;
    }

    public synchronized <T> Index<T> createRandomAccessIndex(Class keyType, boolean unique) {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        Index<T> index = new RndBtree<T>(keyType, unique);
        index.assignOid(this, 0, false);
        return index;
    }

    public synchronized  <T> Index<T> createRandomAccessIndex(Class[] keyTypes, boolean unique) {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        Index<T> index = new RndBtreeCompoundIndex<T>(keyTypes, unique);
        index.assignOid(this, 0, false);
        return index;
    }


    public <T> FieldIndex<T> createRandomAccessFieldIndex(Class type, String fieldName, boolean unique) {    
        return this.<T>createRandomAccessFieldIndex(type, fieldName, unique, false);
    }

    public synchronized <T> FieldIndex<T> createRandomAccessFieldIndex(Class type, String fieldName, boolean unique, boolean caseInsensitive) {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }        
        FieldIndex<T> index = caseInsensitive
            ? (FieldIndex)new RndBtreeCaseInsensitiveFieldIndex<T>(type, fieldName, unique)
            : (FieldIndex)new RndBtreeFieldIndex<T>(type, fieldName, unique);
        index.assignOid(this, 0, false);
        return index;
    }

    public <T> FieldIndex<T> createRandomAccessFieldIndex(Class type, String[] fieldNames, boolean unique) {
        return this.<T>createRandomAccessFieldIndex(type, fieldNames, unique, false);
    }
        
    public synchronized <T> FieldIndex<T> createRandomAccessFieldIndex(Class type, String[] fieldNames, boolean unique, boolean caseInsensitive) {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }        
        FieldIndex<T> index = caseInsensitive
            ? (FieldIndex)new RndBtreeCaseInsensitiveMultiFieldIndex(type, fieldNames, unique)
            : (FieldIndex)new RndBtreeMultiFieldIndex(type, fieldNames, unique);
        index.assignOid(this, 0, false);
        return index;
    }

    public <T> SortedCollection<T> createSortedCollection(PersistentComparator<T> comparator, boolean unique) {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }        
        return new Ttree<T>(this, comparator, unique);
    }
        
    public <T extends Comparable> SortedCollection<T> createSortedCollection(boolean unique) {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }        
        return new Ttree<T>(this, new DefaultPersistentComparator<T>(), unique);
    }
        
    public <T> Link<T> createLink() {
        return createLink(8);
    }

    public <T> Link<T> createLink(int initialSize) {
        return new LinkImpl<T>(this, initialSize);
    }

    public <M, O> Relation<M,O> createRelation(O owner) {
        return new RelationImpl<M,O>(this, owner);
    }

    public Blob createBlob() { 
        return new BlobImpl(this, Page.pageSize - BlobImpl.headerSize);
    }

    public Blob createRandomAccessBlob() { 
        return new RandomAccessBlobImpl(this);
    }

    public <T extends TimeSeries.Tick> TimeSeries<T> createTimeSeries(Class blockClass, long maxBlockTimeInterval) {
        return new TimeSeriesImpl<T>(this, blockClass, maxBlockTimeInterval);
    }

    public <T> PatriciaTrie<T> createPatriciaTrie() { 
        return new PTrie<T>();
    }

    public FullTextIndex createFullTextIndex(FullTextSearchHelper helper) {
        return new FullTextIndexImpl(this, helper);
    }

    public FullTextIndex createFullTextIndex() {
        return createFullTextIndex(new FullTextSearchHelper(this));
    }

    final long getGCPos(int oid) { 
        Page pg = pool.getPage(header.root[currIndex].index 
                               + ((long)(oid >>> dbHandlesPerPageBits) << Page.pageSizeLog));
        long pos = Bytes.unpack8(pg.data, (oid & (dbHandlesPerPage-1)) << 3);
        pool.unfix(pg);
        return pos;
    }
        
    final void markOid(int oid) { 
        if (oid != 0) {  
            long pos = getGCPos(oid);
            if ((pos & (dbFreeHandleFlag|dbPageObjectFlag)) != 0) { 
                throw new StorageError(StorageError.INVALID_OID);
            }
            if (pos < header.root[currIndex].size) { 
                // object was not allocated by custom allocator
                int bit = (int)(pos >>> dbAllocationQuantumBits);
                if ((blackBitmap[bit >>> 5] & (1 << (bit & 31))) == 0) { 
                    greyBitmap[bit >>> 5] |= 1 << (bit & 31);
                }
            }
        }
    }

    final Page getGCPage(int oid) {  
        return pool.getPage(getGCPos(oid) & ~dbFlagsMask);
    }

    public void setGcThreshold(long maxAllocatedDelta) {
        gcThreshold = maxAllocatedDelta;
    }

    private void mark() { 
        int bitmapSize = (int)(header.root[currIndex].size >>> (dbAllocationQuantumBits + 5)) + 1;
        boolean existsNotMarkedObjects;
        long pos;
        int  i, j;
        
        if (listener != null) { 
            listener.gcStarted();
        }           

        greyBitmap = new int[bitmapSize];
        blackBitmap = new int[bitmapSize];
        int rootOid = header.root[currIndex].rootObject;
        if (rootOid != 0) { 
            markOid(rootOid);
            do { 
                existsNotMarkedObjects = false;
                for (i = 0; i < bitmapSize; i++) { 
                    if (greyBitmap[i] != 0) { 
                        existsNotMarkedObjects = true;
                        for (j = 0; j < 32; j++) { 
                            if ((greyBitmap[i] & (1 << j)) != 0) { 
                                pos = (((long)i << 5) + j) << dbAllocationQuantumBits;
                                greyBitmap[i] &= ~(1 << j);
                                blackBitmap[i] |= 1 << j;
                                int offs = (int)pos & (Page.pageSize-1);
                                Page pg = pool.getPage(pos - offs);
                                int typeOid = ObjectHeader.getType(pg.data, offs);
                                if (typeOid != 0) { 
                                    ClassDescriptor desc = findClassDescriptor(typeOid);
                                    if (Btree.class.isAssignableFrom(desc.cls)) { 
                                        Btree btree = new Btree(pg.data, ObjectHeader.sizeof + offs);
                                        btree.assignOid(this, 0, false);
                                        btree.markTree();
                                    } else if (desc.hasReferences) { 
                                        markObject(pool.get(pos), ObjectHeader.sizeof, desc);
                                    }
                                }
                                pool.unfix(pg);                                
                            }
                        }
                    }
                }
            } while (existsNotMarkedObjects);
        }    
    }

    private int sweep() { 
        int nDeallocated = 0;
        long pos;
        gcDone = true;
        for (int i = dbFirstUserId, j = committedIndexSize; i < j; i++) {
            pos = getGCPos(i);
            if (pos != 0 && ((int)pos & (dbPageObjectFlag|dbFreeHandleFlag)) == 0) {
                int bit = (int)(pos >>> dbAllocationQuantumBits);
                if ((blackBitmap[bit >>> 5] & (1 << (bit & 31))) == 0) { 
                    // object is not accessible
                    if (getPos(i) == pos) { 
                        int offs = (int)pos & (Page.pageSize-1);
                        Page pg = pool.getPage(pos - offs);
                        int typeOid = ObjectHeader.getType(pg.data, offs);
                        if (typeOid != 0) { 
                            ClassDescriptor desc = findClassDescriptor(typeOid);
                            nDeallocated += 1;
                            if (Btree.class.isAssignableFrom(desc.cls)) { 
                                Btree btree = new Btree(pg.data, ObjectHeader.sizeof + offs);
                                pool.unfix(pg);
                                btree.assignOid(this, i, false);
                                btree.deallocate();
                            } else { 
                                int size = ObjectHeader.getSize(pg.data, offs);
                                pool.unfix(pg);
                                freeId(i);
                                objectCache.remove(i);                        
                                cloneBitmap(pos, size);
                            }
                            if (listener != null) { 
                                listener.deallocateObject(desc.cls, i);
                            }
                        }
                    }
                }
            }   
        }

        greyBitmap = null;
        blackBitmap = null;
        allocatedDelta = 0;
        gcActive = false;

        if (listener != null) {
            listener.gcCompleted(nDeallocated);
        }
        return nDeallocated;
    }   
     
    class GcThread extends Thread { 
        private boolean go;

        GcThread() { 
            start();
        }

        void activate() { 
            synchronized(backgroundGcStartMonitor) { 
                go = true;
                backgroundGcStartMonitor.notify();
            }
        }

        public void run() { 
            try { 
                while (true) { 
                    synchronized(backgroundGcStartMonitor) { 
                        while (!go && opened) { 
                            backgroundGcStartMonitor.wait();
                        }
                        if (!opened) { 
                            return;
                        }
                        go = false;
                    }
                    synchronized(backgroundGcMonitor) { 
                        if (!opened) { 
                            return;
                        }
                        mark();
                        synchronized (StorageImpl.this) { 
                            synchronized (objectCache) { 
                                sweep();
                            }
                        }                        
                    }
                }
            } catch (InterruptedException x) { 
            }    
        }
    }

    public synchronized int gc() { 
        return gc0();
    }

    private int gc0() { 
        synchronized (objectCache) { 
            if (!opened) {
                throw new StorageError(StorageError.STORAGE_NOT_OPENED);
            }
            if (gcDone || gcActive) { 
                return 0;
            }
            gcActive = true;
            if (backgroundGc) { 
                if (gcThread == null) { 
                    gcThread = new GcThread();
                }
                gcThread.activate();
                return 0;
            }
            // System.out.println("Start GC, allocatedDelta=" + allocatedDelta + ", header[" + currIndex + "].size=" + header.root[currIndex].size + ", gcTreshold=" + gcThreshold);
                        
            mark();
            return sweep();
        }
    }


    public synchronized HashMap getMemoryDump() { 
        synchronized (objectCache) { 
            if (!opened) {
                throw new StorageError(StorageError.STORAGE_NOT_OPENED);
            }
            int bitmapSize = (int)(header.root[currIndex].size >>> (dbAllocationQuantumBits + 5)) + 1;
            boolean existsNotMarkedObjects;
            long pos;
            int  i, j, n;

            // mark
            greyBitmap = new int[bitmapSize];
            blackBitmap = new int[bitmapSize];
            int rootOid = header.root[currIndex].rootObject;
            HashMap map = new HashMap();

            if (rootOid != 0) { 
                MemoryUsage indexUsage = new MemoryUsage(Index.class);
                MemoryUsage fieldIndexUsage = new MemoryUsage(FieldIndex.class);
                MemoryUsage classUsage = new MemoryUsage(Class.class);

                markOid(rootOid);
                do { 
                    existsNotMarkedObjects = false;
                    for (i = 0; i < bitmapSize; i++) { 
                        if (greyBitmap[i] != 0) { 
                            existsNotMarkedObjects = true;
                            for (j = 0; j < 32; j++) { 
                                if ((greyBitmap[i] & (1 << j)) != 0) { 
                                    pos = (((long)i << 5) + j) << dbAllocationQuantumBits;
                                    greyBitmap[i] &= ~(1 << j);
                                    blackBitmap[i] |= 1 << j;
                                    int offs = (int)pos & (Page.pageSize-1);
                                    Page pg = pool.getPage(pos - offs);
                                    int typeOid = ObjectHeader.getType(pg.data, offs);
                                    int objSize = ObjectHeader.getSize(pg.data, offs);
                                    int alignedSize = (objSize + dbAllocationQuantum - 1) & ~(dbAllocationQuantum-1);                                    
                                    if (typeOid != 0) { 
                                        markOid(typeOid);
                                        ClassDescriptor desc = findClassDescriptor(typeOid);
                                        if (Btree.class.isAssignableFrom(desc.cls)) { 
                                            Btree btree = new Btree(pg.data, ObjectHeader.sizeof + offs);
                                            btree.assignOid(this, 0, false);
                                            int nPages = btree.markTree();
                                            if (FieldIndex.class.isAssignableFrom(desc.cls)) { 
                                                fieldIndexUsage.nInstances += 1;
                                                fieldIndexUsage.totalSize += (long)nPages*Page.pageSize + objSize;
                                                fieldIndexUsage.allocatedSize += (long)nPages*Page.pageSize + alignedSize;
                                            } else {
                                                indexUsage.nInstances += 1;
                                                indexUsage.totalSize += (long)nPages*Page.pageSize + objSize;
                                                indexUsage.allocatedSize += (long)nPages*Page.pageSize + alignedSize;
                                            }
                                        } else { 
                                            MemoryUsage usage = (MemoryUsage)map.get(desc.cls);
                                            if (usage == null) { 
                                                usage = new MemoryUsage(desc.cls);
                                                map.put(desc.cls, usage);
                                            }
                                            usage.nInstances += 1;
                                            usage.totalSize += objSize;
                                            usage.allocatedSize += alignedSize;
                                                      
                                            if (desc.hasReferences) { 
                                                markObject(pool.get(pos), ObjectHeader.sizeof, desc);
                                            }
                                        }
                                    } else { 
                                        classUsage.nInstances += 1;
                                        classUsage.totalSize += objSize;
                                        classUsage.allocatedSize += alignedSize;
                                    }
                                    pool.unfix(pg);                                
                                }
                            }
                        }
                    }
                } while (existsNotMarkedObjects);
                
                if (indexUsage.nInstances != 0) { 
                    map.put(Index.class, indexUsage);
                }
                if (fieldIndexUsage.nInstances != 0) { 
                    map.put(FieldIndex.class, fieldIndexUsage);
                }
                if (classUsage.nInstances != 0) { 
                    map.put(Class.class, classUsage);
                }
                MemoryUsage system = new MemoryUsage(Storage.class);
                system.totalSize += header.root[0].indexSize*8L;
                system.totalSize += header.root[1].indexSize*8L;
                system.totalSize += (long)(header.root[currIndex].bitmapEnd-dbBitmapId)*Page.pageSize;
                system.totalSize += Page.pageSize; // root page

                if (header.root[currIndex].bitmapExtent != 0) { 
                    system.allocatedSize = getBitmapUsedSpace(dbBitmapId, dbBitmapId+dbBitmapPages)
                        + getBitmapUsedSpace(header.root[currIndex].bitmapExtent + dbBitmapPages - bitmapExtentBase, 
                                             header.root[currIndex].bitmapExtent + header.root[currIndex].bitmapEnd - dbBitmapId - bitmapExtentBase);
                } else { 
                    system.allocatedSize = getBitmapUsedSpace(dbBitmapId, header.root[currIndex].bitmapEnd);
                }
                system.nInstances = header.root[currIndex].indexSize;
                map.put(Storage.class, system);
            } 
            return map;
        }
    }
        
    final long getBitmapUsedSpace(int from, int till) { 
        long allocated = 0;
        while (from < till) {
            Page pg = getGCPage(from);
            for (int j = 0; j < Page.pageSize; j++) {
                int mask = pg.data[j] & 0xFF;
                while (mask != 0) { 
                    if ((mask & 1) != 0) { 
                        allocated += dbAllocationQuantum;
                    }
                    mask >>= 1;
                }
            }
            pool.unfix(pg);
            from += 1;
        }
        return allocated;
    }

    final int markObjectReference(byte[] obj, int offs)
    {
        int oid = Bytes.unpack4(obj, offs);
        offs += 4;
        if (oid < 0) { 
            int tid = -1 - oid;
            int len;
            switch (tid) {
            case ClassDescriptor.tpString:
                offs = Bytes.skipString(obj, offs);
                break;
            case ClassDescriptor.tpArrayOfByte:
                len = Bytes.unpack4(obj, offs);   
                offs += len + 4;                    
                break;
            case ClassDescriptor.tpArrayOfObject:
                len = Bytes.unpack4(obj, offs);   
                offs += 4;
                for (int i = 0; i < len; i++) { 
                    offs = markObjectReference(obj, offs);
                }
                break;
            case ClassDescriptor.tpArrayOfRaw:
                len = Bytes.unpack4(obj, offs);   
                offs += 8;
                for (int i = 0; i < len; i++) { 
                    offs = markObjectReference(obj, offs);
                }
                break;
            case ClassDescriptor.tpCustom:
                try { 
                    ByteArrayObjectInputStream in = new ByteArrayObjectInputStream(obj, offs, null, false, true);
                    serializer.unpack(in);
                    offs = in.getPosition();
                    break;
                } catch (IOException x) { 
                    throw new StorageError(StorageError.ACCESS_VIOLATION, x);
                }
            default:
                if (tid >= ClassDescriptor.tpValueTypeBias) { 
                    int typeOid = - ClassDescriptor.tpValueTypeBias - oid;
                    ClassDescriptor desc = findClassDescriptor(typeOid);
                    if (desc.isCollection) { 
                        len = Bytes.unpack4(obj, offs);   
                        offs += 4;
                        for (int i = 0; i < len; i++) { 
                            offs = markObjectReference(obj, offs);
                        }                            
                    } else { 
                        offs = markObject(obj, offs, desc);
                    }
                } else {
                    offs += ClassDescriptor.sizeof[tid];
                }
            }
        }       
        else
        {
            markOid(oid);
        }
        return offs;
    }

    final int markObject(byte[] obj, int offs,  ClassDescriptor desc)
    { 
        ClassDescriptor.FieldDescriptor[] all = desc.allFields;

        for (int i = 0, n = all.length; i < n; i++) { 
            ClassDescriptor.FieldDescriptor fd = all[i];
            switch (fd.type) { 
                case ClassDescriptor.tpBoolean:
                case ClassDescriptor.tpByte:
                    offs += 1;
                    continue;
                case ClassDescriptor.tpChar:
                case ClassDescriptor.tpShort:
                    offs += 2;
                    continue;
                case ClassDescriptor.tpInt:
                case ClassDescriptor.tpEnum:
                case ClassDescriptor.tpFloat:
                    offs += 4;
                    continue;
                case ClassDescriptor.tpLong:
                case ClassDescriptor.tpDouble:
                case ClassDescriptor.tpDate:
                    offs += 8;
                    continue;
                case ClassDescriptor.tpString:
                    offs = Bytes.skipString(obj, offs);
                    continue;
                case ClassDescriptor.tpObject:
                    offs = markObjectReference(obj, offs);
                    continue;
                case ClassDescriptor.tpValue:
                    offs = markObject(obj, offs, fd.valueDesc);
                    continue;
                case ClassDescriptor.tpRaw:
                {
                    int len = Bytes.unpack4(obj, offs);
                    offs += 4;
                    if (len > 0) { 
                        offs += len;
                    } else if (len == -2-ClassDescriptor.tpObject) { 
                        markOid(Bytes.unpack4(obj, offs));
                        offs += 4;
                    } else if (len < -1) { 
                        offs += ClassDescriptor.sizeof[-2-len];
                    }
                    continue;
                }
                case ClassDescriptor.tpCustom:
                    try { 
                        ByteArrayObjectInputStream in = new ByteArrayObjectInputStream(obj, offs, null, false, true);
                        serializer.unpack(in);
                        offs = in.getPosition();
                    } catch (IOException x) { 
                        throw new StorageError(StorageError.ACCESS_VIOLATION, x);
                    }
                    continue;
                case ClassDescriptor.tpArrayOfByte:
                case ClassDescriptor.tpArrayOfBoolean:
                {
                    int len = Bytes.unpack4(obj, offs);
                    offs += 4;
                    if (len > 0) { 
                        offs += len;
                    } else if (len < -1) { 
                        offs += ClassDescriptor.sizeof[-2-len];
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfShort:
                case ClassDescriptor.tpArrayOfChar:
                {
                    int len = Bytes.unpack4(obj, offs);
                    offs += 4;
                    if (len > 0) { 
                        offs += len*2;
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfInt:
                case ClassDescriptor.tpArrayOfEnum:
                case ClassDescriptor.tpArrayOfFloat:
                {
                    int len = Bytes.unpack4(obj, offs);
                    offs += 4;
                    if (len > 0) { 
                        offs += len*4;
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfLong:
                case ClassDescriptor.tpArrayOfDouble:
                case ClassDescriptor.tpArrayOfDate:
                {
                    int len = Bytes.unpack4(obj, offs);
                    offs += 4;
                    if (len > 0) { 
                        offs += len*8;
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfString:
                {
                    int len = Bytes.unpack4(obj, offs);
                    offs += 4;
                    while (--len >= 0) {
                        offs = Bytes.skipString(obj, offs);
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfObject:
                {
                    int len = Bytes.unpack4(obj, offs);
                    offs += 4;
                    while (--len >= 0) {
                        offs = markObjectReference(obj, offs);
                    }
                    continue;
                }
                case ClassDescriptor.tpLink:
                {
                    int len = Bytes.unpack4(obj, offs);
                    offs += 4;
                    while (--len >= 0) {
                        markOid(Bytes.unpack4(obj, offs));
                        offs += 4;
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfValue:
                {
                    int len = Bytes.unpack4(obj, offs);
                    offs += 4;
                    ClassDescriptor valueDesc = fd.valueDesc;
                    while (--len >= 0) {
                        offs = markObject(obj, offs, valueDesc);
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfRaw:
                {
                    int len = Bytes.unpack4(obj, offs);
                    offs += 8;
                    while (--len >= 0) {
                        offs = markObjectReference(obj, offs);
                    }
                    continue;
                }
            }
        }
        return offs;
    }

    /**
     * This method is used internally by Perst to get transaction context associated with current thread.
     * But it can be also used by application to get transaction context, store it in some variable and
     * use in another thread. I will make it possible to share one transaction between multiple threads.
     * @return transaction context associated with current thread
     */     
    public ThreadTransactionContext getTransactionContext() { 
        return (ThreadTransactionContext)transactionContext.get();
    }

    /**
     * Associate transaction context with the thread
     * This method can be used by application to share the same transaction between multiple threads
     * @param ctx new transaction context 
     * @return transaction context previously associated with this thread
     */     
    public ThreadTransactionContext setTransactionContext(ThreadTransactionContext ctx) { 
        ThreadTransactionContext oldCtx = (ThreadTransactionContext)transactionContext.get();
        transactionContext.set(ctx);
        return oldCtx;
    }

    public void beginThreadTransaction(int mode)
    {
        switch (mode) {
        case SERIALIZABLE_TRANSACTION:
            if (multiclientSupport) { 
                throw new IllegalArgumentException("Illegal transaction mode");
            }
            useSerializableTransactions = true;
            getTransactionContext().nested += 1;;
            break;
        case EXCLUSIVE_TRANSACTION:
        case COOPERATIVE_TRANSACTION:
            if (multiclientSupport) { 
                if (mode ==  EXCLUSIVE_TRANSACTION) { 
                    transactionLock.exclusiveLock();
                } else {
                    transactionLock.sharedLock();
                }
                synchronized (transactionMonitor) {
                    if (nNestedTransactions++ == 0) { 
                        file.lock(mode == READ_ONLY_TRANSACTION);
                        byte[] buf = new byte[Header.sizeof];
                        int rc = file.read(0, buf);
                        if (rc > 0 && rc < Header.sizeof) { 
                            throw new StorageError(StorageError.DATABASE_CORRUPTED);
                        }
                        header.unpack(buf);
                        int curr = header.curr;
                        currIndex = curr;
                        currIndexSize = header.root[1-curr].indexUsed;
                        committedIndexSize = currIndexSize;
                        usedSize = header.root[curr].size;
                        
                        if (header.transactionId != transactionId) { 
                            if (bitmapPageAvailableSpace != null) { 
                                for (int i = 0; i < bitmapPageAvailableSpace.length; i++) { 
                                    bitmapPageAvailableSpace[i] = Integer.MAX_VALUE;
                                }       
                            } 
                            objectCache.clear();
                            pool.clear();
                            transactionId = header.transactionId;
                        }
                    }
                }
            } else { 
                synchronized (transactionMonitor) {
                    if (scheduledCommitTime != Long.MAX_VALUE) { 
                        nBlockedTransactions += 1;
                        while (System.currentTimeMillis() >= scheduledCommitTime) { 
                            try { 
                                transactionMonitor.wait();
                            } catch (InterruptedException x) {}
                        }
                        nBlockedTransactions -= 1;
                    }
                    nNestedTransactions += 1;
                }           
                if (mode == EXCLUSIVE_TRANSACTION) { 
                    transactionLock.exclusiveLock();
                } else { 
                    transactionLock.sharedLock();
                }
            }
            break;
        default:
            throw new IllegalArgumentException("Illegal transaction mode");
        }
    }

    public void endThreadTransaction() { 
        endThreadTransaction(Integer.MAX_VALUE);
    }

    public void endThreadTransaction(int maxDelay)
    {
        if (multiclientSupport) { 
            if (maxDelay != Integer.MAX_VALUE) { 
                throw new IllegalArgumentException("Delay is not supported for global transactions");
            }
            synchronized (transactionMonitor) { 
                transactionLock.unlock();
                if (nNestedTransactions != 0) { // may be everything is already aborted
                    if (--nNestedTransactions == 0) { 
                        commit();
                        pool.flush();
                        file.unlock();
                    }
                }
            }       
            return;
        }  
        ThreadTransactionContext ctx = getTransactionContext();
        if (ctx.nested != 0) { // serializable transaction
            if (--ctx.nested == 0) { 
                ArrayList modified = ctx.modified;
                ArrayList deleted = ctx.deleted;
                Map locked = ctx.locked;
                synchronized (backgroundGcMonitor) { 
                    synchronized(this) { 
                        synchronized (objectCache) { 
                            for (int i = modified.size(); --i >= 0;) { 
                                store(modified.get(i));
                            } 
                            for (int i = deleted.size(); --i >= 0;) { 
                                deallocateObject0(deleted.get(i));
                            } 
                            if (modified.size() + deleted.size() > 0) { 
                                commit0();
                            }
                        }
                    }
                }
                Iterator iterator = locked.values().iterator();
                while (iterator.hasNext()) { 
                    ((IResource)iterator.next()).reset();
                }
                modified.clear();
                deleted.clear();
                locked.clear();
            } 
        } else { // exclusive or cooperative transaction        
            synchronized (transactionMonitor) { 
                transactionLock.unlock();
                
                if (nNestedTransactions != 0) { // may be everything is already aborted
                    if (--nNestedTransactions == 0) { 
                        nCommittedTransactions += 1;
                        commit();
                        scheduledCommitTime = Long.MAX_VALUE;
                        if (nBlockedTransactions != 0) { 
                            transactionMonitor.notifyAll();
                        }
                    } else {
                        if (maxDelay != Integer.MAX_VALUE) { 
                            long nextCommit = System.currentTimeMillis() + maxDelay;
                            if (nextCommit < scheduledCommitTime) { 
                                scheduledCommitTime = nextCommit;
                            }
                            if (maxDelay == 0) { 
                                int n = nCommittedTransactions;
                                nBlockedTransactions += 1;
                                do { 
                                try { 
                                    transactionMonitor.wait();
                                } catch (InterruptedException x) {}
                                } while (nCommittedTransactions == n);
                                nBlockedTransactions -= 1;
                            }                                   
                        }
                    }
                }
            }
        }
    }


    public void rollbackThreadTransaction()
    {
        if (multiclientSupport) { 
            synchronized (transactionMonitor) { 
                transactionLock.reset();
                nNestedTransactions = 0;
                rollback();
                file.unlock();
            }       
            return;
        }  
        ThreadTransactionContext ctx = getTransactionContext();
        if (ctx.nested != 0) { // serializable transaction
            ArrayList modified = ctx.modified;
            Map locked = ctx.locked;
            synchronized (this) { 
                synchronized (objectCache) {
                    int i = modified.size();
                    while (--i >= 0) { 
                        Object obj = modified.get(i);
                        int oid = getOid(obj);
                        Assert.that(oid != 0);
                        invalidate(obj);
                        if (getPos(oid) == 0) {
                            freeId(oid);
                            objectCache.remove(oid);
                        } else { 
                            loadStub(oid, obj, obj.getClass());
                            objectCache.clearDirty(obj);
                        }
                    }
                }
            }
            Iterator iterator = locked.values().iterator();
            while (iterator.hasNext()) { 
                ((IResource)iterator.next()).reset();
            }
            ctx.nested = 0; 
            modified.clear();
            ctx.deleted.clear();
            locked.clear();
        } else { 
            synchronized (transactionMonitor) { 
                transactionLock.reset();
                nNestedTransactions = 0;
                if (nBlockedTransactions != 0) { 
                    transactionMonitor.notifyAll();
                }
                rollback();
            }
        }
    }
            
    public/*protected*/  boolean lockObject(Object obj) { 
        if (useSerializableTransactions) { 
            ThreadTransactionContext ctx = getTransactionContext();
            if (ctx.nested != 0) { // serializable transaction
                return ctx.locked.put(obj, obj) == null;
            }
        }
        return true;
    }
         
    public void close() 
    {
        synchronized (backgroundGcMonitor) { 
            commit();
            opened = false;
        }
        if (gcThread != null) {             
            gcThread.activate();
            try { 
                gcThread.join();
            } catch (InterruptedException x) {}
        }
        if (isDirty()) { 
            Page pg = pool.putPage(0);
            header.pack(pg.data);
            pool.flush();
            pool.modify(pg);
            header.dirty = false;
            header.pack(pg.data);
            pool.unfix(pg);
            pool.flush();
        }
        pool.close();
        // make GC easier
        pool = null;
        objectCache = null;
        classDescMap = null;
        bitmapPageAvailableSpace = null;
        dirtyPagesMap  = null;
        descList = null;
    }

    public synchronized void exportXML(java.io.Writer writer) throws java.io.IOException
    {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        objectCache.flush();
        int rootOid = header.root[1-currIndex].rootObject;
        if (rootOid != 0) { 
            XMLExporter xmlExporter = new XMLExporter(this, writer);
            xmlExporter.exportDatabase(rootOid);
        }
    }

    public synchronized void importXML(java.io.Reader reader) throws XMLImportException
    {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        XMLImporter xmlImporter = new XMLImporter(this, reader);
        xmlImporter.importDatabase();
    }

    private boolean getBooleanValue(Object value) { 
        if (value instanceof Boolean) { 
            return ((Boolean)value).booleanValue();
        } else if (value instanceof String) {
            String s = (String)value;
            if ("true".equalsIgnoreCase(s) || "t".equalsIgnoreCase(s) || "1".equals(s)) { 
                return true;
            } else if ("false".equalsIgnoreCase(s) || "f".equalsIgnoreCase(s) || "0".equals(s)) { 
                return false;
            }
        }
        throw new StorageError(StorageError.BAD_PROPERTY_VALUE);
    }

    private long getIntegerValue(Object value) { 
        if (value instanceof Number) { 
            return ((Number)value).longValue();
        } else if (value instanceof String) {
            try { 
                return Long.parseLong((String)value, 10);
            } catch (NumberFormatException x) {}
        }
        throw new StorageError(StorageError.BAD_PROPERTY_VALUE);
    }

     
    public void setProperties(Properties props) 
    {
        String value;
        properties.putAll(props);
        if ((value = props.getProperty("perst.implicit.values")) != null) { 
            ClassDescriptor.treateAnyNonPersistentObjectAsValue = getBooleanValue(value);
        } 
        if ((value = props.getProperty("perst.serialize.transient.objects")) != null) { 
            ClassDescriptor.serializeNonPersistentObjects = getBooleanValue(value);
        } 
        if ((value = props.getProperty("perst.object.cache.init.size")) != null) { 
            objectCacheInitSize = (int)getIntegerValue(value);
            if (objectCacheInitSize <= 0) { 
                throw new IllegalArgumentException("Initial object cache size should be positive");
            }            
        }
        if ((value = props.getProperty("perst.object.cache.kind")) != null) { 
            cacheKind = value;
        }
        if ((value = props.getProperty("perst.object.index.init.size")) != null) { 
            initIndexSize = (int)getIntegerValue(value);
        }
        if ((value = props.getProperty("perst.extension.quantum")) != null) { 
            extensionQuantum = getIntegerValue(value);
        } 
        if ((value = props.getProperty("perst.gc.threshold")) != null) { 
            gcThreshold = getIntegerValue(value);
        }
        if ((value = props.getProperty("perst.file.readonly")) != null) { 
            readOnly = getBooleanValue(value);
        }
        if ((value = props.getProperty("perst.file.noflush")) != null) { 
            noFlush = getBooleanValue(value);
        }
        if ((value = props.getProperty("perst.alternative.btree")) != null) { 
            alternativeBtree = getBooleanValue(value);
        }
        if ((value = props.getProperty("perst.background.gc")) != null) { 
            backgroundGc = getBooleanValue(value);
        }
        if ((value = props.getProperty("perst.string.encoding")) != null) { 
            encoding = value;
        }
        if ((value = props.getProperty("perst.lock.file")) != null) { 
            lockFile = getBooleanValue(value);
        }
        if ((value = props.getProperty("perst.replication.ack")) != null) { 
            replicationAck = getBooleanValue(value);
        }
        if ((value = props.getProperty("perst.concurrent.iterator")) != null) { 
            concurrentIterator = getBooleanValue(value);
        }
        if ((value = props.getProperty("perst.slave.connection.timeout")) != null) { 
            slaveConnectionTimeout = (int)getIntegerValue(value);
        }
        if ((value = props.getProperty("perst.force.store")) != null) { 
            forceStore = getBooleanValue(value);
        }
        if ((value = props.getProperty("perst.page.pool.lru.limit")) != null) { 
            pagePoolLruLimit = getIntegerValue(value);
        }
        if ((value = props.getProperty("perst.multiclient.support")) != null) { 
            multiclientSupport = getBooleanValue(value);
        }
        if (multiclientSupport && backgroundGc) { 
            throw new IllegalArgumentException("In mutliclient access mode bachround GC is not supported");
        }
    }

    public void setProperty(String name, Object value)
    {
        properties.put(name, value);
        if (name.equals("perst.implicit.values")) { 
            ClassDescriptor.treateAnyNonPersistentObjectAsValue = getBooleanValue(value);
        } else if (name.equals("perst.serialize.transient.objects")) { 
            ClassDescriptor.serializeNonPersistentObjects = getBooleanValue(value);
        } else if (name.equals("perst.object.cache.init.size")) { 
            objectCacheInitSize = (int)getIntegerValue(value);
            if (objectCacheInitSize <= 0) { 
                throw new IllegalArgumentException("Initial object cache size should be positive");
            }            
        } else if (name.equals("perst.object.cache.kind")) { 
            cacheKind = (String)value;
        } else if (name.equals("perst.object.index.init.size")) { 
            initIndexSize = (int)getIntegerValue(value);
        } else if (name.equals("perst.extension.quantum")) { 
            extensionQuantum = getIntegerValue(value);
        } else if (name.equals("perst.gc.threshold")) { 
            gcThreshold = getIntegerValue(value);
        } else if (name.equals("perst.file.readonly")) { 
            readOnly = getBooleanValue(value);
        } else if (name.equals("perst.file.noflush")) { 
            noFlush = getBooleanValue(value);
        } else if (name.equals("perst.alternative.btree")) { 
            alternativeBtree = getBooleanValue(value);
        } else if (name.equals("perst.background.gc")) {
            backgroundGc = getBooleanValue(value);
        } else if (name.equals("perst.string.encoding")) { 
            encoding = (value == null) ? null : value.toString();
        } else if (name.equals("perst.lock.file")) { 
            lockFile = getBooleanValue(value);
        } else if (name.equals("perst.replication.ack")) { 
            replicationAck = getBooleanValue(value);
        } else if (name.equals("perst.concurrent.iterator")) { 
            concurrentIterator = getBooleanValue(value);
        } else if (name.equals("perst.slave.connection.timeout")) { 
            slaveConnectionTimeout = (int)getIntegerValue(value);
        } else if (name.equals("perst.force.store")) { 
            forceStore = getBooleanValue(value);
        } else if (name.equals("perst.page.pool.lru.limit")) {
            pagePoolLruLimit = getIntegerValue(value);
        } else if (name.equals("perst.multiclient.support")) { 
            multiclientSupport = getBooleanValue(value);
        } else { 
            throw new StorageError(StorageError.NO_SUCH_PROPERTY);
        }
        if (multiclientSupport && backgroundGc) { 
            throw new IllegalArgumentException("In mutliclient access mode bachround GC is not supported");
        }
    }

    public Object getProperty(String name)
    {
        return properties.get(name);
    }

    public Properties getProperties()
    {
        return properties;
    }
    

    public StorageListener setListener(StorageListener listener)
    {
        StorageListener prevListener = this.listener;
        this.listener = listener;
        return prevListener;
    }

    public synchronized Object getObjectByOID(int oid)
    {
        return oid == 0 ? null : lookupObject(oid, null);
    }

    public/*protected*/ synchronized void modifyObject(Object obj) {
        synchronized(objectCache) { 
            if (!isModified(obj)) { 
                if (useSerializableTransactions) { 
                    ThreadTransactionContext ctx = getTransactionContext();
                    if (ctx.nested != 0) { // serializable transaction
                        ctx.modified.add(obj);
                        return;
                    }
                }
                objectCache.setDirty(obj);
            }
        }
    }
    
    public/*protected*/ synchronized void storeObject(Object obj) 
    {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        if (useSerializableTransactions && getTransactionContext().nested != 0) { 
            // Store should not be used in serializable transaction mode
            throw new StorageError(StorageError.INVALID_OPERATION, "store object");
        }
        synchronized (objectCache) { 
            storeObject0(obj, false);
        }
    }

    public/*protected*/ void storeFinalizedObject(Object obj) 
    {
        if (opened) { 
            synchronized (objectCache) { 
                if (getOid(obj) != 0) { 
                    storeObject0(obj, true);
                }
            }
        }
    }

    public synchronized int makePersistent(Object obj) 
    {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        if (obj == null) {
            return 0;
        }
        int oid = getOid(obj);
        if (oid != 0) {
            return oid;
        }
        if (forceStore && (!useSerializableTransactions || getTransactionContext().nested == 0)) {
            synchronized (objectCache) { 
                storeObject0(obj, false);
            }
            return getOid(obj);
        } else { 
            synchronized (objectCache) {
                oid = allocateId();
                assignOid(obj, oid, false);
                setPos(oid, 0);
                objectCache.put(oid, obj);
                modify(obj);
                return oid;
            }
        }
    }

    private final CustomAllocator getCustomAllocator(Class cls) { 
        Object a = customAllocatorMap.get(cls);
        if (a != null) { 
            return a == defaultAllocator ? null : (CustomAllocator)a;
        }
        Class superclass = cls.getSuperclass();
        if (superclass != null) { 
            CustomAllocator alloc = getCustomAllocator(superclass);
            if (alloc != null) { 
                customAllocatorMap.put(cls, alloc);
                return alloc;
            }
        }
        Class[] interfaces = cls.getInterfaces();
        for (int i = 0; i < interfaces.length; i++) { 
            CustomAllocator alloc = getCustomAllocator(interfaces[i]);
            if (alloc != null) { 
                customAllocatorMap.put(cls, alloc);
                return alloc;
            }
        }
        customAllocatorMap.put(cls, defaultAllocator);
        return null;
    }

    private final void storeObject0(Object obj, boolean finalized) 
    {
        if (obj instanceof IStoreable) { 
            ((IPersistent)obj).onStore();
        }
        int oid = getOid(obj);
        boolean newObject = false;
        if (oid == 0) { 
            oid = allocateId();
            if (!finalized) { 
                objectCache.put(oid, obj);
            }
            assignOid(obj, oid, false);
            newObject = true;
        } else if (isModified(obj)) {
            objectCache.clearDirty(obj);
        }
        byte[] data = packObject(obj, finalized);
        long pos;
        int newSize = ObjectHeader.getSize(data, 0);
        CustomAllocator allocator = (customAllocatorMap != null) ? getCustomAllocator(obj.getClass()) : null;
        if (newObject || (pos = getPos(oid)) == 0) { 
            pos = allocator != null ? allocator.allocate(newSize) : allocate(newSize, 0);
            setPos(oid, pos | dbModifiedFlag);
        } else {
            int offs = (int)pos & (Page.pageSize-1);
            if ((offs & (dbFreeHandleFlag|dbPageObjectFlag)) != 0) { 
                throw new StorageError(StorageError.DELETED_OBJECT);
            }
            Page pg = pool.getPage(pos - offs);
            int size = ObjectHeader.getSize(pg.data, offs & ~dbFlagsMask);
            pool.unfix(pg);
            if ((pos & dbModifiedFlag) == 0) { 
                if (allocator != null) { 
                    allocator.free(pos & ~dbFlagsMask, size);
                    pos = allocator.allocate(newSize);
                } else { 
                    cloneBitmap(pos & ~dbFlagsMask, size);
                    pos = allocate(newSize, 0);
                }
                setPos(oid, pos | dbModifiedFlag);
            } else {
                pos &= ~dbFlagsMask;
                if (newSize != size) { 
                    if (allocator != null) {
                        long newPos = allocator.reallocate(pos, size, newSize);
                        if (newPos != pos) { 
                            pos = newPos;
                            setPos(oid, pos | dbModifiedFlag);                   
                        } else if (newSize < size) { 
                            ObjectHeader.setSize(data, 0, size);
                        }
                    } else { 
                        if (((newSize + dbAllocationQuantum - 1) & ~(dbAllocationQuantum-1))
                            > ((size + dbAllocationQuantum - 1) & ~(dbAllocationQuantum-1)))
                        { 
                            long newPos = allocate(newSize, 0);
                            cloneBitmap(pos, size);
                            free(pos, size);
                            pos = newPos;
                            setPos(oid, pos | dbModifiedFlag);
                        } else if (newSize < size) { 
                            ObjectHeader.setSize(data, 0, size);
                        }
                    }
                }
            }
        }        
        modified = true;
        pool.put(pos, data, newSize);
    }

    public/*protected*/ synchronized void loadObject(Object obj) {
        if (isRaw(obj)) { 
            loadStub(getOid(obj), obj, obj.getClass());
        }
    }

    final synchronized Object lookupObject(int oid, Class cls) {
        Object obj = objectCache.get(oid);
        if (obj == null || isRaw(obj)) { 
            obj = loadStub(oid, obj, cls);
        }
        return obj;
    }
 
    protected int swizzle(Object obj, boolean finalized) { 
        int oid = 0;
        if (obj != null) { 
            if (!isPersistent(obj)) { 
                storeObject0(obj, finalized);
            }
            oid = getOid(obj);
        }
        return oid;
    }
        
    final ClassDescriptor findClassDescriptor(int oid) { 
        return (ClassDescriptor)lookupObject(oid, ClassDescriptor.class);
    }



    class ByteArrayObjectInputStream extends PerstInputStream
    {
        private byte[] buf;
        private Object  parent;
        private boolean recursiveLoading;
        private boolean markReferences;

        ByteArrayObjectInputStream(byte[] buf, int offs, Object parent, boolean resursiveLoading, boolean markReferenes) {
            super(new ByteArrayInputStream(buf, offs, buf.length - offs));
            this.buf = buf;
            this.parent = parent;
            this.recursiveLoading = recursiveLoading;
            this.markReferences = markReferences;
        }
            
        int getPosition() throws IOException { 
            return buf.length - in.available();
        }

        public String readString() throws IOException {
            int offs = getPosition();
            ArrayPos pos = new ArrayPos(buf, offs);
            String str = Bytes.unpackString(pos, encoding);
            in.skip(pos.offs - offs);
            return str;            
        }

        public Object readObject() throws IOException {
            int offs = getPosition();
            Object obj = null;
            if (markReferences) { 
                offs = markObjectReference(buf, offs) - offs;
            } else {  
                try { 
                    ArrayPos pos = new ArrayPos(buf, offs);
                    obj = unswizzle(pos, Object.class, parent, recursiveLoading);
                    offs = pos.offs - offs;
                } catch(Exception x) { 
                    throw new StorageError(StorageError.ACCESS_VIOLATION, x);                    
                }
            }
            in.skip(offs);
            return obj;
        }
    }


    protected Object unswizzle(int oid, Class cls, boolean recursiveLoading) { 
        if (oid == 0) { 
            return null;
        } 
        if (recursiveLoading) {
            return lookupObject(oid, cls);
        }
        Object stub = objectCache.get(oid);
        if (stub != null) { 
            return stub;
        }
        ClassDescriptor desc;
        if (cls == Object.class
            || (desc = findClassDescriptor(cls)) == null
            || desc.hasSubclasses) 
        { 
            long pos = getPos(oid);
            int offs = (int)pos & (Page.pageSize-1);
            if ((offs & (dbFreeHandleFlag|dbPageObjectFlag)) != 0) { 
                throw new StorageError(StorageError.DELETED_OBJECT);
            }
            Page pg = pool.getPage(pos - offs);
            int typeOid = ObjectHeader.getType(pg.data, offs & ~dbFlagsMask);
            pool.unfix(pg);
            desc = findClassDescriptor(typeOid);
        }
        stub = desc.newInstance();
        assignOid(stub, oid, true);
        objectCache.put(oid, stub);
        return stub;
    }

    final Object loadStub(int oid, Object obj, Class cls)
    {
        long pos = getPos(oid);
        if ((pos & (dbFreeHandleFlag|dbPageObjectFlag)) != 0) { 
            throw new StorageError(StorageError.DELETED_OBJECT);
        }
        byte[] body = pool.get(pos & ~dbFlagsMask);
        ClassDescriptor desc;
        int typeOid = ObjectHeader.getType(body, 0);
        if (typeOid == 0) { 
            desc = findClassDescriptor(cls);
        } else { 
            desc = findClassDescriptor(typeOid);
        }
        if (obj == null) {                 
            obj = desc.customSerializable ? serializer.create(desc.cls) : desc.newInstance();
            objectCache.put(oid, obj);
        }
        assignOid(obj, oid, false);
        try { 
            if (obj instanceof SelfSerializable) {
                ((SelfSerializable)obj).unpack(new ByteArrayObjectInputStream(body, ObjectHeader.sizeof, obj, recursiveLoading(obj), false));
            } else if (desc.customSerializable) {
                serializer.unpack(obj, new ByteArrayObjectInputStream(body, ObjectHeader.sizeof, obj, recursiveLoading(obj), false));
            } else { 
                unpackObject(obj, desc, recursiveLoading(obj), body, ObjectHeader.sizeof, obj);
            }
        } catch (Exception x) { 
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
        if (obj instanceof ILoadable) { 
            ((IPersistent)obj).onLoad();
        }
        return obj;
    }

    class PersistentObjectInputStream extends ObjectInputStream { 
        PersistentObjectInputStream(InputStream in) throws IOException {
            super(in);
            enableResolveObject(true);
        }
        
        protected Object resolveObject(Object obj) throws IOException {
            int oid = getOid(obj);
            if (oid != 0) { 
                return lookupObject(oid, obj.getClass());
            }
            return obj;
        }
        
        protected Class resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
            String classLoaderName = null;
            if (loaderMap != null) { 
                classLoaderName = (String)readObject();
            }
            ClassLoader cl = (classLoaderName != null) 
                ? findClassLoader(classLoaderName) : loader;
            if (cl != null) { 
                try { 
                    return Class.forName(desc.getName(), false, cl);
                } catch (ClassNotFoundException x) {}
            } 
            return super.resolveClass(desc);
        }
    }

    public class PersistentObjectOutputStream extends ObjectOutputStream { 
        PersistentObjectOutputStream(OutputStream out) throws IOException {
            super(out);
        }
        
        public Storage getStorage() { 
            return StorageImpl.this;
        }
    }

    public class AnnotatedPersistentObjectOutputStream extends PersistentObjectOutputStream { 
        AnnotatedPersistentObjectOutputStream(OutputStream out) throws IOException {
            super(out);
        }

        protected void annotateClass(Class cls) throws IOException {
            ClassLoader loader = cls.getClassLoader();
            writeObject((loader instanceof INamedClassLoader) 
                        ? ((INamedClassLoader)loader).getName() : null);
        }
    }       

    final int skipObjectReference(byte[] obj, int offs) throws Exception
    {
        int oid = Bytes.unpack4(obj, offs);
        int len;
        offs += 4;
        if (oid < 0) { 
            int tid = -1 - oid;
            switch (tid) {
            case ClassDescriptor.tpString:
                offs = Bytes.skipString(obj, offs);
                break;
            case ClassDescriptor.tpArrayOfByte:
                len = Bytes.unpack4(obj, offs);   
                offs += len + 4;                    
                break;
            case ClassDescriptor.tpArrayOfObject:
                len = Bytes.unpack4(obj, offs);   
                offs += 4;
                for (int i = 0; i < len; i++) { 
                    offs = skipObjectReference(obj, offs);
                }
                break;
            case ClassDescriptor.tpArrayOfRaw:
                len = Bytes.unpack4(obj, offs);   
                offs += 8;
                for (int i = 0; i < len; i++) { 
                    offs = skipObjectReference(obj, offs);
                }
                break;
            default:
                if (tid >= ClassDescriptor.tpValueTypeBias) { 
                    int typeOid = - ClassDescriptor.tpValueTypeBias - oid;
                    ClassDescriptor desc = findClassDescriptor(typeOid);
                    if (desc.isCollection) { 
                        len = Bytes.unpack4(obj, offs);   
                        offs += 4;
                        for (int i = 0; i < len; i++) { 
                            offs = skipObjectReference(obj, offs);
                        }                            
                    } else { 
                        offs = unpackObject(null, findClassDescriptor(typeOid), false, obj, offs, null);
                    }
                } else {
                    offs += ClassDescriptor.sizeof[tid];
                }
            }
        }       
        return offs;
    }

    final Object unswizzle(ArrayPos obj, Class cls, Object parent, boolean recursiveLoading) 
      throws Exception
    {
        byte[] body = obj.body;
        int offs = obj.offs;
        int oid = Bytes.unpack4(body, offs);
        offs += 4;
        Object val;
        if (oid < 0) {
            switch (-1-oid) {
            case ClassDescriptor.tpBoolean:
                val = body[offs++] != 0;
                break;
            case ClassDescriptor.tpByte:
                val = body[offs++];
                break;
            case ClassDescriptor.tpChar:
                val = (char)Bytes.unpack2(body, offs);
                offs += 2;
                break;
            case ClassDescriptor.tpShort:
                val = Bytes.unpack2(body, offs);
                offs += 2;
                break;
            case ClassDescriptor.tpInt:
                val = Bytes.unpack4(body, offs);
                offs += 4;
                break;
            case ClassDescriptor.tpLong:
                val = Bytes.unpack8(body, offs);
                offs += 8;
                break;
            case ClassDescriptor.tpFloat:
                val = Bytes.unpackF4(body, offs);
                offs += 4;
                break;
            case ClassDescriptor.tpDouble:
                val = Bytes.unpackF8(body, offs);
                offs += 8;
                break;
            case ClassDescriptor.tpDate:
                val = new Date(Bytes.unpack8(body, offs));
                offs += 8;
                break;
            case ClassDescriptor.tpString:
                obj.offs = offs;
                return Bytes.unpackString(obj, encoding);
            case ClassDescriptor.tpLink:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    Object[] arr = new Object[len];
                    for (int j = 0; j < len; j++) { 
                        int elemOid = Bytes.unpack4(body, offs);
                        offs += 4;
                        if (elemOid != 0) { 
                            arr[j] = new PersistentStub(this, elemOid);
                        }
                    }
                    val = new LinkImpl(this, arr, parent);
                    break;
                }
            case ClassDescriptor.tpArrayOfByte:
                {
                    int len = Bytes.unpack4(body, offs);   
                    offs += 4;
                    byte[] arr = new byte[len];
                    System.arraycopy(body, offs, arr, 0, len);
                    offs += len;
                    val = arr;  
                    break;
                }    
            case ClassDescriptor.tpArrayOfBoolean:
                {
                    int len = Bytes.unpack4(body, offs);   
                    offs += 4;
                    boolean[] arr = new boolean[len];
                    for (int j = 0; j < len; j++) { 
                        arr[j] = body[offs++] != 0;
                    }
                    val = arr;  
                    break;
                }    
            case ClassDescriptor.tpArrayOfShort:
                {
                    int len = Bytes.unpack4(body, offs);   
                    offs += 4;
                    short[] arr = new short[len];
                    for (int j = 0; j < len; j++) { 
                        arr[j] = Bytes.unpack2(body, offs);
                        offs += 2;
                    }
                    val = arr;  
                    break;
                }    
            case ClassDescriptor.tpArrayOfChar:
                {
                    int len = Bytes.unpack4(body, offs);   
                    offs += 4;
                    char[] arr = new char[len];
                    for (int j = 0; j < len; j++) { 
                        arr[j] = (char)Bytes.unpack2(body, offs);
                        offs += 2;
                    }
                    val = arr;  
                    break;
                }    
            case ClassDescriptor.tpArrayOfInt:
                {
                    int len = Bytes.unpack4(body, offs);   
                    offs += 4;
                    int[] arr = new int[len];
                    for (int j = 0; j < len; j++) { 
                        arr[j] = Bytes.unpack4(body, offs);
                        offs += 4;
                    }
                    val = arr;  
                    break;
                }    
            case ClassDescriptor.tpArrayOfLong:
                {
                    int len = Bytes.unpack4(body, offs);   
                    offs += 4;
                    long[] arr = new long[len];
                    for (int j = 0; j < len; j++) { 
                        arr[j] = Bytes.unpack8(body, offs);
                        offs += 8;
                    }
                    val = arr;  
                    break;
                }    
            case ClassDescriptor.tpArrayOfFloat:
                {
                    int len = Bytes.unpack4(body, offs);   
                    offs += 4;
                    float[] arr = new float[len];
                    for (int j = 0; j < len; j++) { 
                        arr[j] = Bytes.unpackF4(body, offs);
                        offs += 4;
                    }
                    val = arr;  
                    break;
                }    
            case ClassDescriptor.tpArrayOfDouble:
                {
                    int len = Bytes.unpack4(body, offs);   
                    offs += 4;
                    double[] arr = new double[len];
                    for (int j = 0; j < len; j++) { 
                        arr[j] = Bytes.unpackF8(body, offs);
                        offs += 8;
                    }
                    val = arr;  
                    break;
                }    
            case ClassDescriptor.tpArrayOfObject:
                {
                    int len = Bytes.unpack4(body, offs);   
                    obj.offs = offs + 4;
                    Object[] arr = new Object[len];
                    for (int j = 0; j < len; j++) { 
                        arr[j] = unswizzle(obj, Object.class, parent, recursiveLoading);
                    }
                    return arr;
                }
            case ClassDescriptor.tpArrayOfRaw:
                {
                    int len = Bytes.unpack4(body, offs);   
                    int typeOid = Bytes.unpack4(body, offs + 4);   
                    obj.offs = offs + 8;
                    ClassDescriptor desc = findClassDescriptor(typeOid);
                    Class elemType = desc.cls;
                    Object arr = Array.newInstance(elemType, len);
                    for (int j = 0; j < len; j++) { 
                        Array.set(arr, j, unswizzle(obj, elemType, parent, recursiveLoading));
                    }
                    return arr;
                }
            case ClassDescriptor.tpCustom:
                { 
                    ByteArrayObjectInputStream in = new ByteArrayObjectInputStream(body, offs, parent, recursiveLoading, false);
                    val = serializer.unpack(in);
                    offs = in.getPosition();
                    break;
                }
            default:                
                if (oid < -ClassDescriptor.tpValueTypeBias) { 
                    int typeOid = - ClassDescriptor.tpValueTypeBias - oid;
                    ClassDescriptor desc = findClassDescriptor(typeOid);
                    val = desc.newInstance();
                    if (desc.isCollection) { 
                        int len = Bytes.unpack4(body, offs);   
                        obj.offs = offs + 4;
                        Collection collection = (Collection)val;
                        for (int i = 0; i < len; i++) {  
                            collection.add(unswizzle(obj, Object.class, parent, recursiveLoading));
                        }                            
                        return collection;
                    } else { 
                        offs = unpackObject(val, desc, recursiveLoading, body, offs, parent);                        
                    }
                } else {
                    throw new StorageError(StorageError.UNSUPPORTED_TYPE);
                }
            }       
        } else {
            val = unswizzle(oid, cls, recursiveLoading);
        }
        obj.offs = offs;
        return val;
    }

    final int unpackObject(Object obj, ClassDescriptor desc, boolean recursiveLoading, byte[] body, int offs, Object parent) 
      throws Exception
    {
        ClassDescriptor.FieldDescriptor[] all = desc.allFields;
        ReflectionProvider provider = ClassDescriptor.getReflectionProvider();
        int len;

        for (int i = 0, n = all.length; i < n; i++) { 
            ClassDescriptor.FieldDescriptor fd = all[i];
            Field f = fd.field;

            if (f == null || obj == null) { 
                switch (fd.type) { 
                case ClassDescriptor.tpBoolean:
                case ClassDescriptor.tpByte:
                    offs += 1;
                    continue;
                case ClassDescriptor.tpChar:
                case ClassDescriptor.tpShort:
                    offs += 2;
                    continue;
                case ClassDescriptor.tpInt:
                case ClassDescriptor.tpFloat:
                case ClassDescriptor.tpEnum:
                    offs += 4;
                    continue;
                case ClassDescriptor.tpObject:
                    offs = skipObjectReference(body, offs);
                    continue;
                case ClassDescriptor.tpLong:
                case ClassDescriptor.tpDouble:
                case ClassDescriptor.tpDate:
                    offs += 8;
                    continue;
                case ClassDescriptor.tpString:
                    offs = Bytes.skipString(body, offs);
                    continue;
                case ClassDescriptor.tpValue:
                    offs = unpackObject(null, fd.valueDesc, recursiveLoading, body, offs, parent);
                    continue;
                case ClassDescriptor.tpRaw:
                case ClassDescriptor.tpArrayOfByte:
                case ClassDescriptor.tpArrayOfBoolean:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len > 0) { 
                        offs += len;
                    } else if (len < -1) { 
                        offs += ClassDescriptor.sizeof[-2-len];
                    } 
                    continue;
                case ClassDescriptor.tpCustom:
                    { 
                        ByteArrayObjectInputStream in = new ByteArrayObjectInputStream(body, offs, parent, recursiveLoading, false);
                        serializer.unpack(in);
                        offs = in.getPosition();
                        continue;
                    }
                case ClassDescriptor.tpArrayOfShort:
                case ClassDescriptor.tpArrayOfChar:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len > 0) { 
                        offs += len*2;
                    }
                    continue;
                case ClassDescriptor.tpArrayOfObject:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    for (int j = 0; j < len; j++) {
                        offs = skipObjectReference(body, offs);
                    }
                    continue;
                case ClassDescriptor.tpArrayOfInt:
                case ClassDescriptor.tpArrayOfEnum:
                case ClassDescriptor.tpArrayOfFloat:
                case ClassDescriptor.tpLink:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len > 0) { 
                        offs += len*4;
                    }
                    continue;
                case ClassDescriptor.tpArrayOfLong:
                case ClassDescriptor.tpArrayOfDouble:
                case ClassDescriptor.tpArrayOfDate:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len > 0) { 
                        offs += len*8;
                    }
                    continue;
                case ClassDescriptor.tpArrayOfString:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len > 0) { 
                        for (int j = 0; j < len; j++) {
                            offs = Bytes.skipString(body, offs);
                        }
                    }
                    continue;
                case ClassDescriptor.tpArrayOfValue:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len > 0) { 
                        ClassDescriptor valueDesc = fd.valueDesc;
                        for (int j = 0; j < len; j++) { 
                            offs = unpackObject(null, valueDesc, recursiveLoading, body, offs, parent);
                        }
                    }
                    continue;
                }                
            } else if (offs < body.length) {                 
                switch (fd.type) { 
                case ClassDescriptor.tpBoolean:
                    provider.setBoolean(f, obj, body[offs++] != 0);
                    continue;
                case ClassDescriptor.tpByte:
                    provider.setByte(f, obj, body[offs++]);
                    continue;
                case ClassDescriptor.tpChar:
                    provider.setChar(f, obj, (char)Bytes.unpack2(body, offs));
                    offs += 2;
                    continue;
                case ClassDescriptor.tpShort:
                    provider.setShort(f, obj, Bytes.unpack2(body, offs));
                    offs += 2;
                    continue;
                case ClassDescriptor.tpInt:
                    provider.setInt(f, obj, Bytes.unpack4(body, offs));
                    offs += 4;
                    continue;
                case ClassDescriptor.tpLong:
                    provider.setLong(f, obj, Bytes.unpack8(body, offs));
                    offs += 8;
                    continue;
                case ClassDescriptor.tpFloat:
                    provider.setFloat(f, obj, Bytes.unpackF4(body, offs));
                    offs += 4;
                    continue;
                case ClassDescriptor.tpDouble:
                    provider.setDouble(f, obj, Bytes.unpackF8(body, offs));
                    offs += 8;
                    continue;
                case ClassDescriptor.tpEnum:
                {
                    int index = Bytes.unpack4(body, offs);
                    if (index >= 0) {
                        provider.set(f, obj, fd.field.getType().getEnumConstants()[index]);
                    } else {
                        provider.set(f, obj, null);
                    }
                    offs += 4;
                    continue;
                }
                case ClassDescriptor.tpString:
                {
                    ArrayPos pos = new ArrayPos(body, offs);
                    provider.set(f, obj, Bytes.unpackString(pos, encoding));
                    offs = pos.offs;
                    continue;
                }
                case ClassDescriptor.tpDate:
                {
                    long msec = Bytes.unpack8(body, offs);
                    offs += 8;
                    Date date = null;
                    if (msec >= 0) { 
                        date = new Date(msec);
                    }
                    provider.set(f, obj, date);
                    continue;
                }
                case ClassDescriptor.tpObject:
                {
                    ArrayPos pos = new ArrayPos(body, offs);
                    provider.set(f, obj, unswizzle(pos, f.getType(), parent, recursiveLoading));
                    offs = pos.offs;
                    continue;
                }
                case ClassDescriptor.tpValue:
                {
                    Object value = fd.valueDesc.newInstance();
                    offs = unpackObject(value, fd.valueDesc, recursiveLoading, body, offs, parent);
                    provider.set(f, obj, value);
                    continue;
                }
                case ClassDescriptor.tpRaw:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len >= 0) { 
                        ByteArrayInputStream bin = new ByteArrayInputStream(body, offs, len);
                        ObjectInputStream in = new PersistentObjectInputStream(bin);
                        provider.set(f, obj, in.readObject());
                        in.close();
                        offs += len;
                    } else if (len < 0) { 
                        Object val = null;
                        switch (-2-len) { 
                        case ClassDescriptor.tpBoolean:
                            val = Boolean.valueOf(body[offs++] != 0);
                            break;
                        case ClassDescriptor.tpByte:
                            val = new Byte(body[offs++]);
                            break;                            
                        case ClassDescriptor.tpChar:
                            val = new Character((char)Bytes.unpack2(body, offs));
                            offs += 2;
                            break;                            
                        case ClassDescriptor.tpShort:
                            val = new Short(Bytes.unpack2(body, offs));
                            offs += 2;
                            break;                            
                        case ClassDescriptor.tpInt:
                            val = new Integer(Bytes.unpack4(body, offs));
                            offs += 4;
                            break;                            
                        case ClassDescriptor.tpLong:
                            val = new Long(Bytes.unpack8(body, offs));
                            offs += 8;
                            break;                            
                        case ClassDescriptor.tpFloat:
                            val = new Float(Float.intBitsToFloat(Bytes.unpack4(body, offs)));
                            offs += 4;
                            break;                            
                        case ClassDescriptor.tpDouble:
                            val = new Double(Double.longBitsToDouble(Bytes.unpack8(body, offs)));
                            offs += 8;
                            break;                            
                        case ClassDescriptor.tpDate:
                            val = new Date(Bytes.unpack8(body, offs));
                            offs += 8;
                            break;                                                       
                        case ClassDescriptor.tpObject:
                            val = unswizzle(Bytes.unpack4(body, offs), Persistent.class, recursiveLoading);
                           offs += 4;
                        }
                        provider.set(f, obj, val);
                    }
                    continue;
                case ClassDescriptor.tpCustom:
                {
                    ByteArrayObjectInputStream in = new ByteArrayObjectInputStream(body, offs, parent, recursiveLoading, false);
                    serializer.unpack(in);
                    provider.set(f, obj, serializer.unpack(in));
                    offs = in.getPosition();
                    continue;
                }
                case ClassDescriptor.tpArrayOfByte:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        provider.set(f, obj, null);
                    } else {
                        byte[] arr = new byte[len];
                        System.arraycopy(body, offs, arr, 0, len);
                        offs += len;
                        provider.set(f, obj, arr);
                    }
                    continue;
                case ClassDescriptor.tpArrayOfBoolean:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        provider.set(f, obj, null);
                    } else {
                        boolean[] arr = new boolean[len];
                        for (int j = 0; j < len; j++) { 
                            arr[j] = body[offs++] != 0;
                        }
                        provider.set(f, obj, arr);
                    }
                    continue;
                case ClassDescriptor.tpArrayOfShort:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        provider.set(f, obj, null);
                    } else {
                        short[] arr = new short[len];
                        for (int j = 0; j < len; j++) { 
                            arr[j] = Bytes.unpack2(body, offs);
                            offs += 2;
                        }
                        provider.set(f, obj, arr);
                    }
                    continue;
                case ClassDescriptor.tpArrayOfChar:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        provider.set(f, obj, null);
                    } else {
                        char[] arr = new char[len];
                        for (int j = 0; j < len; j++) { 
                            arr[j] = (char)Bytes.unpack2(body, offs);
                            offs += 2;
                        }
                        provider.set(f, obj, arr);
                    }
                    continue;
                case ClassDescriptor.tpArrayOfInt:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        provider.set(f, obj, null);
                    } else {
                        int[] arr = new int[len];
                        for (int j = 0; j < len; j++) { 
                            arr[j] = Bytes.unpack4(body, offs);
                            offs += 4;
                        }
                        provider.set(f, obj, arr);
                    }
                    continue;
                case ClassDescriptor.tpArrayOfEnum:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        f.set(obj, null);
                    } else {
                        Class elemType = f.getType().getComponentType();
                        Enum[] enumConstants = (Enum[])elemType.getEnumConstants();
                        Enum[] arr = (Enum[])Array.newInstance(elemType, len);
                        for (int j = 0; j < len; j++) { 
                            int index = Bytes.unpack4(body, offs);
                            if (index >= 0) {
				arr[j] = enumConstants[index];
			    }
                            offs += 4;
                        }
                        f.set(obj, arr);
                    }
                    continue;
                case ClassDescriptor.tpArrayOfLong:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        provider.set(f, obj, null);
                    } else {
                        long[] arr = new long[len];
                        for (int j = 0; j < len; j++) { 
                            arr[j] = Bytes.unpack8(body, offs);
                            offs += 8;
                        }
                        provider.set(f, obj, arr);
                    }
                    continue;
                case ClassDescriptor.tpArrayOfFloat:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        provider.set(f, obj, null);
                    } else {
                        float[] arr = new float[len];
                        for (int j = 0; j < len; j++) { 
                            arr[j] = Bytes.unpackF4(body, offs);
                            offs += 4;
                        }
                        provider.set(f, obj, arr);
                    }
                    continue;
                case ClassDescriptor.tpArrayOfDouble:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        provider.set(f, obj, null);
                    } else {
                        double[] arr = new double[len];
                        for (int j = 0; j < len; j++) { 
                            arr[j] = Bytes.unpackF8(body, offs);
                            offs += 8;
                        }
                        provider.set(f, obj, arr);
                    }
                    continue;
                case ClassDescriptor.tpArrayOfDate:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        provider.set(f, obj, null);
                    } else {
                        Date[] arr = new Date[len];
                        for (int j = 0; j < len; j++) { 
                            long msec = Bytes.unpack8(body, offs);
                            offs += 8;
                            if (msec >= 0) { 
                                arr[j] = new Date(msec);
                            }
                        }
                        provider.set(f, obj, arr);
                    }
                    continue;
                case ClassDescriptor.tpArrayOfString:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        provider.set(f, obj, null);
                    } else {
                        String[] arr = new String[len];
                        ArrayPos pos = new ArrayPos(body, offs);
                        for (int j = 0; j < len; j++) {
                            arr[j] = Bytes.unpackString(pos, encoding);
                        }
                        offs = pos.offs;
                        provider.set(f, obj, arr);
                    }
                    continue;
                case ClassDescriptor.tpArrayOfObject:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        provider.set(f, obj, null);
                    } else {
                        Class elemType = f.getType().getComponentType();
                        Object[] arr = (Object[])Array.newInstance(elemType, len);
                        ArrayPos pos = new ArrayPos(body, offs);
                        for (int j = 0; j < len; j++) { 
                            arr[j] = unswizzle(pos, elemType, parent, recursiveLoading);
                        }
                        offs = pos.offs;
                        provider.set(f, obj, arr);
                    }
                    continue;
                case ClassDescriptor.tpArrayOfValue:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        provider.set(f, obj, null);
                    } else {
                        Class elemType = f.getType().getComponentType();
                        Object[] arr = (Object[])Array.newInstance(elemType, len);
                        ClassDescriptor valueDesc = fd.valueDesc;
                        for (int j = 0; j < len; j++) { 
                            Object value = valueDesc.newInstance();
                            offs = unpackObject(value, valueDesc, recursiveLoading, body, offs, parent);
                            arr[j] = value;
                        }
                        provider.set(f, obj, arr);
                    }
                    continue;
                case ClassDescriptor.tpLink:
                    len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        provider.set(f, obj, null);
                    } else {
                        Object[] arr = new Object[len];
                        for (int j = 0; j < len; j++) { 
                            int elemOid = Bytes.unpack4(body, offs);
                            offs += 4;
                            if (elemOid != 0) { 
                                arr[j] = new PersistentStub(this, elemOid);
                            }
                        }
                        provider.set(f, obj, new LinkImpl(this, arr, parent));
                    }
                }
            }
        }
        return offs;
    }

    final int packValue(Object value, int offs, ByteBuffer buf) throws Exception {
        if (value == null) { 
            buf.extend(offs + 4);
            Bytes.pack4(buf.arr, offs, -1);
            offs += 4;
        } else if (value instanceof IPersistent) { 
            buf.extend(offs + 8);
            Bytes.pack4(buf.arr, offs, -2-ClassDescriptor.tpObject);
            Bytes.pack4(buf.arr, offs+4, swizzle((IPersistent)value, buf.finalized));
            offs += 8;                        
        } else { 
            Class c = value.getClass();
            if (c == Boolean.class) { 
                buf.extend(offs + 5);
                Bytes.pack4(buf.arr, offs, -2-ClassDescriptor.tpBoolean);
                buf.arr[offs+4] = (byte)(((Boolean)value).booleanValue() ? 1 : 0);
                offs += 5;                        
            } else if (c == Character.class) { 
                buf.extend(offs + 6);
                Bytes.pack4(buf.arr, offs, -2-ClassDescriptor.tpChar);
                Bytes.pack2(buf.arr, offs+4, (short)((Character)value).charValue());
                offs += 6;                                                   
            } else if (c == Byte.class) { 
                buf.extend(offs + 5);
                Bytes.pack4(buf.arr, offs, -2-ClassDescriptor.tpByte);
                buf.arr[offs+4] = ((Byte)value).byteValue();
                offs += 5;                        
            } else if (c == Short.class) { 
                buf.extend(offs + 6);
                Bytes.pack4(buf.arr, offs, -2-ClassDescriptor.tpShort);
                Bytes.pack2(buf.arr, offs+4, ((Short)value).shortValue());
                offs += 6;                                                   
            } else if (c == Integer.class) { 
                buf.extend(offs + 8);
                Bytes.pack4(buf.arr, offs, -2-ClassDescriptor.tpInt);
                Bytes.pack4(buf.arr, offs+4, ((Integer)value).intValue());
                offs += 8;                                                   
            } else if (c == Long.class) { 
                buf.extend(offs + 12);
                Bytes.pack4(buf.arr, offs, -2-ClassDescriptor.tpLong);
                Bytes.pack8(buf.arr, offs+4, ((Long)value).longValue());
                offs += 12;                                                   
            } else if (c == Float.class) { 
                buf.extend(offs + 8);
                Bytes.pack4(buf.arr, offs, -2-ClassDescriptor.tpFloat);
                Bytes.pack4(buf.arr, offs+4, Float.floatToIntBits(((Float)value).floatValue()));
                offs += 8;                                                   
            } else if (c == Double.class) { 
                buf.extend(offs + 12);
                Bytes.pack4(buf.arr, offs, -2-ClassDescriptor.tpDouble);
                Bytes.pack8(buf.arr, offs+4, Double.doubleToLongBits(((Double)value).doubleValue()));
                offs += 12;                                                   
            } else if (c == Date.class) { 
                buf.extend(offs + 12);
                Bytes.pack4(buf.arr, offs, -2-ClassDescriptor.tpDate);
                Bytes.pack8(buf.arr, offs+4, ((Date)value).getTime());
                offs += 12;                                                   
            } else {
                ByteArrayOutputStream bout = new ByteArrayOutputStream();
                ObjectOutputStream out = loaderMap != null
                    ? (ObjectOutputStream)new AnnotatedPersistentObjectOutputStream(bout)
                    : (ObjectOutputStream)new PersistentObjectOutputStream(bout);
                out.writeObject(value);
                out.close();
                byte[] arr = bout.toByteArray();
                int len = arr.length;                        
                buf.extend(offs + 4 + len);
                Bytes.pack4(buf.arr, offs, len);
                offs += 4;
                System.arraycopy(arr, 0, buf.arr, offs, len);
                offs += len;
            }
        }
        return offs;
    }

    final byte[] packObject(Object obj, boolean finalized) { 
        ByteBuffer buf = new ByteBuffer(this, obj, finalized);
        int offs = ObjectHeader.sizeof;
        buf.extend(offs);
        ClassDescriptor desc = getClassDescriptor(obj.getClass());
        try {
            if (obj instanceof SelfSerializable) { 
                ((SelfSerializable)obj).pack(buf.getOutputStream());
                offs = buf.used;
            } else if (desc.customSerializable) { 
                serializer.pack(obj, buf.getOutputStream());
                offs = buf.used;
            } else { 
                offs = packObject(obj, desc, offs, buf);
            }
        } catch (Exception x) { 
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
        ObjectHeader.setSize(buf.arr, 0, offs);
        ObjectHeader.setType(buf.arr, 0, desc.getOid());
        return buf.arr;        
    }

    final int swizzle(ByteBuffer buf, int offs, Object obj) throws Exception
    {
        if (obj instanceof IPersistent || obj == null) {
            offs = buf.packI4(offs, swizzle(obj, buf.finalized));
        } else {
            Class t = obj.getClass();
            if (t == Boolean.class){
                buf.extend(offs + 5);
                Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpBoolean);
                buf.arr[offs + 4] = (byte)(((Boolean)obj).booleanValue() ? 1 : 0);
                offs += 5;
            } else if (t == Character.class) {
                buf.extend(offs + 6);
                Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpChar);
                Bytes.pack2(buf.arr, offs + 4, (short)((Character)obj).charValue());
                offs += 6;
            } else if (t == Byte.class) {
                buf.extend(offs + 5);
                Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpByte);
                buf.arr[offs + 4] = ((Byte)obj).byteValue();
                offs += 5;
            } else if (t == Short.class) {
                buf.extend(offs + 6);
                Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpShort);
                Bytes.pack2(buf.arr, offs + 4, ((Short)obj).shortValue());
                offs += 6;
            } else if (t == Integer.class) {
                buf.extend(offs + 8);
                Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpInt);
                Bytes.pack4(buf.arr, offs + 4, ((Integer)obj).intValue());
                offs += 8;
            } else if (t == Long.class) {
                buf.extend(offs + 12);
                Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpLong);
                Bytes.pack8(buf.arr, offs + 4, ((Long)obj).longValue());
                offs += 12;
            } else if (t == Float.class) {
                buf.extend(offs + 8);
                Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpFloat);
                Bytes.packF4(buf.arr, offs + 4, ((Float)obj).floatValue());
                offs += 8;
            } else if (t == Double.class) {
                buf.extend(offs + 12);
                Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpDouble);
                Bytes.packF8(buf.arr, offs + 4, ((Double)obj).doubleValue());
                offs += 12;
            } else if (t == Date.class) {
                buf.extend(offs + 12);
                Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpDate);
                Bytes.pack8(buf.arr, offs + 4, ((Date)obj).getTime());
                offs += 12;
            } else if (t == String.class)  {
                offs = buf.packI4(offs, -1 - ClassDescriptor.tpString);
                offs = buf.packString(offs, (String)obj);
            } else if (obj instanceof LinkImpl)  {
                LinkImpl link = (LinkImpl)obj;  
                link.owner = buf.parent;        
                int len = link.size();  
                buf.extend(offs + 8 + len*4);
                Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpLink);
                offs += 4;
                Bytes.pack4(buf.arr, offs, len);
                offs += 4;
                for (int j = 0; j < len; j++) {
                    Bytes.pack4(buf.arr, offs, swizzle(link.getRaw(j), buf.finalized));
                    offs += 4;
                }                
            } else if (obj instanceof Collection && t.getName().startsWith("java.util.")) {
                ClassDescriptor valueDesc = getClassDescriptor(obj.getClass());
                offs = buf.packI4(offs, -ClassDescriptor.tpValueTypeBias - valueDesc.getOid());
                Collection c = (Collection)obj;
                offs = buf.packI4(offs, c.size());
                for (Object elem : c) {
                    offs = swizzle(buf, offs, elem);
                }                    
            } else if (obj instanceof IValue) { 
                ClassDescriptor valueDesc = getClassDescriptor(obj.getClass());
                offs = buf.packI4(offs, -ClassDescriptor.tpValueTypeBias - valueDesc.getOid());
                offs = packObject(obj, valueDesc, offs, buf);                
            } else if (t.isArray()) {
                Class elemType = t.getComponentType();
                if (elemType == byte.class) {
                    byte[] arr = (byte[])obj;       
                    int len = arr.length;
                    buf.extend(offs + len + 8);
                    Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpArrayOfByte);
                    Bytes.pack4(buf.arr, offs + 4, len);
                    System.arraycopy(arr, 0, buf.arr, offs + 8, len);
                    offs += 8 + len;
                } else if (elemType == boolean.class) {
                    boolean[] arr = (boolean[])obj;       
                    int len = arr.length;
                    buf.extend(offs + len + 8);
                    Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpArrayOfBoolean);
                    offs += 4;
                    Bytes.pack4(buf.arr, offs, len);
                    offs += 4;
                    for (int i = 0; i < len; i++) { 
                        buf.arr[offs++] = (byte)(arr[i]?1:0);
                    }
                } else if (elemType == char.class) {
                    char[] arr = (char[])obj;       
                    int len = arr.length;
                    buf.extend(offs + len*2 + 8);
                    Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpArrayOfChar);
                    offs += 4;
                    Bytes.pack4(buf.arr, offs, len);
                    offs += 4;
                    for (int i = 0; i < len; i++) { 
                        Bytes.pack2(buf.arr, offs, (short)arr[i]);
                        offs += 2;
                    }
                } else if (elemType == short.class) {
                    short[] arr = (short[])obj;       
                    int len = arr.length;
                    buf.extend(offs + len*2 + 8);
                    Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpArrayOfShort);
                    offs += 4;
                    Bytes.pack4(buf.arr, offs, len);
                    offs += 4;
                    for (int i = 0; i < len; i++) { 
                        Bytes.pack2(buf.arr, offs, arr[i]);
                        offs += 2;
                    }
                } else if (elemType == int.class) {
                    int[] arr = (int[])obj;       
                    int len = arr.length;
                    buf.extend(offs + len*4 + 8);
                    Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpArrayOfInt);
                    offs += 4;
                    Bytes.pack4(buf.arr, offs, len);
                    offs += 4;
                    for (int i = 0; i < len; i++) { 
                        Bytes.pack4(buf.arr, offs, arr[i]);
                        offs += 4;
                    }
                } else if (elemType == long.class) {
                    long[] arr = (long[])obj;       
                    int len = arr.length;
                    buf.extend(offs + len*8 + 8);
                    Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpArrayOfLong);
                    offs += 4;
                    Bytes.pack4(buf.arr, offs, len);
                    offs += 4;
                    for (int i = 0; i < len; i++) { 
                        Bytes.pack8(buf.arr, offs, arr[i]);
                        offs += 8;
                    }
                } else if (elemType == float.class) {
                    float[] arr = (float[])obj;       
                    int len = arr.length;
                    buf.extend(offs + len*4 + 8);
                    Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpArrayOfFloat);
                    offs += 4;
                    Bytes.pack4(buf.arr, offs, len);
                    offs += 4;
                    for (int i = 0; i < len; i++) { 
                        Bytes.packF4(buf.arr, offs, arr[i]);
                        offs += 4;
                    }
                } else if (elemType == double.class) {
                    double[] arr = (double[])obj;       
                    int len = arr.length;
                    buf.extend(offs + len*8 + 8);
                    Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpArrayOfLong);
                    offs += 4;
                    Bytes.pack4(buf.arr, offs, len);
                    offs += 4;
                    for (int i = 0; i < len; i++) { 
                        Bytes.packF8(buf.arr, offs, arr[i]);
                        offs += 8;
                    }
                } else if (elemType == Object.class) {
                    offs = buf.packI4(offs, -1 - ClassDescriptor.tpArrayOfObject);
                    Object[] arr = (Object[])obj;
                    int len = arr.length;  
                    offs = buf.packI4(offs, len);
                    for (int i = 0; i < len; i++) {
                        offs = swizzle(buf, offs, arr[i]);
                    }
                } else {
                    offs = buf.packI4(offs, -1 - ClassDescriptor.tpArrayOfRaw);
                    int len = Array.getLength(obj);                    
                    offs = buf.packI4(offs, len);
                    ClassDescriptor desc = getClassDescriptor(elemType);
                    offs = buf.packI4(offs, desc.getOid());
                    for (int i = 0; i < len; i++) {
                        offs = swizzle(buf, offs, Array.get(obj, i));
                    }
                }
            } else if (serializer != null && serializer.isEmbedded(obj)) { 
                buf.packI4(offs, -1 - ClassDescriptor.tpCustom);
                serializer.pack(obj, buf.getOutputStream());
                offs = buf.used;
            } else {
                offs = buf.packI4(offs, swizzle(obj, buf.finalized));
            }
        }
        return offs;
    }


    final int packObject(Object obj, ClassDescriptor desc, int offs, ByteBuffer buf) 
        throws Exception 
    { 
        ClassDescriptor.FieldDescriptor[] flds = desc.allFields;
        for (int i = 0, n = flds.length; i < n; i++) {
            ClassDescriptor.FieldDescriptor fd = flds[i];
            Field f = fd.field;
            switch(fd.type) {
                case ClassDescriptor.tpByte:
                    buf.extend(offs + 1);
                    buf.arr[offs++] = f.getByte(obj);
                    continue;
                case ClassDescriptor.tpBoolean:
                    buf.extend(offs + 1);
                    buf.arr[offs++] = (byte)(f.getBoolean(obj) ? 1 : 0);
                    continue;
                case ClassDescriptor.tpShort:
                    buf.extend(offs + 2);
                    Bytes.pack2(buf.arr, offs, f.getShort(obj));
                    offs += 2;
                    continue;
                case ClassDescriptor.tpChar:
                    buf.extend(offs + 2);
                    Bytes.pack2(buf.arr, offs, (short)f.getChar(obj));
                    offs += 2;
                    continue;
                case ClassDescriptor.tpInt:
                    buf.extend(offs + 4);
                    Bytes.pack4(buf.arr, offs, f.getInt(obj));
                    offs += 4;
                    continue;
                case ClassDescriptor.tpLong:
                    buf.extend(offs + 8);
                    Bytes.pack8(buf.arr, offs, f.getLong(obj));
                    offs += 8;
                    continue;
                case ClassDescriptor.tpFloat:
                    buf.extend(offs + 4);
                    Bytes.packF4(buf.arr, offs, f.getFloat(obj));
                    offs += 4;
                    continue;
                case ClassDescriptor.tpDouble:
                    buf.extend(offs + 8);
                    Bytes.packF8(buf.arr, offs, f.getDouble(obj));
                    offs += 8;
                    continue;
                case ClassDescriptor.tpEnum:
		{
                    Enum e = (Enum)f.get(obj);
                    buf.extend(offs + 4);
                    if (e == null) {
                        Bytes.pack4(buf.arr, offs, -1);
                    } else {
                        Bytes.pack4(buf.arr, offs, e.ordinal());
                    }
                    offs += 4;
                    continue;
		}
                case ClassDescriptor.tpDate:
                {
                    buf.extend(offs + 8);
                    Date d = (Date)f.get(obj);
                    long msec = (d == null) ? -1 : d.getTime();                
                    Bytes.pack8(buf.arr, offs, msec);
                    offs += 8;
                    continue;
                }
                case ClassDescriptor.tpString:
                    offs = buf.packString(offs, (String)f.get(obj));
                    continue;
                case ClassDescriptor.tpObject:
                    offs = swizzle(buf, offs, f.get(obj));
                    continue;
                case ClassDescriptor.tpValue:
                {
                    Object value = f.get(obj);
                    if (value == null) { 
                        throw new StorageError(StorageError.NULL_VALUE, fd.fieldName);
                    } else if (value instanceof IPersistent) { 
                        throw new StorageError(StorageError.SERIALIZE_PERSISTENT);
                    }                        
                    offs = packObject(value, fd.valueDesc, offs, buf);
                    continue;
                }
                case ClassDescriptor.tpRaw:
                    offs = packValue(f.get(obj), offs, buf);
                    continue;
                case ClassDescriptor.tpCustom:
                {
                    serializer.pack(f.get(obj), buf.getOutputStream());
                    offs = buf.size();
                    continue;
                }
                case ClassDescriptor.tpArrayOfByte:
                {
                    byte[] arr = (byte[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4 + len);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        System.arraycopy(arr, 0, buf.arr, offs, len);
                        offs += len;
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfBoolean:
                {
                    boolean[] arr = (boolean[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4 + len);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++, offs++) {
                            buf.arr[offs] = (byte)(arr[j] ? 1 : 0);
                        }
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfShort:
                {
                    short[] arr = (short[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4 + len*2);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++) {
                            Bytes.pack2(buf.arr, offs, arr[j]);
                            offs += 2;
                        }
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfChar:
                {
                    char[] arr = (char[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4 + len*2);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++) {
                            Bytes.pack2(buf.arr, offs, (short)arr[j]);
                            offs += 2;
                        }
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfInt:
                {
                    int[] arr = (int[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4 + len*4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++) {
                            Bytes.pack4(buf.arr, offs, arr[j]);
                            offs += 4;
                        }
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfEnum:
                {
                    Enum[] arr = (Enum[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4 + len*4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++) {
                            if (arr[j] == null) {
                                Bytes.pack4(buf.arr, offs, -1);
                            } else {
                                Bytes.pack4(buf.arr, offs, arr[j].ordinal());
                            }
                            offs += 4;
                        }
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfLong:
                {
                    long[] arr = (long[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4 + len*8);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++) {
                            Bytes.pack8(buf.arr, offs, arr[j]);
                            offs += 8;
                        }
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfFloat:
                {
                    float[] arr = (float[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4 + len*4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++) {
                            Bytes.packF4(buf.arr, offs, arr[j]);
                            offs += 4;
                        }
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfDouble:
                {
                    double[] arr = (double[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4 + len*8);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++) {
                            Bytes.packF8(buf.arr, offs, arr[j]);
                            offs += 8;
                        }
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfDate:
                {
                    Date[] arr = (Date[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4 + len*8);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++) {
                            Date d = arr[j];
                            long msec = (d == null) ? -1 : d.getTime();                            
                            Bytes.pack8(buf.arr, offs, msec);
                            offs += 8;
                        }
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfString:
                {
                    String[] arr = (String[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++) {
                            offs = buf.packString(offs, (String)arr[j]);
                        }
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfObject:
                {
                    Object[] arr = (Object[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4 + len*4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++) {
                            offs = swizzle(buf, offs, arr[j]);
                        }
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfValue:
                {
                    Object[] arr = (Object[])f.get(obj);
                    if (arr == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        int len = arr.length;                        
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        ClassDescriptor elemDesc = fd.valueDesc;
                        for (int j = 0; j < len; j++) {
                            Object value = arr[j];
                            if (value == null) { 
                                throw new StorageError(StorageError.NULL_VALUE, fd.fieldName);
                            }
                            offs = packObject(value, elemDesc, offs, buf);
                        }
                    }
                    continue;
                }
                case ClassDescriptor.tpLink:
                {
                    LinkImpl link = (LinkImpl)f.get(obj);
                    if (link == null) { 
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        link.owner = buf.parent;
                        int len = link.size();                        
                        buf.extend(offs + 4 + len*4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        for (int j = 0; j < len; j++) {
                            Bytes.pack4(buf.arr, offs, swizzle(link.getRaw(j), buf.finalized));
                            offs += 4;
                        }
                        if (!buf.finalized) { 
                            link.unpin();
                        }
                    }
                    continue;
                }
            }
        }
        return offs;
    }
                
    public ClassLoader setClassLoader(ClassLoader loader) 
    { 
        ClassLoader prev = loader;
        this.loader = loader;
        return prev;
    }

    public ClassLoader getClassLoader() {
        return loader;
    }

    public void registerClassLoader(INamedClassLoader loader) { 
        if (loaderMap == null) { 
            loaderMap = new HashMap();
        }
        loaderMap.put(loader.getName(), loader);
    }

    public ClassLoader findClassLoader(String name) { 
        if (loaderMap == null) { 
            return null;
        }
        return (ClassLoader)loaderMap.get(name);
    }


    public void setCustomSerializer(CustomSerializer serializer) { 
        this.serializer = serializer;
    }

    class HashIterator implements PersistentIterator, Iterator
    {
        Iterator oids;

        HashIterator(HashSet result)
        {
            oids = result.iterator();
        }
             
        public Object next() { 
            int oid = ((Integer)oids.next()).intValue();
            return lookupObject(oid, null);
        }

        public int nextOid() { 
            return oids.hasNext() ? ((Integer)oids.next()).intValue() : 0;
        }

        public boolean hasNext() { 
            return oids.hasNext();
        }

        public void remove() { 
            throw new UnsupportedOperationException();
        }
    }

    public Iterator merge(Iterator[] selections) {             
        HashSet result = null;
        for (int i = 0; i < selections.length; i++) { 
            PersistentIterator iterator = (PersistentIterator)selections[i];
            HashSet newResult = new HashSet();
            int oid;
            while ((oid = iterator.nextOid()) != 0) {
                Integer oidWrapper = new Integer(oid);
                if (result == null || result.contains(oidWrapper)) {
                    newResult.add(oidWrapper);
                }  
            }
            result = newResult;
            if (result.size() == 0) {
                break;
            }
        }
        if (result == null) {
            result = new HashSet();
        }
        return new HashIterator(result);     
    }

    public Iterator join(Iterator[] selections) {             
        HashSet result = new HashSet();
        for (int i = 0; i < selections.length; i++) { 
            PersistentIterator iterator = (PersistentIterator)selections[i];
            int oid;
            while ((oid = iterator.nextOid()) != 0) {
                result.add(new Integer(oid));
            }
        }
        return new HashIterator(result);     
    }

    public synchronized void registerCustomAllocator(Class cls, CustomAllocator allocator) {
        synchronized (objectCache) { 
            ClassDescriptor desc = getClassDescriptor(cls);
            desc.allocator = allocator;
            storeObject0(desc, false);
            if (customAllocatorMap == null) { 
                customAllocatorMap = new HashMap();
                customAllocatorList = new ArrayList();
            }
            customAllocatorMap.put(cls, desc.allocator);
            customAllocatorList.add(desc.allocator);
        }
    }


    public CustomAllocator createBitmapAllocator(int quantum, long base, long extension, long limit) { 
        return new BitmapCustomAllocator(this, quantum, base, extension, limit);
    }

    public int getDatabaseFormatVersion() { 
        return header.databaseFormatVersion;
    }

    public void deallocate(Object obj) 
    {
        deallocateObject(obj);
    }

    public void store(Object obj)
    {
        if (obj instanceof IPersistent) {
            ((IPersistent)obj).store();
        } else {
            synchronized (this) { 
                synchronized (objectCache) { 
                    synchronized (objMap) {
                        ObjectMap.Entry e = objMap.put(obj);
                        if ((e.state & Persistent.RAW) != 0) {
                            throw new StorageError(StorageError.ACCESS_TO_STUB);
                        }
                        storeObject(obj);
                        e.state &= ~Persistent.DIRTY;
                    }
                }
            }
        }
    }

    void unassignOid(Object obj)
    {
        if (obj instanceof IPersistent) {
            ((IPersistent)obj).assignOid(null, 0, false);
        } else {
            objMap.remove(obj);
        }
    }
                
    void assignOid(Object obj, int oid, boolean raw) {    
        if (obj instanceof IPersistent) {
            ((IPersistent)obj).assignOid(this, oid, raw);
        } else {
            synchronized (objMap) {
                ObjectMap.Entry e = objMap.put(obj);
                e.oid = oid;
                if (raw) {
                    e.state = Persistent.RAW;
                }
            } 
        }
    }
    
    public void modify(Object obj)
    { 
        if (obj instanceof IPersistent) {
            ((IPersistent)obj).modify();
        } else {
            if (useSerializableTransactions) { 
                ThreadTransactionContext ctx = getTransactionContext();
                if (ctx.nested != 0) { // serializable transaction
                    ctx.modified.add(obj);
                    return;
                }
            }
            synchronized (this) { 
                synchronized (objectCache) { 
                    synchronized (objMap) {
                        ObjectMap.Entry e = objMap.put(obj);
                        if ((e.state & Persistent.DIRTY) == 0 && e.oid != 0) { 
                            if ((e.state & Persistent.RAW) != 0) { 
                                throw new StorageError(StorageError.ACCESS_TO_STUB);
                            }
                            Assert.that((e.state & Persistent.DELETED) == 0);
                            storeObject(obj);
                            e.state &= ~Persistent.DIRTY;
                        }
                    }
                }
            }
        }
    }

    public void invalidate(Object obj)
    { 
        if (obj instanceof IPersistent) {
            ((IPersistent)obj).invalidate();
        } else {
            synchronized (objMap) {
                ObjectMap.Entry e = objMap.put(obj);
                e.state &= ~Persistent.DIRTY;
                e.state |= Persistent.RAW;
            }
        }
    }

    public void load(Object obj)
    {
        if (obj instanceof IPersistent) {
            ((IPersistent)obj).load();
        } else {
            synchronized (objMap) {
                ObjectMap.Entry e = objMap.get(obj);
                if (e == null || (e.state & Persistent.RAW) == 0 || e.oid == 0) { 
                    return;
                }
            }
            loadObject(obj);
        }
    }

    boolean isLoaded(Object obj)
    { 
        if (obj instanceof IPersistent) {
            IPersistent po = (IPersistent)obj;
            return !po.isRaw() && po.isPersistent();
        } else {
            synchronized (objMap)
            {
                ObjectMap.Entry e = objMap.get(obj);
                return e != null && (e.state & Persistent.RAW) == 0 && e.oid != 0;
            }
        }
    }

    public int getOid(Object obj)
    {
        return (obj instanceof IPersistent) ? ((IPersistent)obj).getOid() : obj == null ? 0 : objMap.getOid(obj);
    }
    
    boolean isPersistent(Object obj)
    {
        return getOid(obj) != 0;
    }

    boolean isDeleted(Object obj)
    {
        return (obj instanceof IPersistent) ? ((IPersistent)obj).isDeleted() : obj == null ? false : (objMap.getState(obj) & Persistent.DELETED) != 0;
    }
     
    boolean recursiveLoading(Object obj)
    {
        return (obj instanceof IPersistent) ? ((IPersistent)obj).recursiveLoading() : true;
    }

    boolean isModified(Object obj)
    {
        return (obj instanceof IPersistent) ? ((IPersistent)obj).isModified() : obj == null ? false : (objMap.getState(obj) & Persistent.DIRTY) != 0;
    }

    boolean isRaw(Object obj)
    {
        return (obj instanceof IPersistent) ? ((IPersistent)obj).isRaw() : obj == null ? false : (objMap.getState(obj) & Persistent.RAW) != 0;
    }
                 
    private ObjectMap objMap;

    private int     initIndexSize = dbDefaultInitIndexSize;
    private int     objectCacheInitSize = dbDefaultObjectCacheInitSize;
    private long    extensionQuantum = dbDefaultExtensionQuantum;
    private String  cacheKind = "lru";
    private boolean readOnly = false;
    private boolean noFlush = false;
    private boolean lockFile = false;
    private boolean multiclientSupport = false;
    private boolean alternativeBtree = false;
    private boolean backgroundGc = false;
    private boolean forceStore = true;
    private long    pagePoolLruLimit = dbDefaultPagePoolLruLimit;
    
    private HashMap   customAllocatorMap;
    private ArrayList customAllocatorList;
    private CustomAllocator defaultAllocator;

    boolean replicationAck = false;
    boolean concurrentIterator = false;
    int     slaveConnectionTimeout = 60; // seconds

    Properties properties = new Properties();

    String    encoding = null; 

    PagePool  pool;
    Header    header;           // base address of database file mapping
    int       dirtyPagesMap[];  // bitmap of changed pages in current index
    boolean   modified;

    int       currRBitmapPage;//current bitmap page for allocating records
    int       currRBitmapOffs;//offset in current bitmap page for allocating 
                              //unaligned records
    int       currPBitmapPage;//current bitmap page for allocating page objects
    int       currPBitmapOffs;//offset in current bitmap page for allocating 
                              //page objects
    Location  reservedChain;
    CloneNode cloneList;
    boolean   insideCloneBitmap;

    int       committedIndexSize;
    int       currIndexSize;

    int       currIndex;  // copy of header.root, used to allow read access to the database 
                          // during transaction commit
    long      usedSize;   // total size of allocated objects since the beginning of the session
    int[]     bitmapPageAvailableSpace;
    boolean   opened;

    int[]     greyBitmap; // bitmap of visited during GC but not yet marked object
    int[]     blackBitmap;    // bitmap of objects marked during GC 
    long      gcThreshold;
    long      allocatedDelta;
    boolean   gcDone;
    boolean   gcActive;
    Object    backgroundGcMonitor;
    Object    backgroundGcStartMonitor;
    GcThread  gcThread;

    int       bitmapExtentBase;

    ClassLoader loader;
    HashMap     loaderMap;

    CustomSerializer serializer;

    StorageListener listener;

    long      transactionId;
    IFile     file;

    int       nNestedTransactions;
    int       nBlockedTransactions;
    int       nCommittedTransactions;
    long      scheduledCommitTime;
    Object    transactionMonitor;
    PersistentResource transactionLock;

    final ThreadLocal transactionContext = new ThreadLocal() {
         protected synchronized Object initialValue() {
             return new ThreadTransactionContext();
         }
    };
    boolean useSerializableTransactions;


    OidHashTable     objectCache;
    HashMap          classDescMap;
    ClassDescriptor  descList;
}

class RootPage { 
    long size;            // database file size
    long index;           // offset to object index
    long shadowIndex;     // offset to shadow index
    long usedSize;        // size used by objects
    int  indexSize;       // size of object index
    int  shadowIndexSize; // size of object index
    int  indexUsed;       // userd part of the index   
    int  freeList;        // L1 list of free descriptors
    int  bitmapEnd;       // index of last allocated bitmap page
    int  rootObject;      // OID of root object
    int  classDescList;   // List of class descriptors
    int  bitmapExtent;    // Offset of extended bitmap pages in object index

    final static int sizeof = 64;
} 

class Header { 
    int      curr;  // current root
    boolean  dirty; // database was not closed normally
    byte     databaseFormatVersion;

    RootPage root[];
    long     transactionId;
   
    final static int sizeof = 3 + RootPage.sizeof*2 + 8;
    
    final void pack(byte[] rec) { 
        int offs = 0;
        rec[offs++] = (byte)curr;
        rec[offs++] = (byte)(dirty ? 1 : 0);
        rec[offs++] = databaseFormatVersion;
        for (int i = 0; i < 2; i++) { 
            Bytes.pack8(rec, offs, root[i].size);
            offs += 8;
            Bytes.pack8(rec, offs, root[i].index);
            offs += 8;
            Bytes.pack8(rec, offs, root[i].shadowIndex);
            offs += 8;
            Bytes.pack8(rec, offs, root[i].usedSize);
            offs += 8;
            Bytes.pack4(rec, offs, root[i].indexSize);
            offs += 4;
            Bytes.pack4(rec, offs, root[i].shadowIndexSize);
            offs += 4;
            Bytes.pack4(rec, offs, root[i].indexUsed);
            offs += 4;
            Bytes.pack4(rec, offs, root[i].freeList);
            offs += 4;
            Bytes.pack4(rec, offs, root[i].bitmapEnd);
            offs += 4;
            Bytes.pack4(rec, offs, root[i].rootObject);
            offs += 4;
            Bytes.pack4(rec, offs, root[i].classDescList);
            offs += 4;
            Bytes.pack4(rec, offs, root[i].bitmapExtent);
            offs += 4;
        }
        Bytes.pack8(rec, offs, transactionId);
        offs += 8;
        Assert.that(offs == sizeof);
    }
    
    final void unpack(byte[] rec) { 
        int offs = 0;
        curr = rec[offs++];
        dirty = rec[offs++] != 0;
        databaseFormatVersion = rec[offs++];
        root = new RootPage[2];
        for (int i = 0; i < 2; i++) { 
            root[i] = new RootPage();
            root[i].size = Bytes.unpack8(rec, offs);
            offs += 8;
            root[i].index = Bytes.unpack8(rec, offs);
            offs += 8;
            root[i].shadowIndex = Bytes.unpack8(rec, offs);
            offs += 8;
            root[i].usedSize = Bytes.unpack8(rec, offs);
            offs += 8;
            root[i].indexSize = Bytes.unpack4(rec, offs);
            offs += 4;
            root[i].shadowIndexSize = Bytes.unpack4(rec, offs);
            offs += 4;
            root[i].indexUsed = Bytes.unpack4(rec, offs);
            offs += 4;
            root[i].freeList = Bytes.unpack4(rec, offs);
            offs += 4;
            root[i].bitmapEnd = Bytes.unpack4(rec, offs);
            offs += 4;
            root[i].rootObject = Bytes.unpack4(rec, offs);
            offs += 4;
            root[i].classDescList = Bytes.unpack4(rec, offs);
            offs += 4;
            root[i].bitmapExtent = Bytes.unpack4(rec, offs);
            offs += 4;
        }
        transactionId = Bytes.unpack8(rec, offs);
        offs += 8;
        Assert.that(offs == sizeof);
    }   
}

