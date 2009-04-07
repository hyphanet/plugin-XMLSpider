package plugins.XMLSpider.org.garret.perst.impl;

import plugins.XMLSpider.org.garret.perst.CustomAllocator;
import plugins.XMLSpider.org.garret.perst.Persistent;
import plugins.XMLSpider.org.garret.perst.Storage;

public class DefaultAllocator extends Persistent implements CustomAllocator { 
    public DefaultAllocator(Storage storage) { 
        super(storage);
    }
    
    protected DefaultAllocator() {}

    public long allocate(long size) { 
        return ((StorageImpl)getStorage()).allocate(size, 0);
    }

    public long reallocate(long pos, long oldSize, long newSize) {
        StorageImpl db = (StorageImpl)getStorage();
        if (((newSize + StorageImpl.dbAllocationQuantum - 1) & ~(StorageImpl.dbAllocationQuantum-1))
            > ((oldSize + StorageImpl.dbAllocationQuantum - 1) & ~(StorageImpl.dbAllocationQuantum-1)))
        { 
            long newPos = db.allocate(newSize, 0);
            db.cloneBitmap(pos, oldSize);
            db.free(pos, oldSize);
            pos = newPos;
        }
        return pos;
    }

    public void free(long pos, long size) { 
        ((StorageImpl)getStorage()).cloneBitmap(pos, size);
    }
        
    public void commit() {}
}
        

