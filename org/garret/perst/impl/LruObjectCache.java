package plugins.XMLSpider.org.garret.perst.impl;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;

public class LruObjectCache implements OidHashTable { 
    Entry table[];
    static final float loadFactor = 0.75f;
    static final int defaultInitSize = 1319;
    int count;
    int threshold;
    int pinLimit;
    int nPinned;
    Entry pinList;
    boolean flushing;
    StorageImpl db;

    static Runtime runtime = Runtime.getRuntime();

    public LruObjectCache(StorageImpl db, int size) {
        this.db = db;
        int initialCapacity = size == 0 ? defaultInitSize : size;
        threshold = (int)(initialCapacity * loadFactor);
        table = new Entry[initialCapacity];
        pinList = new Entry(0, null, null);
        pinLimit = size;
        pinList.lru = pinList.mru = pinList;
    }

    public synchronized boolean remove(int oid) {
        Entry tab[] = table;
        int index = (oid & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index], prev = null; e != null; prev = e, e = e.next) {
            if (e.oid == oid) {
                if (prev != null) {
                    prev.next = e.next;
                } else {
                    tab[index] = e.next;
                }
                e.clear();
                unpinObject(e);
                count -= 1;
                return true;
            }
        }
        return false;
    }

    protected Reference createReference(Object obj) { 
        return new WeakReference(obj);
    }

    private final void unpinObject(Entry e) 
    {
        if (e.pin != null) { 
            e.unpin();
            nPinned -= 1;
        }
    }
        

    private final void pinObject(Entry e, Object obj) 
    { 
        if (pinLimit != 0) { 
            if (e.pin != null) { 
                e.unlink();
            } else { 
                if (nPinned == pinLimit) {
                    pinList.lru.unpin();
                } else { 
                    nPinned += 1;
                }
            }
            e.linkAfter(pinList, obj);
        }
    }

    public synchronized void put(int oid, Object obj) { 
        Reference ref = createReference(obj);
        Entry tab[] = table;
        int index = (oid & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index]; e != null; e = e.next) {
            if (e.oid == oid) {
                e.ref = ref;
                pinObject(e, obj);
                return;
            }
        }
        if (count >= threshold && !flushing) {
            // Rehash the table if the threshold is exceeded
            rehash();
            tab = table;
            index = (oid & 0x7FFFFFFF) % tab.length;
        } 
        
        // Creates the new entry.
        tab[index] = new Entry(oid, ref, tab[index]);
        pinObject(tab[index], obj);
        count++;
    }

    public Object get(int oid) {
        while (true) { 
            cs:synchronized(this) { 
                Entry tab[] = table;
                int index = (oid & 0x7FFFFFFF) % tab.length;
                for (Entry e = tab[index]; e != null; e = e.next) {
                    if (e.oid == oid) {
                        Object obj = e.ref.get();
                        if (obj == null) { 
                            if (e.dirty != 0) { 
                                break cs;
                            }
                        } else  { 
                            if (db.isDeleted(obj)) {
                                e.ref.clear();
                                unpinObject(e);
                                return null;
                            }
                            pinObject(e, obj);
                        }
                        return obj;
                    }
                }
                return null;
            }
            runtime.runFinalization();
        } 
    }
    
    public void flush() {
        while (true) { 
          cs:synchronized(this) { 
                flushing = true;
                for (int i = 0; i < table.length; i++) { 
                    Entry e, next, prev;
                    for (e = table[i], prev = null; e != null; e = next) { 
                        Object obj = e.ref.get();
                        next = e.next;
                        if (obj != null) { 
                            if (db.isModified(obj)) { 
                                db.store(obj);
                            }
                            prev = e;
                        } else if (e.dirty != 0) { 
                            break cs;
                        } else { 
                            count -= 1;
                            e.clear();
                            if (prev == null) { 
                                table[i] = next;
                            } else { 
                                prev.next = next;
                            }                            
                        }
                    }
                }
                flushing = false;
                if (count >= threshold) {
                    // Rehash the table if the threshold is exceeded
                    rehash();
                }
                return;
            }
            runtime.runFinalization();
        }
    }

    public void invalidate() {
        while (true) { 
            cs:synchronized(this) { 
                for (int i = 0; i < table.length; i++) { 
                    for (Entry e = table[i]; e != null; e = e.next) { 
                        Object obj = e.ref.get();
                        if (obj != null) { 
                            if (db.isModified(obj)) { 
                                e.dirty = 0;
                                unpinObject(e);
                                db.invalidate(obj);
                            }
                        } else if (e.dirty != 0) { 
                            break cs;
                        }
                    }
                    table[i] = null;
                }
                count = 0;
                nPinned = 0;
                pinList.lru = pinList.mru = pinList;
                return;
            }
            runtime.runFinalization();
        }
    }

    public synchronized void clear() {
        Entry tab[] = table;
        for (int i = 0; i < tab.length; i++) { 
            tab[i] = null;
        }
        count = 0;
        nPinned = 0;
        pinList.lru = pinList.mru = pinList;
    }

    void rehash() {
        int oldCapacity = table.length;
        Entry oldMap[] = table;
        int i;
        for (i = oldCapacity; --i >= 0;) {
            Entry e, next, prev;
            for (prev = null, e = oldMap[i]; e != null; e = next) { 
                next = e.next;
                Object obj = e.ref.get();
                if ((obj == null || db.isDeleted(obj)) && e.dirty == 0) { 
                    count -= 1;
                    e.clear();
                    if (prev == null) { 
                        oldMap[i] = next;
                    } else { 
                        prev.next = next;
                    }
                } else { 
                    prev = e;
                }
            }
        }
        
        if (count <= (threshold >>> 1)) {
            return;
        }
        int newCapacity = oldCapacity * 2 + 1;
        Entry newMap[] = new Entry[newCapacity];

        threshold = (int)(newCapacity * loadFactor);
        table = newMap;

        for (i = oldCapacity; --i >= 0 ;) {
            for (Entry old = oldMap[i]; old != null; ) {
                Entry e = old;
                old = old.next;

                int index = (e.oid & 0x7FFFFFFF) % newCapacity;
                e.next = newMap[index];
                newMap[index] = e;
            }
        }
    }

    public synchronized void setDirty(Object obj) {
        int oid = db.getOid(obj);
        Entry tab[] = table;
        int index = (oid & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index] ; e != null ; e = e.next) {
            if (e.oid == oid) {
                e.dirty += 1;
                return;
            }
        }
    }

    public synchronized void clearDirty(Object obj) {
        int oid = db.getOid(obj);
        Entry tab[] = table;
        int index = (oid & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index] ; e != null ; e = e.next) {
            if (e.oid == oid) {
                if (e.dirty > 0) { 
                    e.dirty -= 1;
                }
                return;
            }
        }
    }

    public int size() { 
        return count;
    }

    static class Entry { 
        Entry       next;
        Reference   ref;
        int         oid;
        int         dirty;
        Entry       lru;
        Entry       mru;
        Object      pin;

        void unlink() { 
            lru.mru = mru;
            mru.lru = lru;
        } 

        void unpin() { 
            unlink();
            lru = mru = null;
            pin = null;
        }

        void linkAfter(Entry head, Object obj) { 
            mru = head.mru;
            mru.lru = this;
            head.mru = this;
            lru = head;
            pin = obj;
        }

        void clear() { 
            ref.clear();
            ref = null;
            dirty = 0;
            next = null;
        }

        Entry(int oid, Reference ref, Entry chain) { 
            next = chain;
            this.oid = oid;
            this.ref = ref;
        }
    }
}

