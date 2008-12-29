package plugins.XMLSpider.org.garret.perst.impl;
import plugins.XMLSpider.org.garret.perst.*;

import java.lang.reflect.*;
import java.util.*;

class AltBtreeMultiFieldIndex<T extends IPersistent> extends AltBtree<T> implements FieldIndex<T> { 
    String   className;
    String[] fieldName;

    transient Class   cls;
    transient Field[] fld;

    AltBtreeMultiFieldIndex() {}
    
    AltBtreeMultiFieldIndex(Class cls, String[] fieldName, boolean unique) {
        this.cls = cls;
        this.unique = unique;
        this.fieldName = fieldName;        
        this.className = ClassDescriptor.getClassName(cls);
        locateFields();
        type = ClassDescriptor.tpRaw;        
    }

    private final void locateFields() 
    {
        Class scope = cls;
        fld = new Field[fieldName.length];
        for (int i = 0; i < fieldName.length; i++) {
            fld[i] = ClassDescriptor.locateField(cls, fieldName[i]);
            if (fld[i] == null) { 
                throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND, className + "." + fieldName[i]);
            }
        }
    }

    public Class getIndexedClass() { 
        return cls;
    }

    public Field[] getKeyFields() { 
        return fld;
    }

    public void onLoad()
    {
        cls = ClassDescriptor.loadClass(getStorage(), className);
        locateFields();
    }

    static class CompoundKey implements Comparable {
        Object[] keys;

        public int compareTo(Object o) { 
            CompoundKey c = (CompoundKey)o;
            int n = keys.length < c.keys.length ? keys.length : c.keys.length; 
            for (int i = 0; i < n; i++) { 
                int diff = ((Comparable)keys[i]).compareTo(c.keys[i]);
                if (diff != 0) { 
                    return diff;
                }
            }
            return 0;  // allow to compare part of the compound key
        }

        CompoundKey(Object[] keys) { 
            this.keys = keys;
        }
    }
                
    private Key convertKey(Key key) { 
        if (key == null) { 
            return null;
        }
        if (key.type != ClassDescriptor.tpArrayOfObject) { 
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }
        return new Key(new CompoundKey((Object[])key.oval), key.inclusion != 0);
    }
            
    private Key extractKey(IPersistent obj) {
        Object[] keys = new Object[fld.length];
        try { 
            for (int i = 0; i < keys.length; i++) { 
                keys[i] = fld[i].get(obj);
                if (keys[i] instanceof IPersistent) { 
                    IPersistent p = (IPersistent)keys[i];
                    if (!p.isPersistent()) { 
                        getStorage().makePersistent(p);
                    }
                }
            }
        } catch (Exception x) { 
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
        return new Key(new CompoundKey(keys));
    }

    public boolean put(T obj) {
        return super.put(extractKey(obj), obj);
    }

    public T set(T obj) {
        return super.set(extractKey(obj), obj);
    }

    public void remove(T obj) {
        super.remove(extractKey(obj), obj);
    }

    public T remove(Key key) {
        return super.remove(convertKey(key));
    }
    
    public boolean containsObject(T obj) {
        Key key = extractKey(obj);
        if (unique) { 
            return super.get(key) != null;
        } else { 
            IPersistent[] mbrs = get(key, key);
            for (int i = 0; i < mbrs.length; i++) { 
                if (mbrs[i] == obj) { 
                    return true;
                }
            }
            return false;
        }
    }

    public boolean contains(T obj) {
        Key key = extractKey(obj);
        if (unique) { 
            return super.get(key) != null;
        } else { 
            IPersistent[] mbrs = get(key, key);
            for (int i = 0; i < mbrs.length; i++) { 
                if (mbrs[i].equals(obj)) { 
                    return true;
                }
            }
            return false;
        }
    }

    public void append(T obj) {
        throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE);
    }

    public T[] get(Key from, Key till) {
        ArrayList list = new ArrayList();
        if (root != null) { 
            root.find(convertKey(from), convertKey(till), height, list);
        }
        return (T[])list.toArray((T[])Array.newInstance(cls, list.size()));
    }

    public T[] getPrefix(String prefix) {
        throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
    }
        

    public T[] prefixSearch(String key) {
        throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
    }

    public T[] toPersistentArray() {
        T[] arr = (T[])Array.newInstance(cls, nElems);
        if (root != null) { 
            root.traverseForward(height, arr, 0);
        }
        return arr;
    }

    public T get(Key key) {
        return super.get(convertKey(key));
    }

    public IterableIterator<T> iterator(Key from, Key till, int order) {
        return super.iterator(convertKey(from), convertKey(till), order);
    }

    public IterableIterator<Map.Entry<Object,T>> entryIterator(Key from, Key till, int order) {
        return super.entryIterator(convertKey(from), convertKey(till), order);
    }

    public IterableIterator<T> queryByExample(T obj) {
        Key key = extractKey(obj);
        return iterator(key, key, ASCENT_ORDER);
    }

    public IterableIterator<T> select(String predicate) { 
        Query<T> query = new QueryImpl<T>(getStorage());
        return query.select(cls, iterator(), predicate);
    }

    public boolean isCaseInsensitive() { 
        return false;
    }
}

class AltBtreeCaseInsensitiveMultiFieldIndex<T extends IPersistent> extends AltBtreeMultiFieldIndex<T> {    
    AltBtreeCaseInsensitiveMultiFieldIndex() {}

    AltBtreeCaseInsensitiveMultiFieldIndex(Class cls, String[] fieldNames, boolean unique) {
        super(cls, fieldNames, unique);
    }

    Key checkKey(Key key) { 
        if (key != null) { 
            CompoundKey ck = (CompoundKey)key.oval;
            for (int i = 0; i < ck.keys.length; i++) { 
                if (ck.keys[i] instanceof String) { 
                    ck.keys[i] = ((String)ck.keys[i]).toLowerCase();
                }
            }
        }
        return super.checkKey(key);
    }

    public boolean isCaseInsensitive() { 
        return true;
    }
}
