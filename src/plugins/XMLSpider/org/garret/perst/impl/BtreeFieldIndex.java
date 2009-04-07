package plugins.XMLSpider.org.garret.perst.impl;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;

import plugins.XMLSpider.org.garret.perst.Assert;
import plugins.XMLSpider.org.garret.perst.FieldIndex;
import plugins.XMLSpider.org.garret.perst.IterableIterator;
import plugins.XMLSpider.org.garret.perst.Key;
import plugins.XMLSpider.org.garret.perst.Query;
import plugins.XMLSpider.org.garret.perst.StorageError;

class BtreeFieldIndex<T> extends Btree<T> implements FieldIndex<T> { 
    String className;
    String fieldName;
    long   autoincCount;
    transient Class cls;
    transient Field fld;

    BtreeFieldIndex() {}
    
    private final void locateField() 
    {
        fld = ClassDescriptor.locateField(cls, fieldName);
        if (fld == null) { 
           throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND, className + "." + fieldName);
        }
    }

    public Class getIndexedClass() { 
        return cls;
    }

    public Field[] getKeyFields() { 
        return new Field[]{fld};
    }

    public void onLoad()
    {
        cls = ClassDescriptor.loadClass(getStorage(), className);
        locateField();
    }

    BtreeFieldIndex(Class cls, String fieldName, boolean unique) {
        this(cls, fieldName, unique, 0);
    }

    BtreeFieldIndex(Class cls, String fieldName, boolean unique, long autoincCount) {
        this.cls = cls;
        this.unique = unique;
        this.fieldName = fieldName;
        this.className = ClassDescriptor.getClassName(cls);
        this.autoincCount = autoincCount;
        locateField();
        type = checkType(fld.getType());
    }

    protected Object unpackEnum(int val) {
        return fld.getType().getEnumConstants()[val];
    }

    private Key extractKey(Object obj) { 
        try { 
            Field f = fld;
            Key key = null;
            switch (type) {
              case ClassDescriptor.tpBoolean:
                key = new Key(f.getBoolean(obj));
                break;
              case ClassDescriptor.tpByte:
                key = new Key(f.getByte(obj));
                break;
              case ClassDescriptor.tpShort:
                key = new Key(f.getShort(obj));
                break;
              case ClassDescriptor.tpChar:
                key = new Key(f.getChar(obj));
                break;
              case ClassDescriptor.tpInt:
                key = new Key(f.getInt(obj));
                break;            
              case ClassDescriptor.tpObject:
                {
                    Object val = f.get(obj);
                    key = new Key(val, getStorage().makePersistent(val), true);
                    break;
                }
              case ClassDescriptor.tpLong:
                key = new Key(f.getLong(obj));
                break;            
              case ClassDescriptor.tpDate:
                key = new Key((Date)f.get(obj));
                break;
              case ClassDescriptor.tpFloat:
                key = new Key(f.getFloat(obj));
                break;
              case ClassDescriptor.tpDouble:
                key = new Key(f.getDouble(obj));
                break;
              case ClassDescriptor.tpEnum:
                key = new Key((Enum)f.get(obj));
                break;
              case ClassDescriptor.tpString:
                key = new Key((String)f.get(obj));
                break;
              default:
                Assert.failed("Invalid type");
            }
            return key;
        } catch (Exception x) { 
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
    }
            
    public boolean add(T obj) {
        return put(obj);
    }

    public boolean put(T obj) {
        return super.insert(extractKey(obj), obj, false) >= 0;
    }

    public T set(T obj) {
         return super.set(extractKey(obj), obj);
    }

    public boolean remove(Object obj) {
        return super.removeIfExists(extractKey(obj), obj);
    }

    public boolean containsObject(T obj) {
        Key key = extractKey(obj);
        if (unique) { 
            return super.get(key) != null;
        } else { 
            Object[] mbrs = get(key, key);
            for (int i = 0; i < mbrs.length; i++) { 
                if (mbrs[i] == obj) { 
                    return true;
                }
            }
            return false;
        }
    }

    public boolean contains(Object obj) {
        Key key = extractKey(obj);
        if (unique) { 
            return super.get(key) != null;
        } else { 
            Object[] mbrs = get(key, key);
            for (int i = 0; i < mbrs.length; i++) { 
                if (mbrs[i].equals(obj)) { 
                    return true;
                }
            }
            return false;
        }
    }

    public synchronized void append(T obj) {
        Key key;
        try { 
            switch (type) {
              case ClassDescriptor.tpInt:
                key = new Key((int)autoincCount);
                fld.setInt(obj, (int)autoincCount);
                break;            
              case ClassDescriptor.tpLong:
                key = new Key(autoincCount);
                fld.setLong(obj, autoincCount);
                break;            
              default:
                throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE, fld.getType());
            }
        } catch (Exception x) { 
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
        autoincCount += 1;
        getStorage().modify(obj);
        super.insert(key, obj, false);
    }

    public T[] getPrefix(String prefix) { 
        ArrayList<T> list = getList(new Key(prefix, true), new Key(prefix + Character.MAX_VALUE, false));
        return (T[])list.toArray((T[])Array.newInstance(cls, list.size()));        
    }

    public T[] prefixSearch(String key) { 
        ArrayList<T> list = prefixSearchList(key);
        return (T[])list.toArray((T[])Array.newInstance(cls, list.size()));
    }

    public T[] get(Key from, Key till) {
        ArrayList list = new ArrayList();
        if (root != 0) { 
            BtreePage.find((StorageImpl)getStorage(), root, checkKey(from), checkKey(till), this, height, list);
        }
        return (T[])list.toArray((T[])Array.newInstance(cls, list.size()));
    }

    public T[] toArray() {
        T[] arr = (T[])Array.newInstance(cls, nElems);
        if (root != 0) { 
            BtreePage.traverseForward((StorageImpl)getStorage(), root, type, height, arr, 0);
        }
        return arr;
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

class BtreeCaseInsensitiveFieldIndex<T>  extends BtreeFieldIndex<T> {    
    BtreeCaseInsensitiveFieldIndex() {}

    BtreeCaseInsensitiveFieldIndex(Class cls, String fieldName, boolean unique) {
        super(cls, fieldName, unique);
    }

    BtreeCaseInsensitiveFieldIndex(Class cls, String fieldName, boolean unique, long autoincCount) {
        super(cls, fieldName, unique, autoincCount);
    }

    Key checkKey(Key key) { 
        if (key != null && key.oval instanceof String) { 
            key = new Key(((String)key.oval).toLowerCase(), key.inclusion != 0);
        }
        return super.checkKey(key);
    }  

    public boolean isCaseInsensitive() { 
        return true;
    }
}
