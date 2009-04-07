package plugins.XMLSpider.org.garret.perst.impl;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import plugins.XMLSpider.org.garret.perst.IPersistentSet;
import plugins.XMLSpider.org.garret.perst.Key;

class PersistentSet<T> extends Btree<T> implements IPersistentSet<T> { 
    PersistentSet() { 
        type = ClassDescriptor.tpObject;
        unique = true;
    }

    public boolean isEmpty() { 
        return nElems == 0;
    }

    public boolean contains(Object o) {
        Key key = new Key(o);
        Iterator i = iterator(key, key, ASCENT_ORDER);
        return i.hasNext();
    }
    
    public <E> E[] toArray(E[] arr) { 
        return (E[])super.toArray((T[])arr);
    }

    public boolean add(T obj) { 
        return put(new Key(obj), obj);
    }

    public boolean remove(Object o) { 
        T obj = (T)o;
        return removeIfExists(new BtreeKey(checkKey(new Key(obj)), getStorage().getOid(obj)));
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof Set)) {
            return false;
        }
        Collection c = (Collection) o;
        if (c.size() != size()) {
            return false;
        }
        return containsAll(c);
    }

    public int hashCode() {
        int h = 0;
        Iterator i = iterator();
        while (i.hasNext()) {
            h += getStorage().getOid(i.next());
        }
        return h;
    }
}
