package plugins.XMLSpider.org.garret.perst.impl;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import plugins.XMLSpider.org.garret.perst.IPersistentSet;
import plugins.XMLSpider.org.garret.perst.Link;
import plugins.XMLSpider.org.garret.perst.PersistentCollection;

class ScalableSet<T> extends PersistentCollection<T> implements IPersistentSet<T> { 
    Link<T>           link;
    IPersistentSet<T> set;

    static final int BTREE_THRESHOLD = 128;

    ScalableSet(StorageImpl storage, int initialSize) { 
        super(storage);
        if (initialSize <= BTREE_THRESHOLD) { 
            link = storage.<T>createLink(initialSize);
        } else { 
            set = storage.<T>createSet();
        }
    }

    ScalableSet() {}

    public boolean isEmpty() { 
        return size() != 0;
    }

    public int size() { 
        return link != null ? link.size() : set.size();
    }

    public void clear() { 
        if (link != null) { 
            link.clear();
            modify();
        } else { 
            set.clear();
        }
    }

    public boolean contains(Object o) {
        return link != null ? link.contains(o) : set.contains(o);
    }
    
    public Object[] toArray() { 
        return link != null ? link.toArray() : set.toArray();
    }

    public <E> E[] toArray(E a[]) { 
        return link != null ? link.<E>toArray(a) : set.<E>toArray(a);
    }

    public Iterator<T> iterator() { 
        return link != null ? link.iterator() : set.iterator();
    }

    public boolean add(T obj) { 
        if (link != null) { 
            if (link.indexOf(obj) >= 0) { 
                return false;
            }
            if (link.size() == BTREE_THRESHOLD) { 
                set = getStorage().<T>createSet();
                for (int i = 0, n = link.size(); i < n; i++) { 
                    set.add(link.get(i));
                }
                link = null;
                modify();
                set.add(obj);
            } else { 
                modify();
                link.add(obj);
            }
            return true;
        } else { 
            return set.add(obj);
        }
    }

    public boolean remove(Object o) { 
        if (link != null) {
            if (link.remove(o)) { 
                modify();
                return true;
            } 
            return false;
        } else { 
            return set.remove(o);
        }
    }
    
    public int hashCode() {
        int h = 0;
        Iterator<T> i = iterator();
        while (i.hasNext()) {
            h += getStorage().getOid(i.next());
        }
        return h;
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

    public void deallocate() { 
        if (set != null) { 
            set.deallocate();
        }
        super.deallocate();
    }
}
   