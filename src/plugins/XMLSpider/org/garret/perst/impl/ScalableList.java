package plugins.XMLSpider.org.garret.perst.impl;
import plugins.XMLSpider.org.garret.perst.*;

import  java.util.*;

class ScalableList<E extends IPersistent> extends PersistentCollection<E> implements IPersistentList<E>
{
    Link<E>            small;
    IPersistentList<E> large;

    static final int BTREE_THRESHOLD = 128;

    ScalableList(Storage storage, int initialSize) { 
        super(storage);
        if (initialSize <= BTREE_THRESHOLD) { 
            small = storage.<E>createLink(initialSize);
        } else { 
            large = storage.<E>createList();
        }
    }

    ScalableList() {}

    public E get(int i) { 
        return small != null ? small.get(i) : large.get(i);
    }
    
    public E set(int i, E obj) { 
        return small != null ? small.set(i, obj) : large.set(i, obj);
    }
       
    public boolean isEmpty() { 
        return small != null ? small.isEmpty() : large.isEmpty();
    }    

    public int size() {
        return small != null ? small.size() : large.size();
    }

    public boolean contains(Object o) {         
        if (o instanceof IPersistent) { 
            IPersistent p = (IPersistent)o;
            return small != null ? small.contains(p) : large.contains(p);
        }
        return false;
    }

    public <T> T[] toArray(T a[]) { 
        return small != null ? small.<T>toArray(a) : large.<T>toArray(a);
    }

    public Object[] toArray() { 
        return small != null ? small.toArray() : large.toArray();
    }
    public boolean add(E o) {
        add(size(), o);
        return true;
    }

    public void add(int i, E o) {
        if (small != null) { 
            if (small.size() == BTREE_THRESHOLD) { 
                large = getStorage().<E>createList();
                large.addAll(small);
                large.add(i, o);
                modify();
                small = null;
            } else { 
                small.add(i, o);
            }
        } else { 
            large.add(i, o);
        }
    }

    public E remove(int i) {
        return small != null ? small.remove(i) : large.remove(i);
    }

    public void clear() {
        if (large != null) { 
            large.clear();            
        } else { 
            small.clear();
        }
    }   

    public int indexOf(Object o) {
        return small != null ? small.indexOf(o) : large.indexOf(o);
    }    

    public int lastIndexOf(Object o) {
        return small != null ? small.lastIndexOf(o) : large.lastIndexOf(o);
    }

    public boolean addAll(int index, Collection<? extends E> c) {
	boolean modified = false;
	Iterator<? extends E> e = c.iterator();
	while (e.hasNext()) {
	    add(index++, e.next());
	    modified = true;
	}
	return modified;
    }    
            
    public Iterator<E> iterator() {
        return small != null ? small.iterator() : large.iterator();
    }
    
    public ListIterator<E> listIterator() {
	return listIterator(0);
    }

    public ListIterator<E> listIterator(int index) {
        return small != null ? small.listIterator(index) : large.listIterator(index);
    }

    public List<E> subList(int fromIndex, int toIndex) {
        return small != null ? small.subList(fromIndex, toIndex) : large.subList(fromIndex, toIndex);
    }
}
        