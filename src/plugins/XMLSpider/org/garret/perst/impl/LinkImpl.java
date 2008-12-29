package plugins.XMLSpider.org.garret.perst.impl;
import plugins.XMLSpider.org.garret.perst.*;

import  java.util.*;
import  java.lang.reflect.Array;

public class LinkImpl<T extends IPersistent> implements EmbeddedLink<T>, ICloneable 
{ 
    private final void modify() { 
        if (owner != null) { 
            owner.modify();
        }
    }

    public int size() {
        return used;
    }

    public Object clone() throws CloneNotSupportedException { 
        return super.clone();
        
    }

    public void setSize(int newSize) { 
        if (newSize < used) { 
            for (int i = used; --i >= newSize; arr[i] = null);
        } else { 
            reserveSpace(newSize - used);            
        }
        used = newSize;
    }

    public T get(int i) {
        if (i < 0 || i >= used) { 
            throw new IndexOutOfBoundsException();
        }
        return (T)loadElem(i);
    }

    public IPersistent getRaw(int i) {
        if (i < 0 || i >= used) { 
            throw new IndexOutOfBoundsException();
        }
        return arr[i];
    }

    public void pin() { 
        for (int i = 0, n = used; i < n; i++) { 
            arr[i] = loadElem(i);
        }
    }

    public void unpin() { 
        for (int i = 0, n = used; i < n; i++) { 
            IPersistent elem = arr[i];
            if (elem != null && !elem.isRaw() && elem.isPersistent()) { 
                arr[i] = new PersistentStub(elem.getStorage(), elem.getOid());
            }
        }
    }


    public T set(int i, T obj) {
        if (i < 0 || i >= used) { 
            throw new IndexOutOfBoundsException();
        }
        T prev = (T)loadElem(i);
        arr[i] = obj;
        modify();
        return prev;
    }

    public void setObject(int i, T obj) {
        if (i < 0 || i >= used) { 
            throw new IndexOutOfBoundsException();
        }
        arr[i] = obj;
        modify();
    }

    public boolean isEmpty() {
        return used == 0;
    }

    protected void removeRange(int fromIndex, int toIndex) {
        int size = used;
 	int numMoved = size - toIndex;
        System.arraycopy(arr, toIndex, arr, fromIndex, numMoved);

	// Let gc do its work
	int newSize = size - (toIndex-fromIndex);
	while (size != newSize) {
	    arr[--size] = null;
        }                                       
        used = size;
        modify();
    }

 
    public void removeObject(int i) {
        if (i < 0 || i >= used) { 
            throw new IndexOutOfBoundsException();
        }
        used -= 1;
        System.arraycopy(arr, i+1, arr, i, used-i);
        arr[used] = null;
        modify();
    }

    public T remove(int i) {
        if (i < 0 || i >= used) { 
            throw new IndexOutOfBoundsException();
        }
        T obj = (T)loadElem(i);
        used -= 1;
        System.arraycopy(arr, i+1, arr, i, used-i);
        arr[used] = null;
        modify();
        return obj;
    }

    void reserveSpace(int len) { 
        if (used + len > arr.length) { 
            IPersistent[] newArr = new IPersistent[used + len > arr.length*2 ? used + len : arr.length*2];
            System.arraycopy(arr, 0, newArr, 0, used);
            arr = newArr;
        }
        modify();
    }

    public void add(int i, T obj) { 
        insert(i, obj);
    }

    public void insert(int i, T obj) { 
         if (i < 0 || i > used) { 
            throw new IndexOutOfBoundsException();
        }
        reserveSpace(1);
        System.arraycopy(arr, i, arr, i+1, used-i);
        arr[i] = obj;
        used += 1;
    }

    public boolean add(T obj) {
        reserveSpace(1);
        arr[used++] = obj;
        return true;
    }

    public void addAll(T[] a) {
        addAll(a, 0, a.length);
    }
    
    public boolean addAll(int index, Collection<? extends T> c) {
	boolean modified = false;
	Iterator<? extends T> e = c.iterator();
	while (e.hasNext()) {
	    add(index++, e.next());
	    modified = true;
	}
	return modified;
    }

    public void addAll(T[] a, int from, int length) {
        reserveSpace(length);
        System.arraycopy(a, from, arr, used, length);
        used += length;
    }

    public boolean addAll(Link<T> link) {        
        int n = link.size();
        reserveSpace(n);
        for (int i = 0, j = used; i < n; i++, j++) { 
            arr[j] = link.getRaw(i);
        }
        used += n;
        return true;
    }

    public Object[] toArray() {
        return toPersistentArray();
    }

    public IPersistent[] toRawArray() {
        return arr;
    }

    public IPersistent[] toPersistentArray() {
        IPersistent[] a = new IPersistent[used];
        for (int i = used; --i >= 0;) { 
            a[i] = loadElem(i);
        }
        return a;
    }
    
    public <T> T[] toArray(T[] arr) {
        if (arr.length < used) { 
            arr = (T[])Array.newInstance(arr.getClass().getComponentType(), used);
        }
        for (int i = used; --i >= 0;) { 
            arr[i] = (T)loadElem(i);
        }
        if (arr.length > used) { 
            arr[used] = null;
        }
        return arr;
    }
    
    public boolean contains(Object obj) {
        return indexOf(obj) >= 0;
    }

    public boolean containsObject(T obj) {
        return indexOfObject(obj) >= 0;
    }

    public int lastIndexOfObject(Object obj) {
        int oid;
        IPersistent[] a = arr;
        if (obj instanceof IPersistent && (oid = ((IPersistent)obj).getOid()) != 0) { 
            for (int i = used; --i >= 0;) {
                IPersistent elem = a[i];
                if (elem != null && elem.getOid() == oid) {
                    return i;
                }
            }
        } else { 
            for (int i = used; --i >= 0;) {
                if (a[i] == obj) {
                    return i;
                }
            }
        }
        return -1;
    }
    
    public int indexOfObject(Object obj) {
        int oid;
        IPersistent[] a = arr;
        if (obj instanceof IPersistent && (oid = ((IPersistent)obj).getOid()) != 0) { 
            for (int i = 0, n = used; i < n; i++) {
                IPersistent elem = a[i];
                if (elem != null && elem.getOid() == oid) {
                    return i;
                }
            }
        } else { 
            for (int i = 0, n = used; i < n; i++) {
                if (a[i] == obj) {
                    return i;
                }
            }
        }
        return -1;
    }
    
    public int indexOf(Object obj) {
        if (obj == null) { 
            for (int i = 0, n = used; i < n; i++) {
                if (arr[i] == null) {
                    return i;
                }
            }
        } else { 
            for (int i = 0, n = used; i < n; i++) {
                if (obj.equals(loadElem(i))) {
                    return i;
                }
            }
        }
        return -1;
    }
    
    public int lastIndexOf(Object obj) {
        if (obj == null) { 
            for (int i = used; --i >= 0;) {
                if (arr[i] == null) {
                    return i;
                }
            }
        } else { 
            for (int i = used; --i >= 0;) {
                if (obj.equals(loadElem(i))) {
                    return i;
                }
            }
        }
        return -1;
    }
    
    public boolean containsElement(int i, T obj) {
        IPersistent elem = arr[i];
        return elem == obj || (elem != null && elem.getOid() != 0 && elem.getOid() == obj.getOid());
    }

    public void clear() { 
        for (int i = used; --i >= 0;) { 
            arr[i] = null;
        }
        used = 0;
        modify();
    }

    public List<T> subList(int fromIndex, int toIndex) {
        return new SubList<T>(this, fromIndex, toIndex);
    }

    static class SubList<T extends IPersistent> extends AbstractList<T> implements RandomAccess {
        private LinkImpl<T> l;
        private int offset;
        private int size;

        SubList(LinkImpl<T> list, int fromIndex, int toIndex) {
            if (fromIndex < 0) {
                throw new IndexOutOfBoundsException("fromIndex = " + fromIndex);
            }
            if (toIndex > list.size()) {
                throw new IndexOutOfBoundsException("toIndex = " + toIndex);
            }
            if (fromIndex > toIndex) { 
                throw new IllegalArgumentException("fromIndex(" + fromIndex + ") > toIndex(" + toIndex + ")");
            }
            l = list;
            offset = fromIndex;
            size = toIndex - fromIndex;
        }
        
        public T set(int index, T element) {
            rangeCheck(index);
            return l.set(index+offset, element);
        }
        
        public T get(int index) {
            rangeCheck(index);
            return l.get(index+offset);
        }
        
        public int size() {
            return size;
        }

        public void add(int index, T element) {
            if (index<0 || index>size) {
                throw new IndexOutOfBoundsException();
            }
            l.add(index+offset, element);
            size++;
        }
        
        public T remove(int index) {
            rangeCheck(index);
            T result = l.remove(index+offset);
            size--;
            return result;
        }

        protected void removeRange(int fromIndex, int toIndex) {
            l.removeRange(fromIndex+offset, toIndex+offset);
            size -= (toIndex-fromIndex);
        }

        public boolean addAll(Collection<? extends T> c) {
            return addAll(size, c);
        }
        
        public boolean addAll(int index, Collection<? extends T> c) {
            if (index<0 || index>size) {
                throw new IndexOutOfBoundsException("Index: "+index+", Size: "+size);
            }
            int cSize = c.size();
            if (cSize==0) {
                return false;
            }
            l.addAll(offset+index, c);
            size += cSize;
            return true;
        }

        public Iterator<T> iterator() {
            return listIterator();
        }

        public ListIterator<T> listIterator(final int index) {
            if (index<0 || index>size) {
                throw new IndexOutOfBoundsException("Index: "+index+", Size: "+size);
            }
            return new ListIterator<T>() {
                private ListIterator<T> i = l.listIterator(index+offset);
                
                public boolean hasNext() {
                    return nextIndex() < size;
                }
                
                public T next() {
                    if (hasNext()) { 
                        return i.next();
                    } else { 
                        throw new NoSuchElementException();
                    }
                }
                
                public boolean hasPrevious() {
                    return previousIndex() >= 0;
                }

                public T previous() {
                    if (hasPrevious()) { 
                        return i.previous();
                    } else { 
                        throw new NoSuchElementException();
                    } 
                }

                public int nextIndex() {
                    return i.nextIndex() - offset;
                }

                public int previousIndex() {
                    return i.previousIndex() - offset;
                }
                
                public void remove() {
                    i.remove();
                    size--;
                }

                public void set(T o) {
                    i.set(o);
                }

                public void add(T o) {
                    i.add(o);
                    size++;
                }
            };
        }

        public List<T> subList(int fromIndex, int toIndex) {
            return new SubList<T>(l, offset+fromIndex, offset+toIndex);
        }
        
        private void rangeCheck(int index) {
            if (index<0 || index>=size) {
                throw new IndexOutOfBoundsException("Index: "+index+",Size: "+size);
            }
        }
    }

    static class LinkIterator<T extends IPersistent> implements PersistentIterator, ListIterator<T> { 
        private Link<T> link;
        private int     i;
        private int     last;

        LinkIterator(Link<T> link, int index) { 
            this.link = link;
            i = index;
            last = -1;
        }

        public boolean hasNext() {
            return i < link.size();
        }

        public T next() throws NoSuchElementException { 
            if (!hasNext()) { 
                throw new NoSuchElementException();
            }
            last = i;
            return link.get(i++);
        }

        public int nextIndex() { 
            return i;
        }

        public boolean hasPrevious() {
            return i > 0;
        }

        public T previous() throws NoSuchElementException { 
            if (!hasPrevious()) { 
                throw new NoSuchElementException();
            }
            return link.get(last = --i);
        }

	public int previousIndex() {
	    return i-1;
	}

        public int nextOid() throws NoSuchElementException { 
            if (!hasNext()) { 
                throw new NoSuchElementException();
            }
            return link.getRaw(i++).getOid();
        }

        public void remove() {
	    if (last < 0) { 
		throw new IllegalStateException();
            }
            link.removeObject(last);
            if (last < i) { 
                i -= 1;
            }
        }

	public void set(T o) {
	    if (last < 0) { 
		throw new IllegalStateException();
            }
            link.setObject(last, o);
        }

	public void add(T o) {
            link.insert(i++, o);
            last = -1;
        }
     }

    public boolean remove(Object o) {
        int i = indexOf(o);
        if (i >= 0) { 
            remove(i);
            return true;
        }
        return false;
    }
        
    public boolean containsAll(Collection<?> c) {
	Iterator<?> e = c.iterator();
	while (e.hasNext()) {
	    if(!contains(e.next())) {
		return false;
            }
        }
	return true;
    }

    public boolean addAll(Collection<? extends T> c) {
	boolean modified = false;
	Iterator<? extends T> e = c.iterator();
	while (e.hasNext()) {
	    if (add(e.next())) { 
		modified = true;
            }
	}
	return modified;
    }

    public boolean removeAll(Collection<?> c) {
	boolean modified = false;
	Iterator<?> e = iterator();
	while (e.hasNext()) {
	    if (c.contains(e.next())) {
		e.remove();
		modified = true;
	    }
	}
	return modified;
    }

    public boolean retainAll(Collection<?> c) {
	boolean modified = false;
	Iterator<T> e = iterator();
	while (e.hasNext()) {
	    if (!c.contains(e.next())) {
		e.remove();
		modified = true;
	    }
	}
	return modified;
    }

    public Iterator<T> iterator() { 
        return new LinkIterator<T>(this, 0);
    }

    public ListIterator<T> listIterator(int index) {
        return new LinkIterator<T>(this, index);
    }

    public ListIterator<T> listIterator() {
        return listIterator(0);
    }

    private final T loadElem(int i) 
    {
        IPersistent elem = arr[i];
        if (elem != null && elem.isRaw()) { 
            elem = ((StorageImpl)elem.getStorage()).lookupObject(elem.getOid(), null);
        }
        return (T)elem;
    }

    public IterableIterator<T> select(Class cls, String predicate) { 
        Query<T> query = new QueryImpl<T>(null);
        return query.select(cls, iterator(), predicate);
    }

    public void setOwner(IPersistent obj) { 
        owner = obj;
    }

    public IPersistent getOwner() { 
        return owner;
    }

    LinkImpl() {}

    public LinkImpl(int initSize) {
        arr = new IPersistent[initSize];
    }

    public LinkImpl(T[] arr, IPersistent owner) { 
        this.arr = arr;
        this.owner = owner;
        used = arr.length;
    }

    public LinkImpl(Link link, IPersistent owner) { 
        used = link.size();
        arr = new IPersistent[used];
        System.arraycopy(arr, 0, link.toRawArray(), 0, used);
        this.owner = owner;
    }

    IPersistent[] arr;
    int           used;
    transient IPersistent owner;
}
