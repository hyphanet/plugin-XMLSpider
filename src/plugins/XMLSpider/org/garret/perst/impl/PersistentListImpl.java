package plugins.XMLSpider.org.garret.perst.impl;
import plugins.XMLSpider.org.garret.perst.*;

import  java.util.*;
import  java.lang.reflect.Array;

class PersistentListImpl<E extends IPersistent> extends PersistentCollection<E> implements IPersistentList<E>
{
    int      nElems;
    ListPage root;

    transient int modCount;

    static final int nLeafPageItems = (Page.pageSize-ObjectHeader.sizeof-8)/4;
    static final int nIntermediatePageItems = (Page.pageSize-ObjectHeader.sizeof-12)/8;

    static class TreePosition {
        /**
         * B-Tree page where element is located
         */
        ListPage page;

        /**
         * Index of first element at the page
         */
        int index;
    }

    
    static class ListPage extends Persistent {
        int   nItems;
        Link  items;

        Object get(int i) { 
            return items.get(i);
        }

        IPersistent getPosition(TreePosition pos, int i) { 
            pos.page = this;
            pos.index -= i;
            return items.get(i);
        }

        IPersistent getRawPosition(TreePosition pos, int i) { 
            pos.page = this;
            pos.index -= i;
            return items.getRaw(i);
        }

        Object set(int i, Object obj) { 
            return items.set(i, (IPersistent)obj);
        }

        void clear(int i, int len) { 
            while (--len >= 0) { 
                items.setObject(i++, null);
            }
        }

        void prune() { 
            deallocate();
        }

        void copy(int dstOffs, ListPage src, int srcOffs, int len) { 
            System.arraycopy(src.items.toRawArray(), srcOffs, items.toRawArray(), dstOffs, len);
        }

        int getMaxItems() {
            return nLeafPageItems;
        }

        void setItem(int i, IPersistent obj) {
            items.setObject(i, obj);
        }

        int size() {
            return nItems;
        }

        ListPage clonePage() { 
            return new ListPage(getStorage());
        }

        ListPage() {}

        ListPage(Storage storage) { 
            super(storage);
            int max = getMaxItems();
            items = storage.createLink(max);
            items.setSize(max);
        }

        IPersistent remove(int i) {
            IPersistent obj = items.get(i);
            nItems -= 1;
            copy(i, this, i+1, nItems-i);
            items.setObject(nItems, null);
            modify();
            return obj;
        }

        boolean underflow() {
            return nItems < getMaxItems()/2;
        }

        ListPage add(int i, IPersistent obj) {
            int max = getMaxItems();
            modify();
            if (nItems < max) {
                copy(i+1, this, i, nItems-i);
                setItem(i, obj);
                nItems += 1;
                return null;
            } else {
                ListPage b = clonePage();
                int m = max/2;
                if (i < m) {
                    b.copy(0, this, 0, i);
                    b.copy(i+1, this, i, m-i-1);
                    copy(0, this, m-1, max-m+1);
                    b.setItem(i, obj);
                } else {
                    b.copy(0, this, 0, m);
                    copy(0, this, m, i-m);
                    copy(i-m+1, this, i, max-i);
                    setItem(i-m, obj);
                }
                clear(max-m+1, m-1);
                nItems = max-m+1;
                b.nItems = m;
                return b;
            }
        }                
    }

    static class ListIntermediatePage extends ListPage {
        int[] nChildren;

        IPersistent getPosition(TreePosition pos, int i) { 
            int j;
            for (j = 0; i >= nChildren[j]; j++) {
                i -= nChildren[j];
            }
            return ((ListPage)items.get(j)).getPosition(pos, i);
        }
            
        IPersistent getRawPosition(TreePosition pos, int i) { 
            int j;
            for (j = 0; i >= nChildren[j]; j++) {
                i -= nChildren[j];
            }
            return ((ListPage)items.get(j)).getRawPosition(pos, i);
        }

        Object get(int i) { 
            int j;
            for (j = 0; i >= nChildren[j]; j++) {
                i -= nChildren[j];
            }
            return ((ListPage)items.get(j)).get(i);
        }

        Object set(int i, Object obj) { 
            int j;
            for (j = 0; i >= nChildren[j]; j++) {
                i -= nChildren[j];
            }
            return ((ListPage)items.get(j)).set(i, obj);
        }

        ListPage add(int i, IPersistent obj) {
            int j;
            for (j = 0; i >= nChildren[j]; j++) {
                i -= nChildren[j];
            }
            ListPage pg = (ListPage)items.get(j);
            ListPage overflow = pg.add(i, obj);
            if (overflow != null) { 
                countChildren(j, pg);
                overflow = super.add(j, overflow);
            } else {
                modify();
                if (nChildren[j] != Integer.MAX_VALUE) { 
                    nChildren[j] += 1;
                }
            }                
            return overflow;
        }

        IPersistent remove(int i) {
            int j;
            for (j = 0; i >= nChildren[j]; j++) {
                i -= nChildren[j];
            }
            ListPage pg = (ListPage)items.get(j);
            IPersistent obj = pg.remove(i);
            modify();
            if (pg.underflow()) { 
                handlePageUnderflow(pg, j);
            } else {
                if (nChildren[j] != Integer.MAX_VALUE) { 
                    nChildren[j] -= 1;
                }
            }
            return obj;
        }

        void countChildren(int i, ListPage pg) {
            if (nChildren[i] != Integer.MAX_VALUE) { 
                nChildren[i] = pg.size();
            }
        }
        
        void prune() {
            for (int i = 0; i < nItems; i++) {
                ((ListPage)items.get(i)).prune();
            }
            deallocate();
        }

        void handlePageUnderflow(ListPage a, int r) {
            int an = a.nItems;
            int max = a.getMaxItems();
            if (r+1 < nItems) { // exists greater page
                ListPage b = (ListPage)items.get(r+1);
                int bn = b.nItems; 
                Assert.that(bn >= an);
                if (an + bn > max) { 
                    // reallocation of nodes between pages a and b
                    int i = bn - ((an + bn) >> 1);
                    b.modify();
                    a.copy(an, b, 0, i);
                    b.copy(0, b, i, bn-i);
                    b.clear(bn-i, i);
                    b.nItems -= i;
                    a.nItems += i;
                    nChildren[r] = a.size();
                    countChildren(r+1, b);
                } else { // merge page b to a  
                    a.copy(an, b, 0, bn);
                    a.nItems += bn;
                    nItems -= 1;
                    nChildren[r] = nChildren[r+1];
                    copy(r+1, this, r+2, nItems-r-1);
                    countChildren(r, a);
                    items.set(nItems, null);
                    b.deallocate();
                }
            } else { // page b is before a
                ListPage b = (ListPage)items.get(r-1);
                int bn = b.nItems; 
                Assert.that(bn >= an);
                b.modify();
                if (an + bn > max) { 
                    // reallocation of nodes between pages a and b
                    int i = bn - ((an + bn) >> 1);
                    a.copy(i, a, 0, an);
                    a.copy(0, b, bn-i, i);
                    b.clear(bn-i, i);
                    b.nItems -= i;
                    a.nItems += i;
                    nChildren[r-1] = b.size();
                    countChildren(r, a);
                } else { // merge page b to a
                    b.copy(bn, a, 0, an);
                    b.nItems += an;
                    nItems -= 1;
                    nChildren[r-1] = nChildren[r];
                    countChildren(r-1, b);
                    items.set(r, null);
                    a.deallocate();
                }
            }
        }

        void copy(int dstOffs, ListPage src, int srcOffs, int len) { 
            super.copy(dstOffs, src, srcOffs, len);
            System.arraycopy(((ListIntermediatePage)src).nChildren, srcOffs, nChildren, dstOffs, len);
        }

        int getMaxItems() {
            return nIntermediatePageItems;
        }

        void setItem(int i, IPersistent obj) {
            super.setItem(i, obj);
            nChildren[i] = ((ListPage)obj).size();
        }

        int size() {
            if (nChildren[nItems-1] == Integer.MAX_VALUE) { 
                return Integer.MAX_VALUE;
            } else { 
                int n = 0;
                for (int i = 0; i < nItems; i++) { 
                    n += nChildren[i];
                }
                return n;
            }
        }

        ListPage clonePage() { 
            return new ListIntermediatePage(getStorage());
        }

        ListIntermediatePage() {}

        ListIntermediatePage(Storage storage) { 
            super(storage);
            nChildren = new int[nIntermediatePageItems];
        }
    }

    public E get(int i) { 
        if (i < 0 || i >= nElems) { 
            throw new IndexOutOfBoundsException("index=" + i + ", size=" + nElems);
        }
        return (E)root.get(i);
    }
    
    E getPosition(TreePosition pos, int i) { 
        if (i < 0 || i >= nElems) { 
            throw new IndexOutOfBoundsException("index=" + i + ", size=" + nElems);
        }
        if (pos.page != null && i >= pos.index && i < pos.index + pos.page.nItems) { 
            return (E)pos.page.items.get(i - pos.index);
        }
        pos.index = i;
        return (E)root.getPosition(pos, i);
    }

    IPersistent getRawPosition(TreePosition pos, int i) { 
        if (i < 0 || i >= nElems) { 
            throw new IndexOutOfBoundsException("index=" + i + ", size=" + nElems);
        }
        if (pos.page != null && i >= pos.index && i < pos.index + pos.page.nItems) { 
            return pos.page.items.getRaw(i - pos.index);
        }
        pos.index = i;
        return root.getRawPosition(pos, i);
    }

    public E set(int i, E obj) { 
        if (i < 0 || i >= nElems) { 
            throw new IndexOutOfBoundsException("index=" + i + ", size=" + nElems);
        }
        return (E)root.set(i, obj);
    }
       
    public Object[] toArray() { 
        int n = nElems;
        Object[] arr = new Object[n];
        Iterator<E> iterator = listIterator(0);    
        for (int i = 0; i < n; i++) {
            arr[i] = iterator.next();
        }
        return arr;
    }

    public <T> T[] toArray(T[] arr) {
        int n = nElems;
        if (arr.length < n) { 
            arr = (T[])Array.newInstance(arr.getClass().getComponentType(), n);
        }
        Iterator<E> iterator = listIterator(0);    
        for (int i = 0; i < n; i++) {
            arr[i] = (T)iterator.next();
        }
        return arr;
    }

    public boolean isEmpty() { 
        return nElems == 0;
    }    

    public int size() {
        return nElems;
    }

    public boolean contains(Object o) {         
	Iterator e = iterator();
	if (o==null) {
	    while (e.hasNext()) { 
		if (e.next()==null) {
		    return true;
                }
            }
	} else {
	    while (e.hasNext()) {
		if (o.equals(e.next())) {
		    return true;
                }
            }
	}
	return false;
    }

    public boolean add(E o) {
        add(nElems, o);
        return true;
    }

    public void add(int i, E o) {
        if (i < 0 || i > nElems) { 
            throw new IndexOutOfBoundsException("index=" + i + ", size=" + nElems);
        }
        IPersistent obj = (IPersistent)o;
        ListPage overflow = root.add(i, obj);
        if (overflow != null) { 
            ListIntermediatePage pg = new ListIntermediatePage(getStorage());
            pg.setItem(0, overflow);            
            pg.items.setObject(1, root);
            pg.nChildren[1] = Integer.MAX_VALUE;
            pg.nItems = 2;
            root = pg;
        }
        nElems += 1;
        modCount += 1;
        modify();
    }
   
    public E remove(int i) {
        if (i < 0 || i >= nElems) { 
            throw new IndexOutOfBoundsException("index=" + i + ", size=" + nElems);
        }
        E obj = (E)root.remove(i);
        if (root.nItems == 1 && root instanceof ListIntermediatePage) {
            ListPage newRoot = (ListPage)root.items.get(0);
            root.deallocate();
            root = newRoot;
        }
        nElems -= 1;
        modCount += 1;
        modify();
        return obj;
    }

    public void clear() {
        modCount += 1;
        root.prune();
        root = new ListPage(getStorage()); 
        nElems = 0;
        modify();
    }        
    
    public int indexOf(Object o) {
	ListIterator<E> e = listIterator();
	if (o==null) {
	    while (e.hasNext())
		if (e.next()==null)
		    return e.previousIndex();
	} else {
	    while (e.hasNext())
		if (o.equals(e.next()))
		    return e.previousIndex();
	}
	return -1;
    }

    public int lastIndexOf(Object o) {
	ListIterator<E> e = listIterator(size());
	if (o==null) {
	    while (e.hasPrevious())
		if (e.previous()==null)
		    return e.nextIndex();
	} else {
	    while (e.hasPrevious())
		if (o.equals(e.previous()))
		    return e.nextIndex();
	}
	return -1;
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
	return new Itr();
    }

    public ListIterator<E> listIterator() {
	return listIterator(0);
    }

    public ListIterator<E> listIterator(int index) {
	if (index<0 || index>size())
	  throw new IndexOutOfBoundsException("Index: "+index);

	return new ListItr(index);
    }

    private class Itr extends TreePosition implements  PersistentIterator, Iterator<E> {
	/**
	 * Index of element to be returned by subsequent call to next.
	 */
	int cursor = 0;

	/**
	 * Index of element returned by most recent call to next or
	 * previous.  Reset to -1 if this element is deleted by a call
	 * to remove.
	 */
	int lastRet = -1;

	/**
	 * The modCount value that the iterator believes that the backing
	 * List should have.  If this expectation is violated, the iterator
	 * has detected concurrent modification.
	 */
	int expectedModCount = modCount;

	public boolean hasNext() {
            return cursor != size();
	}

        public int nextOid() {
            checkForComodification();
	    try {
		int oid = getRawPosition(this, cursor).getOid();
		lastRet = cursor++;
		return oid;
	    } catch(IndexOutOfBoundsException e) {
		checkForComodification();
		throw new NoSuchElementException();
	    }
        }

	public E next() {
            checkForComodification();
	    try {
		E next = getPosition(this, cursor);
		lastRet = cursor++;
		return next;
	    } catch(IndexOutOfBoundsException e) {
		checkForComodification();
		throw new NoSuchElementException();
	    }
	}

	public void remove() {
	    if (lastRet == -1) {
		throw new IllegalStateException();
            }
            checkForComodification();

	    try {
		PersistentListImpl.this.remove(lastRet);
		if (lastRet < cursor) {
		    cursor--;
                }
                page = null;
		lastRet = -1;
		expectedModCount = modCount;
	    } catch(IndexOutOfBoundsException e) {
		throw new ConcurrentModificationException();
	    }
	}

	final void checkForComodification() {
	    if (modCount != expectedModCount) {
		throw new ConcurrentModificationException();
            }
	}
    }

    private class ListItr extends Itr implements ListIterator<E> {
	ListItr(int index) {
	    cursor = index;
	}

	public boolean hasPrevious() {
	    return cursor != 0;
	}

        public E previous() {
            checkForComodification();
            try {
                int i = cursor - 1;
                E previous = getPosition(this, i);
                lastRet = cursor = i;
                return previous;
            } catch(IndexOutOfBoundsException e) {
                checkForComodification();
                throw new NoSuchElementException();
            }
        }

	public int nextIndex() {
	    return cursor;
	}

	public int previousIndex() {
	    return cursor-1;
	}

	public void set(E o) {
	    if (lastRet == -1) {
		throw new IllegalStateException();
            }
            checkForComodification();

	    try {
		PersistentListImpl.this.set(lastRet, o);
		expectedModCount = modCount;
	    } catch(IndexOutOfBoundsException e) {
		throw new ConcurrentModificationException();
	    }
	}

	public void add(E o) {
            checkForComodification();

	    try {
		PersistentListImpl.this.add(cursor++, o);
		lastRet = -1;
                page = null;
		expectedModCount = modCount;
	    } catch(IndexOutOfBoundsException e) {
		throw new ConcurrentModificationException();
	    }
	}
    }

    public List<E> subList(int fromIndex, int toIndex) {
        return new SubList<E>(this, fromIndex, toIndex);
    }

    protected void removeRange(int fromIndex, int toIndex) {
        while (fromIndex < toIndex) { 
            remove(fromIndex);
            toIndex -= 1;
        }
    }

    PersistentListImpl() {}
    
    PersistentListImpl(Storage storage) { 
        super(storage);
        root = new ListPage(storage);
    }
}
        
class SubList<E extends IPersistent> extends AbstractList<E> {
    private PersistentListImpl<E> l;
    private int offset;
    private int size;
    private int expectedModCount;

    SubList(PersistentListImpl<E> list, int fromIndex, int toIndex) {
        if (fromIndex < 0) {
            throw new IndexOutOfBoundsException("fromIndex = " + fromIndex);
        }
        if (toIndex > list.size()) {
            throw new IndexOutOfBoundsException("toIndex = " + toIndex);
        }
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException("fromIndex(" + fromIndex +
                                               ") > toIndex(" + toIndex + ")");
        }
        l = list;
        offset = fromIndex;
        size = toIndex - fromIndex;
        expectedModCount = l.modCount;
    }

    public E set(int index, E element) {
        rangeCheck(index);
        checkForComodification();
        return l.set(index+offset, element);
    }

    public E get(int index) {
        rangeCheck(index);
        checkForComodification();
        return l.get(index+offset);
    }

    public int size() {
        checkForComodification();
        return size;
    }

    public void add(int index, E element) {
        if (index<0 || index>size) {
            throw new IndexOutOfBoundsException();
        }
        checkForComodification();
        l.add(index+offset, element);
        expectedModCount = l.modCount;
        size++;
        modCount++;
    }

    public E remove(int index) {
        rangeCheck(index);
        checkForComodification();
        E result = l.remove(index+offset);
        expectedModCount = l.modCount;
        size--;
        modCount++;
        return result;
    }

    protected void removeRange(int fromIndex, int toIndex) {
        checkForComodification();
        l.removeRange(fromIndex+offset, toIndex+offset);
        expectedModCount = l.modCount;
        size -= (toIndex-fromIndex);
        modCount++;
    }

    public boolean addAll(Collection<? extends E> c) {
        return addAll(size, c);
    }

    public boolean addAll(int index, Collection<? extends E> c) {
        if (index<0 || index>size) {
            throw new IndexOutOfBoundsException("Index: "+index+", Size: "+size);
        }
        int cSize = c.size();
        if (cSize==0) {
            return false;
        }
        checkForComodification();
        l.addAll(offset+index, c);
        expectedModCount = l.modCount;
        size += cSize;
        modCount++;
        return true;
    }

    public Iterator<E> iterator() {
        return listIterator();
    }

    public ListIterator<E> listIterator(final int index) {
        checkForComodification();
        if (index<0 || index>size) {
            throw new IndexOutOfBoundsException("Index: "+index+", Size: "+size);
        }
        return new ListIterator<E>() {
            private ListIterator<E> i = l.listIterator(index+offset);

            public boolean hasNext() {
                return nextIndex() < size;
            }

            public E next() {
                if (hasNext()) {
                    return i.next();
                } else {
                    throw new NoSuchElementException();
                }
            }

            public boolean hasPrevious() {
                return previousIndex() >= 0;
            }

            public E previous() {
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
                expectedModCount = l.modCount;
                size--;
                modCount++;
            }

            public void set(E o) {
                i.set(o);
            }

            public void add(E o) {
                i.add(o);
                expectedModCount = l.modCount;
                size++;
                modCount++;
            }        
        };
    }

    public List<E> subList(int fromIndex, int toIndex) {
        return new SubList<E>(l, offset+fromIndex, offset+toIndex);
    }

    private void rangeCheck(int index) {
        if (index<0 || index>=size) {
            throw new IndexOutOfBoundsException("Index: "+index+
                                                ",Size: "+size);
        }
    }

    private void checkForComodification() {
        if (l.modCount != expectedModCount) {
            throw new ConcurrentModificationException();
        }
    }
}
