package plugins.XMLSpider.org.garret.perst.impl;
import plugins.XMLSpider.org.garret.perst.*;

import  java.util.*;

public class RelationImpl<M extends IPersistent, O extends IPersistent> extends Relation<M,O> {
    public int size() {
        return link.size();
    }
    
    public void setSize(int newSize) {
        link.setSize(newSize);
    }
    
    public boolean isEmpty() {
        return link.isEmpty();
    }
    
    public boolean remove(Object o) {
        return link.remove(o);
    }

    public M get(int i) {
        return link.get(i);
    }

    public IPersistent getRaw(int i) {
        return link.getRaw(i);
    }

    public M set(int i, M obj) {
        return link.set(i, obj);
    }

    public void setObject(int i, M obj) {
        link.setObject(i, obj);
    }

    public void removeObject(int i) {
        link.removeObject(i);
    }

    public M remove(int i) {
        return link.remove(i);
    }

    public void insert(int i, M obj) {
        link.insert(i, obj);
    }

    public void add(int i, M obj) {
        link.add(i, obj);
    }

    public boolean add(M obj) {
        return link.add(obj);
    }

    public void addAll(M[] arr) {
        link.addAll(arr);
    }

    public void addAll(M[] arr, int from, int length) {
        link.addAll(arr, from, length);
    }

    public boolean addAll(Link<M> anotherLink) {
        return link.addAll(anotherLink); 
    }

    public IPersistent[] toPersistentArray() {
        return link.toPersistentArray();
    }

    public IPersistent[] toRawArray() {
        return link.toRawArray();
    }

    public Object[] toArray() {
        return link.toArray();
    }

    public <T> T[] toArray(T[] arr) {
        return link.<T>toArray(arr);
    }

    public boolean contains(Object obj) {
        return link.contains(obj);
    }

    public boolean containsObject(M obj) {
        return link.containsObject(obj);
    }

    public int indexOf(Object obj) {
        return link.indexOf(obj);
    }

    public int lastIndexOf(Object obj) {
        return link.lastIndexOf(obj);
    }
      
    public int indexOfObject(Object obj) {
        return link.indexOfObject(obj);
    }

    public int lastIndexOfObject(Object obj) {
        return link.lastIndexOfObject(obj);
    }
      
    public void clear() {
        link.clear();
    }

    public Iterator<M> iterator() {
        return link.iterator();
    }
    

    public boolean containsAll(Collection<?> c) {        
        return link.containsAll(c);
    }

    public boolean containsElement(int i, M obj) {
        return link.containsElement(i, obj);
    }

    public boolean addAll(Collection<? extends M> c) {
        return link.addAll(c);
    }

    public boolean addAll(int index, Collection<? extends M> c) {
        return link.addAll(index, c);
    }

    public boolean removeAll(Collection<?> c) {
        return link.removeAll(c);
    }

    public boolean retainAll(Collection<?> c) {
        return link.retainAll(c);
    }

    public void pin() { 
        link.pin();
    }
    
    public void unpin() { 
        link.unpin();
    }
    
    public List<M> subList(int fromIndex, int toIndex) {
        return link.subList(fromIndex, toIndex);
    }

    public ListIterator<M> listIterator(int index) {
        return link.listIterator(index);
    }

    public ListIterator<M> listIterator() {
        return link.listIterator();
    }

    public IterableIterator<M> select(Class cls, String predicate) { 
        Query<M> query = new QueryImpl<M>(getStorage());
        return query.select(cls, link.iterator(), predicate);
    }

    RelationImpl() {}

    RelationImpl(O owner) { 
        super(owner);
        link = new LinkImpl<M>(8);
    }

    Link<M> link;
}
