package plugins.XMLSpider.org.garret.perst;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Interface of sorted collection.
 * Sorted collections keeps in members in order specified by comparator.
 * Members in the collections can be located using key or range of keys.
 * The SortedCollection is efficient container of objects for in-memory databases.
 * For databases which size is significatly larger than size of page pool, operation with SortedList
 * can cause trashing and so very bad performance. Unlike other index structures SortedCollection
 * doesn't store values of keys and so search in the collection requires fetching of its members.
 */
public interface SortedCollection<T> extends IPersistent, IResource, ITable<T> { 
    /**
     * Get member with specified key.
     * @param key specified key. It should match with type of the index and should be inclusive.
     * @return object with this value of the key or <code>null</code> if key nmot found
     * @exception StorageError(StorageError.KEY_NOT_UNIQUE) exception if there are more than 
     * one objects in the collection with specified value of the key.  
     */
    public T get(Object key);

    /**
     * Get members which key value belongs to the specified range.
     * Either from boundary, either till boundary either both of them can be <code>null</code>.
     * In last case the method returns all objects from the collection.
     * @param from inclusive low boundary. If <code>null</code> then low boundary is not specified.
     * @param till inclusive high boundary. If <code>null</code> then high boundary is not specified.
     * High boundary can be inclusive or exclusive. 
     * @return array of objects which keys belongs to the specified interval, ordered by key value
     */
    public Object[] get(Object from, Object till);

    /**
     * Get members which key value belongs to the specified range.
     * Either from boundary, either till boundary either both of them can be <code>null</code>.
     * In last case the method returns all objects from the collection.
     * @param from inclusive low boundary. If <code>null</code> then low boundary is not specified.
     * @param fromInclusive specifies whether from boundary is inclusive or exclusive
     * @param till inclusive high boundary. If <code>null</code> then high boundary is not specified.
     * @param tillInclusive specifies whether till boundary is inclusive or exclusive
     * @return array of objects which keys belongs to the specified interval, ordered by key value
     */
    public Object[] get(Object from, boolean fromInclusive, Object till, boolean tillInclusive);

    /**
     * Get members which key value belongs to the specified range.
     * Either from boundary, either till boundary either both of them can be <code>null</code>.
     * In last case the method returns all objects from the collection.
     * @param from inclusive low boundary. If <code>null</code> then low boundary is not specified.
     * @param till inclusive high boundary. If <code>null</code> then high boundary is not specified.
     * @return array of objects which keys belongs to the specified interval, ordered by key value
     */
    public ArrayList<T> getList(Object from, Object till);

    /**
     * Get members which key value belongs to the specified range.
     * Either from boundary, either till boundary either both of them can be <code>null</code>.
     * In last case the method returns all objects from the collection.
     * @param from inclusive low boundary. If <code>null</code> then low boundary is not specified.
     * @param fromInclusive specifies whether from boundary is inclusive or exclusive
     * @param till inclusive high boundary. If <code>null</code> then high boundary is not specified.
     * @param tillInclusive specifies whether till boundary is inclusive or exclusive
     * @return array of objects which keys belongs to the specified interval, ordered by key value
     */
    public ArrayList<T> getList(Object from, boolean fromInclusive, Object till, boolean tillInclusive);

    /**
     * Add new member to collection
     * @param obj new member
     * @return <code>true</code> if object is successfully added in the index, 
     * <code>false</code> if collection was declared as unique and there is already member with such value
     * of the key in the collection. 
     */
    public boolean add(T obj);

    /**
     * Check if collections contains specified object instance
     * @param obj object to be searched in the collection
     * @return <code>true</code> if specified member belongs to the collection
     */
    public boolean containsObject(T obj);

    /**
     * Check if collections contains object with specified key
     * @param key searched key
     * @return <code>true</code> if collection contains object with specified key
     */
    public boolean containsKey(Object key);

    /**
     * Get iterator for traversing all collection members.
     * This iterator fetches all selected objects in memory. 
     * It supports remove() method and all modifications of collection during iteration
     * has no influence on iterator.
     * @return collection iterator
     */
    public Iterator<T> iterator();
    /**
     * Get iterator for traversing collection members  with key belonging to the specified range. 
     * This iterator fetches all selected objects in memory. 
     * It supports remove() method and all modifications of collection during iteration
     * has no influence on iterator.
     * @param from inclusive low boundary. If <code>null</code> then low boundary is not specified.
     * @param till inclusive high boundary. If <code>null</code> then high boundary is not specified.
     * @return selection iterator
     */
    public IterableIterator<T> iterator(Object from, Object till);

    /**
     * Get iterator for traversing collection members  with key belonging to the specified range. 
     * This iterator fetches all selected objects in memory. 
     * It supports remove() method and all modifications of collection during iteration
     * has no influence on iterator.
     * @param from inclusive low boundary. If <code>null</code> then low boundary is not specified.
     * @param fromInclusive specifies whether from boundary is inclusive or exclusive
     * @param till inclusive high boundary. If <code>null</code> then high boundary is not specified.
     * @param tillInclusive specifies whether till boundary is inclusive or exclusive
     * @return selection iterator
     */
    public IterableIterator<T> iterator(Object from, boolean fromInclusive, Object till, boolean tillInclusive);

    /**
     * Get comparator used in this collection
     * @return collection comparator
     */
    public PersistentComparator<T> getComparator();
}
