package plugins.XMLSpider.org.garret.perst;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

/**
 * Interface of object index.
 * This is base interface for Index and FieldIndex, allowing to write generic algorithms 
 * working with both itype of indices.
 */
public interface GenericIndex<T> extends IPersistent, IResource, ITable<T> { 
    /**
     * Get object by key (exact match)     
     * @param key specified key. It should match with type of the index and should be inclusive.
     * @return object with this value of the key or <code>null</code> if key not found
     * @exception StorageError(StorageError.KEY_NOT_UNIQUE) exception if there are more than 
     * one objects in the index with specified value of the key.
     */
    public T get(Key key);
    
    /**
     * Get objects which key value belongs to the specified range.
     * Either from boundary, either till boundary either both of them can be <code>null</code>.
     * In last case the method returns all objects from the index.
     * @param from low boundary. If <code>null</code> then low boundary is not specified.
     * Low boundary can be inclusive or exclusive. 
     * @param till high boundary. If <code>null</code> then high boundary is not specified.
     * High boundary can be inclusive or exclusive. 
     * @return array of objects which keys belongs to the specified interval, ordered by key value
     */
    public ArrayList<T> getList(Key from, Key till);

     /**
     * Get objects which key value belongs to the specified range.
     * Either from boundary, either till boundary either both of them can be <code>null</code>.
     * In last case the method returns all objects from the index.
     * @param from low boundary. If <code>null</code> then low boundary is not specified.
     * Low boundary can be inclusive or exclusive. 
     * @param till high boundary. If <code>null</code> then high boundary is not specified.
     * High boundary can be inclusive or exclusive. 
     * @return array of objects which keys belongs to the specified interval, ordered by key value
     */
    public Object[] get(Key from, Key till);


    /**
     * Get object by string key (exact match)     
     * @param key packed key 
     * @return object with this value of the key or <code>null</code> if key not[ found
     * @exception StorageError(StorageError.KEY_NOT_UNIQUE) exception if there are more than 
     * one objects in the index with specified value of the key.
     */
    public T get(Object key);
    
    /**
     * Get objects which key value belongs to the specified range.
     * Either from boundary, either till boundary either both of them can be <code>null</code>.
     * In last case the method returns all objects from the index.
     * @param from inclusive low boundary. If <code>null</code> then low boundary is not specified.
     * @param till inclusive high boundary. If <code>null</code> then high boundary is not specified.
     * @return array of objects which keys belongs to the specified interval, ordered by key value
     */
    public Object[] get(Object from, Object till);

    /**
     * Get objects which key value belongs to the specified range.
     * Either from boundary, either till boundary either both of them can be <code>null</code>.
     * In last case the method returns all objects from the index.
     * @param from inclusive low boundary. If <code>null</code> then low boundary is not specified.
     * @param till inclusive high boundary. If <code>null</code> then high boundary is not specified.
     * @return array of objects which keys belongs to the specified interval, ordered by key value
     */
    public ArrayList<T> getList(Object from, Object till);

    /**
     * Get objects with objects with key started with specified prefix,
     * i.e. getPrefix("abc") will return "abc", "abcd", "abcdef", ... but not "ab".     
     * @param prefix string key prefix
     * @return array of objects which key starts with this prefix 
     */
    public Object[] getPrefix(String prefix);
    
    /**
     * Get objects with string key prefix 
     * @param prefix string key prefix
     * @return list of objects which key starts with this prefix 
     */
    public ArrayList<T> getPrefixList(String prefix);
    
    /**
     * Locate all objects which key is prefix of specified word, 
     * i.e. prefixSearch("12345") will return "12", "123", "1234", "12345", but not "123456"
     * @param word string which prefixes are located in index
     * @return array of objects which key is prefix of specified word, ordered by key value
     */
    public Object[] prefixSearch(String word);
    
    /**
     * Locate all objects which key is prefix of specified word.
     * @param word string which prefixes are located in index
     * @return list of objects which key is prefix of specified word, ordered by key value
     */
    public ArrayList<T> prefixSearchList(String word);
    
    /**
     * Get iterator for traversing all objects in the index. 
     * Objects are iterated in the ascent key order. 
     * This iterator supports remove() method. To make it possible to update, remove or add 
     * members to the index during iteration it is necessary to set "perst.concurrent.iterator"
     * property (by default it is not supported because it cause extra overhead during iteration)
     * @return index iterator
     */
    public Iterator<T> iterator();

    /**
     * Get iterator for traversing all entries in the index. 
     * Iterator next() method returns object implementing <code>Map.Entry</code> interface
     * which allows to get entry key and value.
     * Objects are iterated in the ascent key order. 
     * This iterator supports remove() method. To make it possible to update, remove or add 
     * members to the index during iteration it is necessary to set "perst.concurrent.iterator"
     * property (by default it is not supported because it cause extra overhead during iteration)
     * @return index entries iterator
     */
    public IterableIterator<Map.Entry<Object,T>> entryIterator();

    static final int ASCENT_ORDER  = 0;
    static final int DESCENT_ORDER = 1;
    /**
     * Get iterator for traversing objects in the index with key belonging to the specified range. 
     * This iterator supports remove() method. To make it possible to update, remove or add 
     * members to the index during iteration it is necessary to set "perst.concurrent.iterator"
     * property (by default it is not supported because it cause extra overhead during iteration)
     * @param from low boundary. If <code>null</code> then low boundary is not specified.
     * Low boundary can be inclusive or exclusive. 
     * @param till high boundary. If <code>null</code> then high boundary is not specified.
     * High boundary can be inclusive or exclusive. 
     * @param order <code>ASCENT_ORDER</code> or <code>DESCENT_ORDER</code>
     * @return selection iterator
     */
    public IterableIterator<T> iterator(Key from, Key till, int order);

    /**
     * Get iterator for traversing objects in the index with key belonging to the specified range. 
     * This iterator supports remove() method. To make it possible to update, remove or add 
     * members to the index during iteration it is necessary to set "perst.concurrent.iterator"
     * property (by default it is not supported because it cause extra overhead during iteration)
     * @param from inclusive low boundary. If <code>null</code> then low boundary is not specified.
     * Low boundary can be inclusive or exclusive. 
     * @param till inclusive high boundary. If <code>null</code> then high boundary is not specified.
     * @param order <code>ASCENT_ORDER</code> or <code>DESCENT_ORDER</code>
     * @return selection iterator
     */
    public IterableIterator<T> iterator(Object from, Object till, int order);

    /**
     * Get iterator for traversing index entries with key belonging to the specified range. 
     * Iterator next() method returns object implementing <code>Map.Entry</code> interface
     * This iterator supports remove() method. To make it possible to update, remove or add 
     * members to the index during iteration it is necessary to set "perst.concurrent.iterator"
     * property (by default it is not supported because it cause extra overhead during iteration)
     * @param from low boundary. If <code>null</code> then low boundary is not specified.
     * Low boundary can be inclusive or exclusive. 
     * @param till high boundary. If <code>null</code> then high boundary is not specified.
     * High boundary can be inclusive or exclusive. 
     * @param order <code>ASCENT_ORDER</code> or <code>DESCENT_ORDER</code>
     * @return selection iterator
     */
    public IterableIterator<Map.Entry<Object,T>> entryIterator(Key from, Key till, int order);

    /**
     * Get iterator for traversing index entries with key belonging to the specified range. 
     * Iterator next() method returns object implementing <code>Map.Entry</code> interface
     * This iterator supports remove() method. To make it possible to update, remove or add 
     * members to the index during iteration it is necessary to set "perst.concurrent.iterator"
     * property (by default it is not supported because it cause extra overhead during iteration)
     * @param from inclusive low boundary. If <code>null</code> then low boundary is not specified.
     * @param till inclusive high boundary. If <code>null</code> then high boundary is not specified.
     * @param order <code>ASCENT_ORDER</code> or <code>DESCENT_ORDER</code>
     * @return selection iterator
     */
    public IterableIterator<Map.Entry<Object,T>> entryIterator(Object from, Object till, int order);

    /**
     * Get iterator for records which keys started with specified prefix
     * Objects are iterated in the ascent key order. 
     * This iterator supports remove() method. To make it possible to update, remove or add 
     * members to the index during iteration it is necessary to set "perst.concurrent.iterator"
     * property (by default it is not supported because it cause extra overhead during iteration)
     * @param prefix key prefix
     * @return selection iterator
     */
    public IterableIterator<T> prefixIterator(String prefix);


    /**
     * Get type of index key
     * @return type of index key
     */
    public Class getKeyType();

    /**
     * Get types of index compound key components
     * @return array of types of compound key components
     */
    public Class[] getKeyTypes();

    /**
     * Get element at specified position. This methid is efficient only for random access indices
     * @param i position of element in the index<
     * @return object at sepcified position
     * @exception IndexOutOfBoundsException if position is less than 0 or greater or equal than index size
     */
    T getAt(int i);
 
    /**
     * Get position of the first element with specified key. This method is efficient only for random access indices
     * @param key located key
     * @return position of the first element with this key or -1 if no such element is found
     */    
    int indexOf(Key key);

    /**
     * Get entry iterator of objects in the index starting with specified position.
     * This methid is efficient only for random access indices
     * You should not update/remove or add members to the index during iteration
     * @param start Start position in the index. First <code>pos</code> elements will be skipped.
     * @param order <code>ASCENT_ORDER</code> or <code>DESCENT_ORDER</code>
     * @return index entries iterator
     */
    IterableIterator<Map.Entry<Object,T>> entryIterator(int start, int order);

    /**
     * Check if index is unique
     * @return true if index doesn't allow duplicates
     */
    boolean isUnique();    
}
