package plugins.XMLSpider.org.garret.perst;

import java.util.Iterator;
import java.util.Map;
import java.lang.reflect.Field;

/**
 * Interface of indexed field.
 * Index is used to provide fast access to the object by the value of indexed field. 
 * Objects in the index are stored ordered by the value of indexed field. 
 * It is possible to select object using exact value of the key or 
 * select set of objects which key belongs to the specified interval 
 * (each boundary can be specified or unspecified and can be inclusive or exclusive)
 * Key should be of scalar, String, java.util.Date or peristent object type.
 */
public interface FieldIndex<T extends IPersistent> extends GenericIndex<T> 
{ 
    /**
     * Put new object in the index. 
     * @param obj object to be inserted in index. Object should contain indexed field. 
     * Object can be not yet peristent, in this case
     * its forced to become persistent by assigning OID to it.
     * @return <code>true</code> if object is successfully inserted in the index, 
     * <code>false</code> if index was declared as unique and there is already object with such value
     * of the key in the index. 
     */
    public boolean put(T obj);

    /**
     * Associate new object with the key specified by object field value. 
     * If there is already object with such key in the index, 
     * then it will be removed from the index and new value associated with this key.
     * @param obj object to be inserted in index. Object should contain indexed field. 
     * Object can be not yet peristent, in this case
     * its forced to become persistent by assigning OID to it.
     * @return object previously associated with this key, <code>null</code> if there was no such object
     */
    public T set(T obj);

    /**
     * Assign to the integer indexed field unique autoicremented value and 
     * insert object in the index. 
     * @param obj object to be inserted in index. Object should contain indexed field
     * of integer (<code>int</code> or <code>long</code>) type.
     * This field is assigned unique value (which will not be reused while 
     * this index exists) and object is marked as modified.
     * Object can be not yet peristent, in this case
     * its forced to become persistent by assigning OID to it.
     * @exception StorageError(StorageError.INCOMPATIBLE_KEY_TYPE) when indexed field
     * has type other than <code>int</code> or <code>long</code>
     */
    public void append(T obj);

    /**
     * Remove object from the index
     * @param obj object removed from the index. Object should contain indexed field. 
     * @exception StorageError(StorageError.KEY_NOT_FOUND) exception if there is no such key in the index
     */
    public void remove(T obj);

    /**
     * Remove object with specified key from the unique index
     * @param key value of removed key
     * @return removed object
     * @exception StorageError(StorageError.KEY_NOT_FOUND) exception if there is no such key in the index,
     * or StorageError(StorageError.KEY_NOT_UNIQUE) if index is not unique.
     */
    public T remove(Key key);

    /**
     * Remove object with specified key from the unique index
     * @param key value of removed key
     * @return removed object
     * @exception StorageError(StorageError.KEY_NOT_FOUND) exception if there is no such key in the index,
     * or StorageError(StorageError.KEY_NOT_UNIQUE) if index is not unique.
     */
    public T removeKey(Object key);

     /**
     * Check if index contains specified object instance.
     * @param obj object to be searched in the index. Object should contain indexed field. 
     * @return <code>true</code> if object is present in the index, <code>false</code> otherwise
     */
    public boolean containsObject(T obj);

     /**
     * Check if index contains object which is equal to the specified object.
     * More formally, returns <tt>true</tt> if and only if this
     * collection contains at least one element <tt>e</tt> such that
     * <tt>(obj==null ? e==null : obj.equals(e))</tt>.<p>
     * @param obj object to be searched in the index. Object should contain indexed field. 
     * @return <code>true</code> if collection contains object equals to the specified
     */
    public boolean contains(T obj);

    /**
     * Locate objects with the same value of the key as specified object
     * @param obj object specifying search key value
     * @return selection iterator
     */
    public IterableIterator<T> queryByExample(T obj);

    /**
     * Get class obejct objects which can be inserted in this index
     * @return class specified in Storage.createFielIndex method
     */
    public Class getIndexedClass();

    /**
     * Get fields used as a key
     * @return array of index key fields
     */
    public Field[] getKeyFields();

    /**
     * Select members of the collection using search predicate
     * This iterator doesn't support remove() method.
     * To make it possible to update, remove or add members to the index 
     * during iteration it is necessary to set "perst.concurrent.iterator"
     * property (by default it is not supported because it cause extra overhead during iteration)
     * @param predicate JSQL condition
     * @return iterator through members of the collection matching search condition
     */
    public IterableIterator<T> select(String predicate);

    /**
     * Check if field index is case insensitive
     * @return true if index ignore case of string keys
     */
    boolean isCaseInsensitive();    
}

