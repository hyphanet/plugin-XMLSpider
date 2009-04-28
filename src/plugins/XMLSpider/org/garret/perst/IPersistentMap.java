package plugins.XMLSpider.org.garret.perst;

import java.util.*;

/**
 * Interface of persistent map
 */
public interface IPersistentMap<K extends Comparable, V extends IPersistent> extends SortedMap<K,V>, IPersistent, IResource
{
    /**
     * Get entry for the specified key. 
     * This method can be used to obtains both key and value.
     * It is needed when key is persistent object.
     * @param key searched key
     * @return entry associated with this key or null if there is no such key in the map
     */
    Entry<K,V> getEntry(Object key);

    /**
     * Select values of the map using search predicate
     * This iterator doesn't support remove() method.
     * @param cls class of index members
     * @param predicate JSQL condition
     * @return iterator through members of the collection matching search condition
     */
    public Iterator<V> select(Class cls, String predicate);
} 