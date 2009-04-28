package plugins.XMLSpider.org.garret.perst;

import java.util.*;

/**
 * Interface of selectable collection.
 * Selectable collections allows to selct its memebers using JSQL query
 */
public interface ITable<T> extends Collection<T> { 
    /**
     * Select members of the collection using search predicate
     * This iterator doesn't support remove() method.
     * @param cls class of index members
     * @param predicate JSQL condition
     * @return iterator through members of the collection matching search condition
     */
    public IterableIterator<T> select(Class cls, String predicate);
}