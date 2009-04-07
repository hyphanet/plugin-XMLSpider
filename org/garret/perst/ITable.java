package plugins.XMLSpider.org.garret.perst;

import java.util.Collection;

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

    /**
     * Remove all objects from the index and deallocate them.
     * This method is equivalent to th following peace of code:
     * { 
     *     Iterator i = index.iterator();
     *     while (i.hasNext()) ((IPersistent)i.next()).deallocate();
     *     index.clear();
     * }
     * Please notice that this method doesn't check if there are some other references to the deallocated objects.
     * If deallocated object is included in some other index or is referenced from some other objects, then after deallocation
     * there will be dangling references and dereferencing them can cause unpredictable behavior of the program.
     */
    public void          deallocateMembers();
}