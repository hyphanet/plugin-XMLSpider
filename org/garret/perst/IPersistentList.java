package plugins.XMLSpider.org.garret.perst;

import java.util.List;

/**
 * Interface for ordered collection (sequence). 
 * The user can access elements by their integer index (position in
 * the list), and search for elements in the list.<p>
 */
public interface IPersistentList<E> extends IPersistent, List<E> {
}

