package plugins.XMLSpider.org.garret.perst;

import java.util.Set;

/**
 * Interface of persistent set. 
 */
public interface IPersistentSet<T> extends IPersistent, IResource, Set<T>, ITable<T> {}

