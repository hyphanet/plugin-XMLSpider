package plugins.XMLSpider.org.garret.perst;

/**
  * This interface allows to associate class loader with the particular persistent class.
  * When persistent object is stored in Perst storage, Perst checks if class descriptor for 
  * this class is already present in the storage. If it is not present, then Perst create class 
  * descriptor and stores it in database. Perst inspects class loader of the persistent class
  * and if it implements INamedClassLoader interface, then class name is prefixed by
  * name of class loader obtained by INamedClassLoader.getName() method. Class loader
  * name is separated from class name by ':'. When persistent class is loaded (during database open time)
  * Perst tries to locate class loader with the specified name among named class loaders previously
  * registered using Storage.registerClassLoader method. If no registered class loader with such name
  * is found then class is ignored. Any attempt to access object of this class will cause an error.
  */
public interface INamedClassLoader 
{  
    /**
     * Get name of this class loader.
     * @return class loader name (any sequence of character except ':')
     */
    public String getName();
}