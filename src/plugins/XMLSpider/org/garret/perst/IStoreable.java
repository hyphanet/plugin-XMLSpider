package plugins.XMLSpider.org.garret.perst;

/**
 * Interface for classes which need onStore callback to be invoked
 * when Perst storess object to the storage
 */
public interface IStoreable 
{
   /**
     * Method called by the database before storing of the object.
     * It can be used to save or close transient fields of the object. 
     * Default implementation of this method do nothing
     */
    public void onStore();
}
