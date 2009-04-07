package plugins.XMLSpider.org.garret.perst;

/**
 * Interface used by Query to get index for the specified key
 */
public interface IndexProvider 
{ 
    /**
     * Get index for the specified field of the class
     * @param cls class where index is located
     * @param key field of the class
     * @return Index for this field or null if index doesn't exist
     */
    GenericIndex getIndex(Class cls, String key);
}