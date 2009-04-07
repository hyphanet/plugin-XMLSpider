package plugins.XMLSpider.org.garret.perst;

import java.io.IOException;

/**
 * Interface of custome serializer
 */
public interface CustomSerializer { 
    /**
     * Serialize object
     * @param obj object to be packed
     * @param out output stream to which object should be serialized
     */
    void pack(Object obj, PerstOutputStream out) throws IOException;

    /**
     * Deserialize object
     * @param in input stream from which object should be deserialized
     * @return created and unpacked object
     */
    Object unpack(PerstInputStream in) throws IOException;

    /**
     * Create instance of specified class
     * @param cls created object class
     */
    Object create(Class cls);

    /**
     * Deserialize object
     * @param obj unpacked object 
     * @param in input stream from which object should be deserialized
     */
    void unpack(Object obj, PerstInputStream in) throws IOException;

    /**
     * Create object from its string representation
     * @param str string representation of object (created by toString() method)
     */
    Object parse(String str) throws Exception;

    /**
     * Get string representation of the object
     * @param str object which string representation is taken
     */
    String print(Object str);

    /**
     * Check if serializer can pack  objects of this class
     * @param cls inspected object class
     * @return true if serializer can pack instances of this class
     */
    boolean isApplicable(Class cls);
  
    /**
     * Check if serializer can pack this object component
     * @param obj bject component to be packed
     * @return true if serializer can pack this object inside some other object
     */
    boolean isEmbedded(Object obj);
}