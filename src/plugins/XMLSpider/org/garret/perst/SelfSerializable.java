package plugins.XMLSpider.org.garret.perst;

/**
 * Interface of the classes sleft responsible for their serialization
 */
public interface SelfSerializable
{
    /**
     * Serialize object
     * @param out writer to be used for object serialization
     */        
    void pack(PerstOutputStream out) throws java.io.IOException;

    /**
     * Deserialize object
     * @param in reader to be used for objet deserialization
     */
    void unpack(PerstInputStream in) throws java.io.IOException;
}