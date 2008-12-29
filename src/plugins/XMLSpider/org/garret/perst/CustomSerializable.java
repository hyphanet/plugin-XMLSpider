package plugins.XMLSpider.org.garret.perst;

/**
 * Interface used to mark objects serialized using custom serializer
 */
public interface CustomSerializable {
    /**
     * Get string representation of object. This string representation may be used
     * by CustomSerailize.parse method to create new instance of this object
     * @return string representation of object
     */
    public String toString();
}
