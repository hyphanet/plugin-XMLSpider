package plugins.XMLSpider.org.garret.perst;

import java.io.*;

/**
 * Interface of custome serializer
 */
public interface CustomSerializer { 
    /**
     * Serialize object
     * @param obj object to be packed
     * @param out output stream to which object should be serialized
     */
    void pack(CustomSerializable obj, OutputStream out) throws IOException;

    /**
     * Deserialize object
     * @param in input stream from which object should be deserialized
     * @return unpacked object
     */
    CustomSerializable unpack(InputStream in) throws IOException;

    /**
     * Create object from its string representation
     * @param str string representation of object (created by toString() method)
     */
    CustomSerializable parse(String str) throws IOException;
}