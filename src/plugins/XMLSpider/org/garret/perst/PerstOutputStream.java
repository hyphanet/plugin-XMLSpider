package plugins.XMLSpider.org.garret.perst;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Output stream for SelfSerializable and CustumSerializer
 */
public abstract class PerstOutputStream extends DataOutputStream
{
    /**
     * Write reference to the object or content of embedded object
     * @param obj swizzled object
     */
    public abstract void writeObject(Object obj) throws IOException;

    /**
     * Write string according to the Perst string encoding
     * @param str string to be packed (myay be null)
     */
    public abstract void writeString(String str) throws IOException;
    
    public PerstOutputStream(OutputStream stream) { super(stream); }
}
