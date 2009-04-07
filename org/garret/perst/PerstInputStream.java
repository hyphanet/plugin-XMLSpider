package plugins.XMLSpider.org.garret.perst;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Input stream for SelfSerializable and CustumSerializer
 */
public abstract class PerstInputStream extends DataInputStream
{
    /**
     * Read refeernce to the object or content of the enbedded object
     * @return unswizzled object
     */     
    public abstract Object readObject() throws IOException;

    /**
     * Read string according to the Perst string encoding
     * @return extracted string or null
     */
    public abstract String readString() throws IOException;

    public PerstInputStream(InputStream stream) { super(stream); }
}
