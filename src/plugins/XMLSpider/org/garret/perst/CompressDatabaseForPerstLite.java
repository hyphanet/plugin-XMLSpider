package plugins.XMLSpider.org.garret.perst;

import java.io.*;
import java.util.*;
import java.util.zip.*;

/**
 * Utility used to compress database file. You should create database using normal file (OSFile).
 * Then use this utiulity to compress database file. 
 * To work with compressed database file you should path instance if this class in <code>Storage.open</code> method
 */
public class CompressDatabaseForPerstLite {
    static final int PAGE_SIZE = plugins.XMLSpider.org.garret.perst.impl.Page.pageSize;

    public static void compress(OutputStream out, InputStream in, String cipherKey) throws IOException 
    { 
        int rc;
        byte[] buf = new byte[PAGE_SIZE];
        DataInputStream din = new DataInputStream(in);
        DataOutputStream dout = new DataOutputStream(out);

        byte[] initState = new byte[256];
        byte[] state = new byte[256];
        if (cipherKey != null) { 
            byte[] key = cipherKey.getBytes();
            for (int counter = 0; counter < 256; ++counter) { 
                initState[counter] = (byte)counter;
            }
            int index1 = 0;
            int index2 = 0;
            for (int counter = 0; counter < 256; ++counter) {
                index2 = (key[index1] + initState[counter] + index2) & 0xff;
                byte temp = initState[counter];
                initState[counter] = initState[index2];
                initState[index2] = temp;
                index1 = (index1 + 1) % key.length;
            }
        }
        
        while ((rc = in.read(buf, 0, PAGE_SIZE)) >= 0) { 
            if (rc != PAGE_SIZE) { 
                throw new IOException("Database file is corrupted");
            }
            ByteArrayOutputStream bs = new ByteArrayOutputStream();
            GZIPOutputStream gs = new GZIPOutputStream(bs);
            gs.write(buf, 0, PAGE_SIZE);
            gs.finish();
            byte[] compressed = bs.toByteArray();
            int len = compressed.length;
            if (cipherKey != null) { 
                int x = 0, y = 0;
                System.arraycopy(initState, 0, state, 0, state.length);
                for (int i = 0; i < len; i++) {
                    x = (x + 1) & 0xff;
                    y = (y + state[x]) & 0xff;
                    byte temp = state[x];
                    state[x] = state[y];
                    state[y] = temp;
                    int nextState = (state[x] + state[y]) & 0xff;
                    compressed[i] ^= state[nextState];
                }
            }
            dout.writeShort(len);
            dout.write(compressed, 0, compressed.length);
        }
        dout.flush();
    }

    /**
     * This utility accepts one argument: path to database file.
     * It creates new file at the same location and with the same name but with with ".dbz" extension.
     */
    public static void main(String[] args) throws IOException 
    { 
        if (args.length == 0 || args.length > 2) { 
            System.err.println("Usage: java plugins.XMLSpider.org.garret.perst.CompressDatabaseForPerstLite DATABASE_FILE_PATH [cipher-key]");
            return;
        }
        String path = args[0];
        FileInputStream in = new FileInputStream(path);
        int ext = path.lastIndexOf('.');
        String gzip = path.substring(0, ext) + ".dgz";
        String cipherKey = null;
        if (args.length > 1) { 
            cipherKey = args[1];
        }
        FileOutputStream out = new FileOutputStream(gzip);
        compress(out, in, cipherKey);
        in.close();
        out.close();
        System.out.println("File " + gzip + " is written");
    }
}      
                
        