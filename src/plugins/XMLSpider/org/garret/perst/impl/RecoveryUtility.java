package plugins.XMLSpider.org.garret.perst.impl;
import plugins.XMLSpider.org.garret.perst.*;

public class RecoveryUtility
{
    static void printIndex(String prefix, RootPage page)
    {
       System.out.println(prefix + "size=" + page.size);
       System.out.println(prefix + "index=" + page.index);
       System.out.println(prefix + "shadowIndex=" + page.shadowIndex);
       System.out.println(prefix + "usedSize=" + page.usedSize);
       System.out.println(prefix + "indexSize=" + page.indexSize);
       System.out.println(prefix + "shadowIndexSize=" + page.shadowIndexSize);
       System.out.println(prefix + "indexUsed=" + page.indexUsed);
       System.out.println(prefix + "freeList=" + page.freeList);
       System.out.println(prefix + "bitmapEnd=" + page.bitmapEnd);
       System.out.println(prefix + "rootObject=" + page.rootObject);
       System.out.println(prefix + "classDescList=" + page.classDescList);
       System.out.println(prefix + "bitmapExtent=" + page.bitmapExtent);
    }
        
            

    public static void main(String args[]) throws Exception
    {
        if (args.length == 0) { 
            System.err.println("Usage: java RecoveryUtility FILE-PATH [CURRENT]");
            return;
        }
        String filePath = args[0];
        IFile file = filePath.startsWith("@") 
            ? (IFile)new MultiFile(filePath.substring(1), false, false)
            : (IFile)new OSFile(filePath, false, false);      
        byte[] buf = new byte[Header.sizeof];
        int rc = file.read(0, buf);
        if (rc > 0 && rc < Header.sizeof) { 
            System.err.println("Failed to read database header: rc=" + rc);
            return;
        }
        Header header = new Header();
        header.unpack(buf);
        if (header.curr < 0 || header.curr > 1) { 
            System.err.println("Database header was corrupted, header.curr=" + header.curr);
        }
        System.out.println("curr=" + header.curr);
        System.out.println("dirty=" + header.dirty);
        System.out.println("database format version=" + header.databaseFormatVersion);
        System.out.println("transactionId=" + header.transactionId);
        printIndex("index[0].", header.root[0]);
        printIndex("index[1].", header.root[1]);
        if (args.length > 1) { 
            header.curr = Integer.parseInt(args[1]);
            header.dirty = true;
            header.pack(buf);
            file.write(0, buf);
            System.err.println("Set current index to " + args[1]);
        }
        file.close();
    }
}