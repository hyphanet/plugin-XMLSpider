package plugins.XMLSpider.org.garret.perst;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Utility used to compress database file. You should create database using normal file (OSFile).
 * Then use this utiulity to compress database file. 
 * To work with compressed database file you should path instance if this class in <code>Storage.open</code> method
 */
public class CompressDatabase {
    /**
     * This utility accepts one argument: path to database file.
     * It creates new file at the same location and with the same name but with with ".dbz" extension.
     */
    public static void main(String[] args) throws IOException { 
        if (args.length == 0 || args.length > 2) { 
            System.err.println("Usage: java org.garret.perst.CompressDatabase DATABASE_FILE_PATH [COMPRESSION-LEVEL]");
            return;
        }
        String path = args[0];
        FileInputStream in = new FileInputStream(path);
        int ext = path.lastIndexOf('.');
        String zip = path.substring(0, ext) + ".dbz";
        FileOutputStream out = new FileOutputStream(zip);
        byte[] segment = new byte[CompressedFile.SEGMENT_LENGTH];
        ZipOutputStream zout = new ZipOutputStream(out);
        if (args.length == 2) { 
            zout.setLevel(Integer.parseInt(args[1]));
        }
        long pos = 0;
        int rc = -1;
        do { 
            int offs = 0;
            while (offs < segment.length && (rc = in.read(segment, offs, segment.length - offs)) >= 0) { 
                offs += rc;
            }
            if (offs > 0) { 
                String name = "000000000000" + pos;                
                ZipEntry entry = new ZipEntry(name.substring(name.length()-12));
                entry.setSize(offs);
                zout.putNextEntry(entry);
                zout.write(segment, 0, offs);
                zout.closeEntry();
                pos += offs;
            }
        } while (rc >= 0);

        zout.finish();
        zout.close();
        System.out.println("File " + zip + " is written");
    }
}      
                
        