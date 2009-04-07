package plugins.XMLSpider.org.garret.perst.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import plugins.XMLSpider.org.garret.perst.PerstOutputStream;
import plugins.XMLSpider.org.garret.perst.StorageError;

public class ByteBuffer {
    public final void extend(int size) {  
        if (size > arr.length) { 
            int newLen = size > arr.length*2 ? size : arr.length*2;
            byte[] newArr = new byte[newLen];
            System.arraycopy(arr, 0, newArr, 0, used); 
            arr = newArr;
        }
        used = size;
    }
    
    final byte[] toArray() { 
        byte[] result = new byte[used];
        System.arraycopy(arr, 0, result, 0, used); 
        return result;
    }

    int packI4(int dst, int value) { 
        extend(dst + 4);
        Bytes.pack4(arr, dst, value);
        return dst + 4;
    }

    int packString(int dst, String value) { 
        if (value == null) { 
            extend(dst + 4);
            Bytes.pack4(arr, dst, -1);
            dst += 4;        
        } else { 
            int length = value.length();
            if (encoding == null) { 
                extend(dst + 4 + 2*length);
                Bytes.pack4(arr, dst, length);
                dst += 4;
                for (int i = 0; i < length; i++) { 
                    Bytes.pack2(arr, dst, (short)value.charAt(i));
                    dst += 2;
                }
            } else { 
                try { 
                    byte[] bytes = value.getBytes(encoding);
                    extend(dst + 4 + bytes.length);
                    Bytes.pack4(arr, dst, -2-bytes.length);
                    System.arraycopy(bytes, 0, arr, dst+4, bytes.length);
                    dst += 4 + bytes.length;
                } catch (UnsupportedEncodingException x) { 
                    throw new StorageError(StorageError.UNSUPPORTED_ENCODING);
                }
            }        
        }
        return dst;
    }

    class ByteBufferOutputStream extends OutputStream { 
        public void write(int b) {
            write(new byte[]{(byte)b}, 0, 1);
        }

        public void write(byte b[], int off, int len) {
            int pos = used;
            extend(pos + len);
            System.arraycopy(b, off, arr, pos, len);
        }
    }

    class ByteBufferObjectOutputStream extends PerstOutputStream { 
        ByteBufferObjectOutputStream() { 
            super(new ByteBufferOutputStream());
        }

        public void writeObject(Object obj) throws IOException {                
            try { 
                flush();
                db.swizzle(ByteBuffer.this, used, obj);
            } catch(Exception x) { 
                throw new StorageError(StorageError.ACCESS_VIOLATION, x);
            } 
        }
        
        public void writeString(String str) throws IOException {      
            flush();
            packString(used, str);
        }
    }

    public PerstOutputStream getOutputStream() { 
        return new ByteBufferObjectOutputStream();
    }

    public int size() { 
        return used;
    }

    ByteBuffer(StorageImpl db, Object parent, boolean finalized) { 
        this();
        this.db = db;
        encoding = db.encoding;
        this.parent = parent;
        this.finalized = finalized;
    }

    ByteBuffer() {
        arr = new byte[64];
    }

    public byte[]      arr;
    public int         used;
    public String      encoding;
    public Object      parent;
    public boolean     finalized; 
    public StorageImpl db;
}




