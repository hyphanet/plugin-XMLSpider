package plugins.XMLSpider.org.garret.perst.impl;
import plugins.XMLSpider.org.garret.perst.*;

import java.io.*;

public class MultiFile implements IFile 
{ 
    public static class MultiFileSegment { 
	IFile  f;
	long   size;
    }

   long seek(long pos) {
	currSeg = 0;
	while (pos >= segment[currSeg].size) { 
	    pos -= segment[currSeg].size;
	    currSeg += 1;
	}
        return pos;
    }


    public void write(long pos, byte[] b) 
    {
        pos = seek(pos);
        segment[currSeg].f.write(pos, b);
    }

    public int read(long pos, byte[] b) 
    { 
        pos = seek(pos);
        return segment[currSeg].f.read(pos, b);
    }
        
    public void sync()
    { 
        if (!noFlush) { 
            for (int i = segment.length; --i >= 0;) { 
                segment[i].f.sync();
            }
        }
    }
    
    public boolean tryLock(boolean shared) 
    { 
        return segment[0].f.tryLock(shared);
    }

    public void lock(boolean shared) 
    {
        segment[0].f.lock(shared);
    }

    public void unlock() 
    { 
        segment[0].f.unlock();
    }


    public void close() 
    { 
        for (int i = segment.length; --i >= 0;) { 
            segment[i].f.close();
        }
    }

    public MultiFile(String[] segmentPath, long[] segmentSize, boolean readOnly, boolean noFlush) { 
        this.noFlush = noFlush;
        segment = new MultiFileSegment[segmentPath.length];
        for (int i = 0; i < segment.length; i++) { 
            MultiFileSegment seg = new MultiFileSegment();
            seg.f = new OSFile(segmentPath[i], readOnly, noFlush);
            seg.size = segmentSize[i];
            fixedSize += seg.size;
            segment[i] = seg;
        }
        fixedSize -= segment[segment.length-1].size;
        segment[segment.length-1].size = Long.MAX_VALUE;
    }

    public MultiFile(MultiFileSegment[] segments) { 
        segment = segments;
        for (int i = 0; i < segments.length; i++) { 
            fixedSize += segments[i].size;
        }
        fixedSize -= segment[segment.length-1].size;
        segment[segment.length-1].size = Long.MAX_VALUE;
    }

    public MultiFile(String filePath, boolean readOnly, boolean noFlush) {
        try { 
            StreamTokenizer in = 
                new StreamTokenizer(new BufferedReader(new FileReader(filePath)));
            File dir = new File(filePath).getParentFile();
            
            this.noFlush = noFlush;
            segment = new MultiFileSegment[0];
            int tkn = in.nextToken();
            do {
                MultiFileSegment seg = new MultiFileSegment();
                if (tkn != StreamTokenizer.TT_WORD && tkn != '"') { 
                    throw new StorageError(StorageError.FILE_ACCESS_ERROR, "Multifile segment name expected");
                }
                String path = in.sval;
                tkn = in.nextToken();
                if (tkn != StreamTokenizer.TT_EOF) { 
                    if (tkn != StreamTokenizer.TT_NUMBER) { 
                        throw new StorageError(StorageError.FILE_ACCESS_ERROR, "Multifile segment size expected");
                    }
                    seg.size = (long)in.nval*1024; // kilobytes
                    tkn = in.nextToken();
                }
                fixedSize += seg.size;
                if (dir != null) { 
                    File f = new File(path);
                    if (!f.isAbsolute()) { 
                        f = new File(dir, path);
                        path = f.getPath();
                    }
                }
                seg.f = new OSFile(path, readOnly, noFlush);
                MultiFileSegment[] newSegment = new MultiFileSegment[segment.length+1];
                System.arraycopy(segment, 0, newSegment, 0, segment.length);
                newSegment[segment.length] = seg;
                segment = newSegment;
            } while (tkn != StreamTokenizer.TT_EOF);
        
            fixedSize -= segment[segment.length-1].size;
            segment[segment.length-1].size = Long.MAX_VALUE;
        } catch (IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR);
        }
    }

    public long length() {
        return fixedSize +  segment[segment.length-1].f.length();
    }

    MultiFileSegment segment[];
    long             fixedSize;
    int              currSeg;
    boolean          noFlush;
}
