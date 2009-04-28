package plugins.XMLSpider.org.garret.perst.impl;
import plugins.XMLSpider.org.garret.perst.IPersistent;

public interface OidHashTable { 
    boolean     remove(int oid);
    void        put(int oid, IPersistent obj);
    IPersistent get(int oid);
    void        flush();
    void        invalidate();
    void        clear();
    int         size();
    void        setDirty(IPersistent obj);
    void        clearDirty(IPersistent obj);
}
