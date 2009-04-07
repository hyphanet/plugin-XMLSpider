package plugins.XMLSpider.org.garret.perst.impl;

public interface OidHashTable { 
    boolean     remove(int oid);
    void        put(int oid, Object obj);
    Object      get(int oid);
    void        flush();
    void        invalidate();
    void        clear();
    int         size();
    void        setDirty(Object obj);
    void        clearDirty(Object obj);
}
