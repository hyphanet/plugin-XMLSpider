package plugins.XMLSpider.org.garret.perst.impl;

import plugins.XMLSpider.org.garret.perst.*;

public class DefaultPersistentComparator<T extends IPersistent&Comparable> extends PersistentComparator<T> { 
    public int compareMembers(T m1, T m2) {
        return m1.compareTo(m2);
    }
        
    public int compareMemberWithKey(T mbr, Object key) { 
        return mbr.compareTo(key);
    }
}