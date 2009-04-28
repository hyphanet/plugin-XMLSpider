package plugins.XMLSpider.org.garret.perst.impl;

import java.util.*;

/**
 * This class store transaction context associated with thread.
 * Content of this class is opaque for application, but it can use 
 * this context to share the single transaction between multiple threads
 */
public class ThreadTransactionContext { 
    int             nested;
    IdentityHashMap locked = new IdentityHashMap();
    ArrayList       modified = new ArrayList();
    ArrayList       deleted = new ArrayList();
}

