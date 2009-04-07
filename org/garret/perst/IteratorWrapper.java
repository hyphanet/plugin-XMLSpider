package plugins.XMLSpider.org.garret.perst;

import java.util.Iterator;

public class IteratorWrapper<T> extends IterableIterator<T> 
{ 
    private Iterator<T> iterator;

    public IteratorWrapper(Iterator<T> iterator) { 
        this.iterator = iterator;
    }
    
    public boolean hasNext() { 
        return iterator.hasNext();
    }
    
    public T next() { 
        return iterator.next();
    }

    public void remove() {
        iterator.remove();
    }
}

