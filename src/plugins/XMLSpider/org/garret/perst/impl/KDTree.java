package plugins.XMLSpider.org.garret.perst.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Stack;

import plugins.XMLSpider.org.garret.perst.IterableIterator;
import plugins.XMLSpider.org.garret.perst.MultidimensionalComparator;
import plugins.XMLSpider.org.garret.perst.MultidimensionalIndex;
import plugins.XMLSpider.org.garret.perst.Persistent;
import plugins.XMLSpider.org.garret.perst.PersistentCollection;
import plugins.XMLSpider.org.garret.perst.PersistentIterator;
import plugins.XMLSpider.org.garret.perst.Storage;

public class KDTree<T> extends PersistentCollection<T> implements MultidimensionalIndex<T>{
    KDTreeNode root;
    int        nMembers;
    int        height;
    MultidimensionalComparator<T> comparator;

    private KDTree() {} 

    KDTree(Storage storage, MultidimensionalComparator<T> comparator) { 
        super(storage);
        this.comparator = comparator;
    }

    KDTree(Storage storage, Class cls, String[] fieldNames, boolean treateZeroAsUndefinedValue) { 
        super(storage);
        this.comparator = new ReflectionMultidimensionalComparator<T>(storage, cls, fieldNames, treateZeroAsUndefinedValue);
    }

    public MultidimensionalComparator<T> getComparator() { 
        return comparator;
    }

    static final int OK = 0;
    static final int NOT_FOUND = 1;
    static final int TRUNCATE  = 2;
    

    static class KDTreeNode<T> extends Persistent 
    {
        KDTreeNode  left;
        KDTreeNode  right;
        T           obj;
        boolean     deleted;
        
        KDTreeNode(Storage db, T obj) {
            super(db);
            this.obj = obj;
        }        

        private KDTreeNode() {}

        public void load() {
            super.load();
            getStorage().load(obj);
        }

        public boolean recursiveLoading() {
            return false;
        }

        int insert(T ins, MultidimensionalComparator<T> comparator, int level) 
        { 
            load();
            int diff = comparator.compare(ins, obj, level % comparator.getNumberOfDimensions());
            if (diff == MultidimensionalComparator.EQ && deleted) { 
                getStorage().deallocate(obj);
                modify();
                obj = ins;
                deleted = false;
                return level;
            } else if (diff != MultidimensionalComparator.GT) { 
                if (left == null) { 
                    modify();
                    left = new KDTreeNode<T>(getStorage(), ins);
                    return level+1;
                } else { 
                    return left.insert(ins, comparator, level + 1);
                }
            } else { 
                if (right == null) { 
                    modify();
                    right = new KDTreeNode<T>(getStorage(), ins);
                    return level+1;
                } else { 
                    return right.insert(ins, comparator, level + 1);
                }
            }
        }
        
        int remove(T rem, MultidimensionalComparator<T> comparator, int level) 
        { 
            load();
            if (obj == rem) { 
                if (left == null && right == null) { 
                    deallocate();
                    return TRUNCATE;
                } else {
                    modify();
                    obj = comparator.cloneField(obj, level % comparator.getNumberOfDimensions());
                    deleted = true;
                    return OK;
                }  
            }
            int diff = comparator.compare(rem, obj, level % comparator.getNumberOfDimensions());
            if (diff != MultidimensionalComparator.GT && left != null) {
                int result = left.remove(rem, comparator, level + 1);
                if (result == TRUNCATE) { 
                    modify();
                    left = null;
                    return OK;
                } else if (result == OK) { 
                    return OK;
                }
            } 
            if (diff != MultidimensionalComparator.LT && right != null) { 
                int result = right.remove(rem, comparator, level + 1);
                if (result == TRUNCATE) { 
                    modify();
                    right = null;
                    return OK;
                } else if (result == OK) { 
                    return OK;
                }
            }
            return NOT_FOUND;
        }
                
        public void deallocate() { 
            load();
            if (deleted) { 
                getStorage().deallocate(obj);
            }
            if (left != null) { 
                left.deallocate();
            }
            if (right != null) { 
                right.deallocate();
            }
            super.deallocate();
        }
    }

    public void optimize() { 
        Iterator<T> itr = iterator();
        int n = nMembers;
        Object[] members = new Object[n];
        for (int i = 0; i < n; i++) { 
            members[i] = itr.next();
        }
        Random rnd = new Random();
        for (int i = 0; i < n; i++) { 
            int j = rnd.nextInt(n);
            Object tmp = members[j];
            members[j] = members[i];
            members[i] = tmp;
        }
        clear();
        for (int i = 0; i < n; i++) { 
            add((T)members[i]);
        }
    }           

    public boolean add(T obj) 
    { 
        modify();
        if (root == null) {
            root = new KDTreeNode<T>(getStorage(), obj);
            height = 1;
        } else {  
            int level = root.insert(obj, comparator, 0);
            if (level >= height) { 
                height = level+1;
            }
        }
        nMembers += 1;
        return true;
    }

    public boolean remove(Object obj) 
    {
        if (root == null) { 
            return false;
        }
        int result = root.remove(obj, comparator, 0);
        if (result == NOT_FOUND) { 
            return false;
        } 
        modify();
        if (result == TRUNCATE) { 
            root = null;
        }
        nMembers -= 1;
        return true;
    }

    public Iterator<T> iterator() { 
        return iterator(null, null);
    }

    public IterableIterator<T> iterator(T pattern) { 
        return iterator(pattern, pattern);
    }

    public IterableIterator<T> iterator(T low, T high) { 
        return new KDTreeIterator(low, high);
    }

    public ArrayList<T> queryByExample(T pattern) { 
        return queryByExample(pattern, pattern);
    }

    public ArrayList<T> queryByExample(T low, T high) { 
        Iterator<T> i = iterator(low, high);
        ArrayList<T> list = new ArrayList<T>();
        while (i.hasNext()) { 
            list.add(i.next());
        }
        return list;
    }

    public Object[] toArray() {
        return  queryByExample(null, null).toArray();
    }

    public <E> E[] toArray(E[] arr) {
        return queryByExample(null, null).toArray(arr);
    }

    public int size() { 
        return nMembers;
    }

    public int getHeight() { 
        return height;
    }

    public void clear() {
        if (root != null) { 
            root.deallocate();
            modify();
            root = null;
            nMembers = 0;
            height = 0;
        }
    }

    public boolean contains(Object member) {
        Iterator<T> i = iterator((T)member);
        while (i.hasNext()) { 
            if (i.next() == member) { 
                return true;
            }
        }
        return false;
    } 

    public void deallocate() {
        if (root != null) { 
            root.deallocate();
        }
        super.deallocate();
    }

    int compareAllComponents(T pattern, T obj) 
    { 
        int n = comparator.getNumberOfDimensions();
        int result = MultidimensionalComparator.EQ;
        for (int i = 0; i < n; i++) { 
            int diff = comparator.compare(pattern, obj, i);
            if (diff == MultidimensionalComparator.RIGHT_UNDEFINED) { 
                return diff;
            } else if (diff == MultidimensionalComparator.LT) { 
                if (result == MultidimensionalComparator.GT) { 
                    return MultidimensionalComparator.NE;
                } else { 
                    result = MultidimensionalComparator.LT;
                }
            } else if (diff == MultidimensionalComparator.GT) { 
                if (result == MultidimensionalComparator.LT) { 
                    return MultidimensionalComparator.NE;
                } else { 
                    result = MultidimensionalComparator.GT;
                }
            }
        }
        return result;
    }
                

    public class KDTreeIterator extends IterableIterator<T> implements PersistentIterator
    { 
        Stack<KDTreeNode<T>> stack;
        int                  nDims;
        T                    high;
        T                    low;
        KDTreeNode<T>        curr;
        KDTreeNode<T>        next;
        int                  currLevel;
 
        KDTreeIterator(T low, T high) { 
            this.low = low;
            this.high = high;
            nDims = comparator.getNumberOfDimensions();
            stack = new Stack<KDTreeNode<T>>();
            getMin(root);
        }
        
        public int getLevel() {
            return currLevel;
        }

        private boolean getMin(KDTreeNode<T> node) { 
            if (node != null) { 
                while (true) { 
                    node.load();
                    stack.push(node);
                    int diff = low == null 
                        ? MultidimensionalComparator.LEFT_UNDEFINED 
                        : comparator.compare(low, node.obj, (stack.size()-1) % nDims);
                    if (diff != MultidimensionalComparator.GT && node.left != null) { 
                        node = node.left;
                    } else { 
                        return true;
                    }
                }
            }                         
            return false;
        }

        public boolean hasNext() {
            if (next != null) { 
                return true;
            }
            while (!stack.empty()) { 
                KDTreeNode<T> node = stack.pop();                    
                if (node != null) { 
                    if (!node.deleted) { 
                        int result;
                        if ((low == null 
                             || (result = compareAllComponents(low, node.obj)) == MultidimensionalComparator.LT 
                             || result == MultidimensionalComparator.EQ)
                            && (high == null 
                                || (result = compareAllComponents(high, node.obj)) == MultidimensionalComparator.GT 
                                || result == MultidimensionalComparator.EQ))
                        {
                            next = node;
                            currLevel = stack.size();
                        }
                    }
                    if (node.right != null 
                        && (high == null 
                            || comparator.compare(high, node.obj, stack.size() % nDims) != MultidimensionalComparator.LT)) 
                    { 
                        stack.push(null);
                        if (!getMin(node.right)) { 
                            stack.pop();
                        }
                    }
                    if (next != null) { 
                        return true;
                    }
                }
            }
            return false;
        }                                

        public T next() { 
            if (!hasNext()) { 
                throw new NoSuchElementException();
            }
            curr = next;
            next = null;
            return curr.obj;
        }
        
        public int nextOid() { 
            if (!hasNext()) { 
                return 0;
            }
            curr = next;
            next = null;
            return getStorage().getOid(curr.obj);
        }
        
        public void remove() { 
            if (curr == null) { 
                throw new IllegalStateException();
            }
            curr.modify();
            curr.obj = comparator.cloneField(curr.obj, currLevel % nDims);
            curr.deleted = true;
            curr = null;
        }
    }
}

