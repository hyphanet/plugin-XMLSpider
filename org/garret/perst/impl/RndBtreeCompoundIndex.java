package plugins.XMLSpider.org.garret.perst.impl;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import plugins.XMLSpider.org.garret.perst.IValue;
import plugins.XMLSpider.org.garret.perst.Index;
import plugins.XMLSpider.org.garret.perst.IterableIterator;
import plugins.XMLSpider.org.garret.perst.Key;
import plugins.XMLSpider.org.garret.perst.StorageError;

class RndBtreeCompoundIndex<T> extends RndBtree<T> implements Index<T> { 
    int[]    types;

    RndBtreeCompoundIndex() {}
    
    RndBtreeCompoundIndex(Class[] keyTypes, boolean unique) {
        this.unique = unique;
        type = ClassDescriptor.tpValue;        
        types = new int[keyTypes.length];
        for (int i = 0; i < keyTypes.length; i++) {
            types[i] = getCompoundKeyComponentType(keyTypes[i]);
        }
    }

    static int getCompoundKeyComponentType(Class c) { 
        if (c.equals(Boolean.class)) { 
            return ClassDescriptor.tpBoolean;
        } else if (c.equals(Byte.class)) { 
            return ClassDescriptor.tpByte;
        } else if (c.equals(Character.class)) { 
            return ClassDescriptor.tpChar;
        } else if (c.equals(Short.class)) { 
            return ClassDescriptor.tpShort;
        } else if (c.equals(Integer.class)) { 
            return ClassDescriptor.tpInt;
        } else if (c.equals(Long.class)) { 
            return ClassDescriptor.tpLong;
        } else if (c.equals(Float.class)) { 
            return ClassDescriptor.tpFloat;
        } else if (c.equals(Double.class)) { 
            return ClassDescriptor.tpDouble;
        } else if (c.equals(String.class)) { 
            return ClassDescriptor.tpString;
        } else if (c.equals(Date.class)) { 
            return ClassDescriptor.tpDate;
        } else if (IValue.class.isAssignableFrom(c)) {
            return ClassDescriptor.tpValue;
        } else { 
            return ClassDescriptor.tpObject;
        }
    }

    public Class[] getKeyTypes() {
        Class[] keyTypes = new Class[types.length];
        for (int i = 0; i < keyTypes.length; i++) { 
            keyTypes[i] = mapKeyType(types[i]);
        }
        return keyTypes;
    }

    static class CompoundKey implements Comparable, IValue {
        Object[] keys;

        public int compareTo(Object o) { 
            CompoundKey c = (CompoundKey)o;
            int n = keys.length < c.keys.length ? keys.length : c.keys.length; 
            for (int i = 0; i < n; i++) { 
                int diff = ((Comparable)keys[i]).compareTo(c.keys[i]);
                if (diff != 0) { 
                    return diff;
                }
            }
            return 0;  // allow to compare part of the compound key
        }

        CompoundKey(Object[] keys) { 
            this.keys = keys;
        }
    }
                
    private Key convertKey(Key key) { 
        if (key == null) { 
            return null;
        }
        if (key.type != ClassDescriptor.tpArrayOfObject) { 
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }
        return new Key(new CompoundKey((Object[])key.oval), key.inclusion != 0);
    }
            
    public ArrayList<T> getList(Key from, Key till) {
        return super.getList(convertKey(from), convertKey(till));
    }

    public T get(Key key) {
        return super.get(convertKey(key));
    }

    public T  remove(Key key) { 
        return super.remove(convertKey(key));
    }

    public void remove(Key key, T obj) { 
        super.remove(convertKey(key), obj);
    }

    public T  set(Key key, T obj) { 
        return super.set(convertKey(key), obj);
    }

    public boolean put(Key key, T obj) {
        return super.put(convertKey(key), obj);
    }

    public IterableIterator<T> iterator(Key from, Key till, int order) {
        return super.iterator(convertKey(from), convertKey(till), order);
    }

    public IterableIterator<Map.Entry<Object,T>> entryIterator(Key from, Key till, int order) {
        return super.entryIterator(convertKey(from), convertKey(till), order);
    }
    
    public int indexOf(Key key) { 
        return super.indexOf(convertKey(key));
    }
}

