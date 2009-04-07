package plugins.XMLSpider.org.garret.perst.impl;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;

import plugins.XMLSpider.org.garret.perst.IPersistent;
import plugins.XMLSpider.org.garret.perst.IPersistentMap;
import plugins.XMLSpider.org.garret.perst.IValue;
import plugins.XMLSpider.org.garret.perst.Index;
import plugins.XMLSpider.org.garret.perst.Key;
import plugins.XMLSpider.org.garret.perst.Link;
import plugins.XMLSpider.org.garret.perst.Persistent;
import plugins.XMLSpider.org.garret.perst.PersistentComparator;
import plugins.XMLSpider.org.garret.perst.PersistentResource;
import plugins.XMLSpider.org.garret.perst.Query;
import plugins.XMLSpider.org.garret.perst.SortedCollection;
import plugins.XMLSpider.org.garret.perst.Storage;
import plugins.XMLSpider.org.garret.perst.StorageError;

class PersistentMapImpl<K extends Comparable, V> extends PersistentResource implements IPersistentMap<K, V>
{
    IPersistent index;
    Object      keys;
    Link<V>     values;
    int         type;

    transient volatile Set<Entry<K,V>> entrySet;
    transient volatile Set<K>          keySet;
    transient volatile Collection<V>   valuesCol;

    static final int BTREE_TRESHOLD = 128;

    PersistentMapImpl(Storage storage, Class keyType, int initialSize) { 
        super(storage);
        type = getTypeCode(keyType);
        keys = new Comparable[initialSize];
        values = storage.<V>createLink(initialSize);
    }

   static class PersistentMapEntry<K extends Comparable,V> extends Persistent implements Entry<K,V> { 
        K key;
        V value;

        public K getKey() { 
            return key;
        }

        public V getValue() { 
            return value;
        }

        public V setValue(V value) { 
            modify();
            V prevValue = this.value;
            this.value = value;
            return prevValue;
        }

        PersistentMapEntry(K key, V value) { 
            this.key = key;
            this.value = value;
        }
        PersistentMapEntry() {}
    }

    static class PersistentMapComparator<K extends Comparable, V> extends PersistentComparator<PersistentMapEntry<K,V>> { 
        public int compareMembers(PersistentMapEntry<K,V> m1, PersistentMapEntry<K,V> m2) {
            return m1.key.compareTo(m2.key);
        }

        public int compareMemberWithKey(PersistentMapEntry<K,V> mbr, Object key) {
            return mbr.key.compareTo(key);
        }
    }

    PersistentMapImpl() {}

    protected int getTypeCode(Class c) { 
        if (c.equals(byte.class) || c.equals(Byte.class)) { 
            return ClassDescriptor.tpByte;
        } else if (c.equals(short.class) || c.equals(Short.class)) {
            return ClassDescriptor.tpShort;
        } else if (c.equals(char.class) || c.equals(Character.class)) {
            return ClassDescriptor.tpChar;
        } else if (c.equals(int.class) || c.equals(Integer.class)) {
            return ClassDescriptor.tpInt;
        } else if (c.equals(long.class) || c.equals(Long.class)) {
            return ClassDescriptor.tpLong;
        } else if (c.equals(float.class) || c.equals(Float.class)) {
            return ClassDescriptor.tpFloat;
        } else if (c.equals(double.class) || c.equals(Double.class)) {
            return ClassDescriptor.tpDouble;
        } else if (c.equals(String.class)) {
            return ClassDescriptor.tpString;
        } else if (c.equals(boolean.class) || c.equals(Boolean.class)) {
            return ClassDescriptor.tpBoolean;
        } else if (c.isEnum()) {
            return ClassDescriptor.tpEnum;
        } else if (c.equals(java.util.Date.class)) {
            return ClassDescriptor.tpDate;
        } else if (IValue.class.isAssignableFrom(c)) {
            return ClassDescriptor.tpValue;
        } else { 
            return ClassDescriptor.tpObject;
        }
    }

    public int size() {
        return index != null ? ((Collection)index).size() : values.size();
    }

    public boolean isEmpty() {
	return size() == 0;
    }

    public boolean containsValue(Object value) {
	Iterator<Entry<K,V>> i = entrySet().iterator();
	if (value==null) {
	    while (i.hasNext()) {
		Entry<K,V> e = i.next();
		if (e.getValue()==null)
		    return true;
	    }
	} else {
	    while (i.hasNext()) {
		Entry<K,V> e = i.next();
		if (value.equals(e.getValue()))
		    return true;
	    }
	}
	return false;
    }

    private int binarySearch(Object key) {
        Comparable[] keys = (Comparable[])this.keys;
        int l = 0, r = values.size();
        while (l < r) {
            int i = (l + r) >> 1;
            if (keys[i].compareTo(key) < 0) { 
                l = i+1;
            } else { 
                r = i;
            }
        }
        return r;
    }

    public boolean containsKey(Object key) {
        if (index != null) { 
            if (type == ClassDescriptor.tpValue) { 
                return ((SortedCollection)index).containsKey(key);
            } else { 
                Key k = generateKey(key);
                return ((Index)index).entryIterator(k, k, Index.ASCENT_ORDER).hasNext();
            } 
        } else {
            int i = binarySearch(key);
            return i < values.size() && ((Comparable[])keys)[i].equals(key);
        }
    }

    public V get(Object key) {
        if (index != null) { 
            if (type == ClassDescriptor.tpValue) { 
                PersistentMapEntry<K,V> entry = ((SortedCollection<PersistentMapEntry<K,V>>)index).get(key);
                return (entry != null) ? entry.value : null;
            } else { 
                return ((Index<V>)index).get(generateKey(key));
            }
        } else {
            int i = binarySearch(key);
            if (i < values.size() && ((Comparable[])keys)[i].equals(key)) {
                return values.get(i);
            }
            return null;
        }
    }

    public Entry<K,V> getEntry(Object key) {
        if (index != null) { 
            if (type == ClassDescriptor.tpValue) { 
                return ((SortedCollection<PersistentMapEntry<K,V>>)index).get(key);
            } else { 
                V value = ((Index<V>)index).get(generateKey(key));
                return value != null ? new PersistentMapEntry((K)key, value) : null;
            }
        } else {
            int i = binarySearch(key);
            if (i < values.size() && ((Comparable[])keys)[i].equals(key)) {
                V value = values.get(i);
                return value != null ? new PersistentMapEntry((K)key, value) : null;                
            }
            return null;
        }
    }
    
    public V put(K key, V value) {
        V prev = null;
        if (index == null) { 
            int size = values.size();
            int i = binarySearch(key);
            if (i < size && key.equals(((Comparable[])keys)[i])) {
                prev = values.set(i, value);
            } else {
                if (size == BTREE_TRESHOLD) { 
                    Comparable[] keys = (Comparable[])this.keys;
                    if (type == ClassDescriptor.tpValue) { 
                        SortedCollection<PersistentMapEntry<K,V>> col 
                            = getStorage().<PersistentMapEntry<K,V>>createSortedCollection(new PersistentMapComparator<K,V>(), true);
                        index = col;
                        for (i = 0; i < size; i++) { 
                            col.add(new PersistentMapEntry((K)keys[i], values.get(i)));
                        }                
                        prev = insertInSortedCollection(key, value);
                    } else { 
                        Index<V> idx = getStorage().<V>createIndex(Btree.mapKeyType(type), true);
                        index = idx;
                        for (i = 0; i < size; i++) { 
                            idx.set(generateKey(keys[i]), values.get(i));
                        }                
                        prev = idx.set(generateKey(key), value);
                    }
                    this.keys = null;
                    this.values = null;                
                    modify();
                } else {
                    Object[] oldKeys = (Object[])keys;
                    if (size >= oldKeys.length) { 
                        Comparable[] newKeys = new Comparable[size+1 > oldKeys.length*2 ? size+1 : oldKeys.length*2];
                        System.arraycopy(oldKeys, 0, newKeys, 0, i);                
                        System.arraycopy(oldKeys, i, newKeys, i+1, size-i);
                        keys = newKeys;
                        newKeys[i] = key;
                    } else {
                        System.arraycopy(oldKeys, i, oldKeys, i+1, size-i);
                        oldKeys[i] = key;
                    }
                    values.insert(i, value);
                }
            }
        } else { 
            if (type == ClassDescriptor.tpValue) {               
                prev = insertInSortedCollection(key, value);
            } else { 
                prev = ((Index<V>)index).set(generateKey(key), value);
            }
        }
        return prev;
    }

    private V insertInSortedCollection(K key, V value) {
        SortedCollection<PersistentMapEntry<K,V>> col = (SortedCollection<PersistentMapEntry<K,V>>)index;
        PersistentMapEntry<K,V> entry = col.get(key);
        V prev = null;
        getStorage().makePersistent(value);
        if (entry == null) { 
            col.add(new PersistentMapEntry(key, value));
        } else {
            prev = entry.setValue(value);
        }
        return prev;
    }

    public V remove(Object key) {
        if (index == null) { 
            int size = values.size();
            int i = binarySearch(key);
            if (i < size && ((Comparable[])keys)[i].equals(key)) {
                System.arraycopy(keys, i+1, keys, i, size-i-1);
                ((Comparable[])keys)[size-1] = null;
                return values.remove(i);
            }
            return null;
        } else {
            if (type == ClassDescriptor.tpValue) {               
                SortedCollection<PersistentMapEntry<K,V>> col = (SortedCollection<PersistentMapEntry<K,V>>)index;
                PersistentMapEntry<K,V> entry = col.get(key);
                if (entry == null) { 
                    return null;
                }
                col.remove(entry);
                return entry.value;
            } else { 
                try { 
                    return ((Index<V>)index).remove(generateKey(key));
                } catch (StorageError x) { 
                    if (x.getErrorCode() == StorageError.KEY_NOT_FOUND) { 
                        return null;
                    }
                    throw x;
                }
            }
        }
    }

    public void putAll(Map<? extends K, ? extends V> t) {
	Iterator<? extends Entry<? extends K, ? extends V>> i = t.entrySet().iterator();
	while (i.hasNext()) {
	    Entry<? extends K, ? extends V> e = i.next();
	    put(e.getKey(), e.getValue());
	}
    }

    public void clear() {
        if (index != null) { 
            ((Collection)index).clear();
        } else {
            values.clear();
            keys = new Comparable[((Comparable[])keys).length];
        }
    }

    public Set<K> keySet() {
	if (keySet == null) {
	    keySet = new AbstractSet<K>() {
		public Iterator<K> iterator() {
		    return new Iterator<K>() {
			private Iterator<Entry<K,V>> i = entrySet().iterator();

			public boolean hasNext() {
			    return i.hasNext();
			}

			public K next() {
			    return i.next().getKey();
			}

			public void remove() {
			    i.remove();
			}
                    };
		}

		public int size() {
		    return PersistentMapImpl.this.size();
		}

		public boolean contains(Object k) {
		    return PersistentMapImpl.this.containsKey(k);
		}
	    };
	}
	return keySet;
    }

    public Collection<V> values() {
	if (valuesCol == null) {
	    valuesCol = new AbstractCollection<V>() {
		public Iterator<V> iterator() {
		    return new Iterator<V>() {
			private Iterator<Entry<K,V>> i = entrySet().iterator();

			public boolean hasNext() {
			    return i.hasNext();
			}

			public V next() {
			    return i.next().getValue();
			}

			public void remove() {
			    i.remove();
			}
                    };
                }

		public int size() {
		    return PersistentMapImpl.this.size();
		}

		public boolean contains(Object v) {
		    return PersistentMapImpl.this.containsValue(v);
		}
	    };
	}
	return valuesCol;
    }

    protected Iterator<Entry<K,V>> entryIterator() {
        if (index != null) { 
            if (type == ClassDescriptor.tpValue) {
                return new Iterator<Entry<K,V>>() {          
                    private Iterator<PersistentMapEntry<K,V>> i = ((SortedCollection<PersistentMapEntry<K,V>>)index).iterator();

                    public boolean hasNext() {
                        return i.hasNext();
                    }
                    
                    public Entry<K,V> next() {
                        return i.next();
                    }

                    public void remove() {
                        i.remove();
                    }
                };
            } else { 
                return new Iterator<Entry<K,V>>() {          
                    private Iterator<Entry<Object,V>> i = ((Index<V>)index).entryIterator();

                    public boolean hasNext() {
                        return i.hasNext();
                    }
                    
                    public Entry<K,V> next() {
                        final Entry<Object,V> e = i.next();
                        return new Entry<K,V>() {
                            public K getKey() { 
                                return (K)e.getKey();
                            }
                            public V getValue() { 
                                return e.getValue();
                            }
                            public V setValue(V value) {
                                throw new UnsupportedOperationException("Entry.Map.setValue");
                            }
                        };                        
                    }

                    public void remove() {
                        i.remove();
                    }
                };
            }
        } else {
            return new Iterator<Entry<K,V>>() {                     
                private int i = -1;

                public boolean hasNext() {
                    return i+1 < values.size();
                }

                public Entry<K,V> next() {
                    if (!hasNext()) { 
                        throw new NoSuchElementException(); 
                    }
                    i += 1;
                    return new Entry<K,V>() {
                        public K getKey() { 
                            return (K)(((Comparable[])keys)[i]);
                        }
                        public V getValue() { 
                            return values.get(i);
                        }
                        public V setValue(V value) {
                            throw new UnsupportedOperationException("Entry.Map.setValue");
                        }
                    };  
                }

                public void remove() {
                    if (i < 0) {
                        throw new IllegalStateException();
                    }
                    int size = values.size();
                    System.arraycopy(keys, i+1, keys, i, size-i-1);
                    ((Comparable[])keys)[size-1] = null;
                    values.removeObject(i);
                    i -= 1;
                }
            };
        }
    }

    public Set<Entry<K,V>> entrySet() {
	if (entrySet == null) {
	    entrySet = new AbstractSet<Entry<K,V>>() {
		public Iterator<Entry<K,V>> iterator() {
		    return entryIterator();
		}

		public int size() {
		    return PersistentMapImpl.this.size();
		}

                public boolean remove(Object o) {
                    if (!(o instanceof Map.Entry)) {
                        return false;
                    }
                    Map.Entry<K,V> entry = (Map.Entry<K,V>) o;
                    K key = entry.getKey();
                    V value = entry.getValue();
                    if (value != null) { 
                        V v = PersistentMapImpl.this.get(key);
                        if (value.equals(v)) {
                            PersistentMapImpl.this.remove(key);
                            return true;
                        }
                    } else {
                        if (PersistentMapImpl.this.containsKey(key)
                            && PersistentMapImpl.this.get(key) == null)
                        {
                            PersistentMapImpl.this.remove(key);
                            return true;
                        }
                    }
                    return false;
                }
                
		public boolean contains(Object k) {
                    Entry<K,V> e = (Entry<K,V>)k;
                    if (e.getValue() != null) { 
                        return e.getValue().equals(PersistentMapImpl.this.get(e.getKey()));
                    } else {
                        return PersistentMapImpl.this.containsKey(e.getKey()) 
                            && PersistentMapImpl.this.get(e.getKey()) == null;
                    }
		}
	    };
	}
	return entrySet;
    }   

     
    public boolean equals(Object o) {
	if (o == this) {
	    return true;
        }
	if (!(o instanceof Map)) {
	    return false;
        }
	Map<K,V> t = (Map<K,V>) o;
	if (t.size() != size()) {
	    return false;
        }

        try {
            Iterator<Entry<K,V>> i = entrySet().iterator();
            while (i.hasNext()) {
                Entry<K,V> e = i.next();
		K key = e.getKey();
                V value = e.getValue();
                if (value == null) {
                    if (!(t.get(key)==null && t.containsKey(key))) {
                        return false;
                    }
                } else {
                    if (!value.equals(t.get(key))) {
                        return false;
                    }
                }
            }
        } catch(ClassCastException unused) {
            return false;
        } catch(NullPointerException unused) {
            return false;
        }

	return true;
    }

    public int hashCode() {
	int h = 0;
	Iterator<Entry<K,V>> i = entrySet().iterator();
	while (i.hasNext()) {
	    h += i.next().hashCode();
        }
	return h;
    }

    public String toString() {
	StringBuffer buf = new StringBuffer();
	buf.append("{");

	Iterator<Entry<K,V>> i = entrySet().iterator();
        boolean hasNext = i.hasNext();
        while (hasNext) {
	    Entry<K,V> e = i.next();
	    K key = e.getKey();
            V value = e.getValue();
	    if (key == this) {
		buf.append("(this Map)");
            } else {
		buf.append(key);
            }
	    buf.append("=");
	    if (value == this) {
		buf.append("(this Map)");
            } else {
		buf.append(value);
            }
            hasNext = i.hasNext();
            if (hasNext) {
                buf.append(", ");
            }
        }

	buf.append("}");
	return buf.toString();
    }

    final Key generateKey(Object key) {
        return generateKey(key, true);
    }

    final Key generateKey(Object key, boolean inclusive) {
        if (key instanceof Integer) { 
            return new Key(((Integer)key).intValue(), inclusive);
        } else if (key instanceof Byte) { 
            return new Key(((Byte)key).byteValue(), inclusive);
        } else if (key instanceof Character) { 
            return new Key(((Character)key).charValue(), inclusive);
        } else if (key instanceof Short) { 
            return new Key(((Short)key).shortValue(), inclusive);
        } else if (key instanceof Long) { 
            return new Key(((Long)key).longValue(), inclusive);
        } else if (key instanceof Float) { 
            return new Key(((Float)key).floatValue(), inclusive);
        } else if (key instanceof Double) { 
            return new Key(((Double)key).doubleValue(), inclusive);
        } else if (key instanceof String) { 
            return new Key((String)key, inclusive);
        } else if (key instanceof Enum) { 
            return new Key((Enum)key, inclusive);
        } else if (key instanceof java.util.Date) { 
            return new Key((java.util.Date)key, inclusive);
        } else if (key instanceof IValue) { 
            return new Key((IValue)key, inclusive);
        } else { 
            return new Key(key, inclusive);
        }
    }

    public Comparator<? super K> comparator() {
        return null;
    }

    public SortedMap<K,V> subMap(K from, K to) {
        if (from.compareTo(to) > 0) {
            throw new IllegalArgumentException("from > to");
        }
        return new SubMap(from, to);
    }

    public SortedMap<K,V> headMap(K to) {
        return new SubMap(null, to);
    }

    public SortedMap<K,V> tailMap(K from) {
        return new SubMap(from, null);
    }

    private class SubMap extends AbstractMap<K,V> implements SortedMap<K,V> {
        private Key fromKey;
        private Key toKey;
        private K   from;
        private K   to;
        volatile Set<Entry<K,V>> entrySet;

        SubMap(K from, K to) {
            this.from = from;
            this.to = to;
            this.fromKey = from != null ? generateKey(from, true) : null;
            this.toKey = to != null ? generateKey(to, false) : null;
        }

        public boolean isEmpty() {
            return entrySet().isEmpty();
        }

        public boolean containsKey(Object key) {
            return inRange((K)key) && PersistentMapImpl.this.containsKey(key);
        }

        public V get(Object key) {
            if (!inRange((K)key)) {
                return null;
            }
            return PersistentMapImpl.this.get(key);
        }

        public V put(K key, V value) {
            if (!inRange(key)) {
                throw new IllegalArgumentException("key out of range");
            }
            return PersistentMapImpl.this.put(key, value);
        }

        public Comparator<? super K> comparator() {
            return null;
        }

        public K firstKey() {
            return entryIterator(Index.ASCENT_ORDER).next().getKey();
        }

        public K lastKey() {
            return entryIterator(Index.DESCENT_ORDER).next().getKey();
        }

        protected Iterator<Entry<K,V>> entryIterator(final int order) {
            if (index != null) { 
                if (type == ClassDescriptor.tpValue) {               
                    if (order == Index.ASCENT_ORDER) { 
                        return new Iterator<Entry<K,V>>() { 
                            private Iterator<PersistentMapEntry<K,V>> i = ((SortedCollection<PersistentMapEntry<K,V>>)index).iterator(fromKey, toKey);

                            public boolean hasNext() {
                                return i.hasNext();
                            }
                            public Entry<K,V> next() {
                                return i.next();
                            }
                            public void remove() {
                                i.remove();
                            }
                        };
                    } else { 
                        return new Iterator<Entry<K,V>>() { 
                            private ArrayList<PersistentMapEntry<K,V>> entries 
                                = ((SortedCollection<PersistentMapEntry<K,V>>)index).getList(fromKey, toKey);
                            private int i = entries.size();
                            
                            public boolean hasNext() {
                                return i > 0;
                            }
                            public Entry<K,V> next() {
                                if (!hasNext()) { 
                                    throw new NoSuchElementException(); 
                                }
                                return entries.get(--i);
                            }
                            
                            public void remove() {
                                if (i < entries.size() || entries.get(i) == null) {
                                    throw new IllegalStateException();
                                }
                                ((SortedCollection<PersistentMapEntry<K,V>>)index).remove(entries.get(i));
                                entries.set(i, null);
                            }
                        };
                    }
                } else {
                    return new Iterator<Entry<K,V>>() {                        
                        private Iterator<Entry<Object,V>> i = ((Index<V>)index).entryIterator(fromKey, toKey, order);
                        
                        public boolean hasNext() {
                            return i.hasNext();
                        }
                        
                        public Entry<K,V> next() {
                            final Entry<Object,V> e = i.next();
                            return new Entry<K,V>() {
                                public K getKey() { 
                                    return (K)e.getKey();
                                }
                                public V getValue() { 
                                    return e.getValue();
                                }
                                public V setValue(V value) {
                                    throw new UnsupportedOperationException("Entry.Map.setValue");
                                }
                            };  
                        }
                        
                        public void remove() {
                            i.remove();
                        }
                    };
                } 
            } else {
                if (order == Index.ASCENT_ORDER) { 
                    final int beg = (from != null ? binarySearch(from) : 0) - 1;
                    final int end = values.size();

                    return new Iterator<Entry<K,V>>() {                     
                        private int i = beg;
                        
                        public boolean hasNext() {
                            return i+1 < end && (to == null || ((Comparable[])keys)[i+1].compareTo(to) < 0);
                        }
                        
                        public Entry<K,V> next() {
                            if (!hasNext()) { 
                                throw new NoSuchElementException(); 
                            }
                            i += 1;
                            return new Entry<K,V>() {
                                public K getKey() { 
                                    return (K)((Comparable[])keys)[i];
                                }
                                public V getValue() { 
                                    return values.get(i);
                                }
                                public V setValue(V value) {
                                    throw new UnsupportedOperationException("Entry.Map.setValue");
                                }
                            };  
                        }
                        
                        public void remove() {
                            if (i < 0) {
                                throw new IllegalStateException();
                            }
                            int size = values.size();
                            System.arraycopy(keys, i+1, keys, i, size-i-1);
                            ((Comparable[])keys)[size-1] = null;
                            values.removeObject(i);
                            i -= 1;
                        }
                    };
                } else {
                    final int beg = (to != null ? binarySearch(to) : 0) - 1;

                    return new Iterator<Entry<K,V>>() {                     
                        private int i = beg;
                        
                        public boolean hasNext() {
                            return i > 0 && (from == null || ((Comparable[])keys)[i-1].compareTo(from) >= 0);
                        }
                        
                        public Entry<K,V> next() {
                            if (!hasNext()) { 
                                throw new NoSuchElementException(); 
                            }
                            i -= 1;
                            return new Entry<K,V>() {
                                public K getKey() { 
                                    return (K)((Comparable[])keys)[i];
                                }
                                public V getValue() { 
                                    return values.get(i);
                                }
                                public V setValue(V value) {
                                    throw new UnsupportedOperationException("Entry.Map.setValue");
                                }
                            };  
                        }
                        
                        public void remove() {
                            if (i < 0) {
                                throw new IllegalStateException();
                            }
                            int size = values.size();
                            System.arraycopy(keys, i+1, keys, i, size-i-1);
                            ((Comparable[])keys)[size-1] = null;
                            values.removeObject(i);
                        }
                    };
                }
            }
        }

        public Set<Entry<K,V>> entrySet() {
            if (entrySet == null) {
                entrySet = new AbstractSet<Entry<K,V>>() {
                    public Iterator<Entry<K,V>> iterator() {
                        return entryIterator(Index.ASCENT_ORDER);
                    }
                    
                    public int size() {
                        Iterator<Entry<K,V>> i = iterator();
                        int n;
                        for (n = 0; i.hasNext(); i.next()) { 
                            n += 1;
                        }
                        return n;
                    }

                    public boolean isEmpty() {
                        return !iterator().hasNext();
                    }

                    public boolean remove(Object o) {
                        if (!(o instanceof Map.Entry)) {
                            return false;
                        }
                        Map.Entry<K,V> entry = (Map.Entry<K,V>) o;
                        K key = entry.getKey();
                        if (!inRange(key)) {
                            return false;
                        }
                        V value = entry.getValue();
                        if (value != null) { 
                            V v = PersistentMapImpl.this.get(key);
                            if (value.equals(v)) {
                                PersistentMapImpl.this.remove(key);
                                return true;
                            }
                        } else {
                            if (PersistentMapImpl.this.containsKey(key)
                                && PersistentMapImpl.this.get(key) == null)
                            {
                                PersistentMapImpl.this.remove(key);
                                return true;
                            }
                        }
                        return false;
                    }

                    public boolean contains(Object k) {
                        Entry<K,V> e = (Entry<K,V>)k;
                        if (!inRange(e.getKey())) {
                            return false;
                        }                        
                        if (e.getValue() != null) { 
                            return e.getValue().equals(PersistentMapImpl.this.get(e.getKey()));
                        } else {
                            return PersistentMapImpl.this.containsKey(e.getKey()) 
                                && PersistentMapImpl.this.get(e.getKey()) == null;
                        }
                    }
                };
            }
            return entrySet;
        }   

        public SortedMap<K,V> subMap(K from, K to) {
            if (!inRange2(from)) {
                throw new IllegalArgumentException("'from' out of range");
            }
            if (!inRange2(to)) {
                throw new IllegalArgumentException("'to' out of range");
            }
            return new SubMap(from, to);
        }

        public SortedMap<K,V> headMap(K to) {
            if (!inRange2(to)) {
                throw new IllegalArgumentException("'to' out of range");
            }
            return new SubMap(this.from, to);
        }

        public SortedMap<K,V> tailMap(K from) {
            if (!inRange2(from)) {
                throw new IllegalArgumentException("'from' out of range");
            }
            return new SubMap(from, this.to);
        }

        private boolean inRange(K key) {
            return (from == null || key.compareTo(from) >= 0) &&
                (to == null || key.compareTo(to) < 0);
        }

        // This form allows the high endpoint (as well as all legit keys)
        private boolean inRange2(K key) {
            return (from == null || key.compareTo(from) >= 0) &&
                (to == null || key.compareTo(to) <= 0);
        }
    }

    public K firstKey() {
        if (index != null) { 
            if (type == ClassDescriptor.tpValue) {               
                return ((SortedCollection<PersistentMapEntry<K,V>>)index).iterator().next().key;
            } else { 
                return (K)((Index<V>)index).entryIterator().next().getKey();
            }
        } else { 
            Comparable[] keys = (Comparable[])this.keys;
            if (values.size() == 0) {
                throw new NoSuchElementException(); 
            }
            return (K)((Comparable[])keys)[0];
        }
    }

    public K lastKey() {
        if (index != null) { 
            if (type == ClassDescriptor.tpValue) {       
                ArrayList<PersistentMapEntry<K,V>> entries = ((SortedCollection<PersistentMapEntry<K,V>>)index).getList(null, null);
                return entries.get(entries.size()-1).key;
            } else { 
                return (K)((Index<V>)index).entryIterator(null, null, Index.DESCENT_ORDER).next().getKey();
            }
        } else { 
            int size = values.size();
            if (size == 0) {
                throw new NoSuchElementException(); 
            }
            return (K)((Comparable[])keys)[size-1];
        }
    }

    public Iterator<V> select(Class cls, String predicate) { 
        Query<V> query = new QueryImpl<V>(getStorage());
        return query.select(cls, values().iterator(), predicate);
    }
} 
