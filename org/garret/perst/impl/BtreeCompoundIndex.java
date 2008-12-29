package plugins.XMLSpider.org.garret.perst.impl;
import plugins.XMLSpider.org.garret.perst.*;

import java.util.*;

class BtreeCompoundIndex<T extends IPersistent> extends Btree<T> implements Index<T> { 
    int[]    types;

    BtreeCompoundIndex() {}
    
    BtreeCompoundIndex(Class[] keyTypes, boolean unique) {
        this.unique = unique;
        type = ClassDescriptor.tpArrayOfByte;        
        types = new int[keyTypes.length];
        for (int i = 0; i < keyTypes.length; i++) {
            types[i] = getCompoundKeyComponentType(keyTypes[i]);
        }
    }

    BtreeCompoundIndex(int[] types, boolean unique) {
        this.types = types;
        this.unique = unique;
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
        } else if (c.equals(byte[].class)) { 
            return ClassDescriptor.tpArrayOfByte;
        } else if (IPersistent.class.isAssignableFrom(c)) {
            return ClassDescriptor.tpObject;
        } else { 
            throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE, c);
        }
    }

    public Class[] getKeyTypes() {
        Class[] keyTypes = new Class[types.length];
        for (int i = 0; i < keyTypes.length; i++) { 
            keyTypes[i] = mapKeyType(types[i]);
        }
        return keyTypes;
    }

    int compareByteArrays(byte[] key, byte[] item, int offs, int lengtn) { 
        int o1 = 0;
        int o2 = offs;
        byte[] a1 = key;
        byte[] a2 = item;
        for (int i = 0; i < types.length && o1 < key.length; i++) {
            int diff = 0;
            switch (types[i]) { 
              case ClassDescriptor.tpBoolean:
              case ClassDescriptor.tpByte:
                diff = a1[o1++] - a2[o2++];
                break;
              case ClassDescriptor.tpShort:
                diff = Bytes.unpack2(a1, o1) - Bytes.unpack2(a2, o2);
                o1 += 2;
                o2 += 2;
                break;
              case ClassDescriptor.tpChar:
                diff = (char)Bytes.unpack2(a1, o1) - (char)Bytes.unpack2(a2, o2);
                o1 += 2;
                o2 += 2;
                break;
              case ClassDescriptor.tpInt:
              case ClassDescriptor.tpObject:
              case ClassDescriptor.tpEnum:
              {
                  int i1 = Bytes.unpack4(a1, o1);
                  int i2 = Bytes.unpack4(a2, o2);
                  diff = i1 < i2 ? -1 : i1 == i2 ? 0 : 1;
                  o1 += 4;
                  o2 += 4;
                  break;
              }
              case ClassDescriptor.tpLong:
              case ClassDescriptor.tpDate:
              {
                  long l1 = Bytes.unpack8(a1, o1);
                  long l2 = Bytes.unpack8(a2, o2);
                  diff = l1 < l2 ? -1 : l1 == l2 ? 0 : 1;
                  o1 += 8;
                  o2 += 8;
                  break;
              }
              case ClassDescriptor.tpFloat:
              {
                  float f1 = Float.intBitsToFloat(Bytes.unpack4(a1, o1));
                  float f2 = Float.intBitsToFloat(Bytes.unpack4(a2, o2));
                  diff = f1 < f2 ? -1 : f1 == f2 ? 0 : 1;
                  o1 += 4;
                  o2 += 4;
                  break;
              }
              case ClassDescriptor.tpDouble:
              {
                  double d1 = Double.longBitsToDouble(Bytes.unpack8(a1, o1));
                  double d2 = Double.longBitsToDouble(Bytes.unpack8(a2, o2));
                  diff = d1 < d2 ? -1 : d1 == d2 ? 0 : 1;
                  o1 += 8;
                  o2 += 8;
                  break;
              }
              case ClassDescriptor.tpString:
              {
                  int len1 = Bytes.unpack4(a1, o1);
                  int len2 = Bytes.unpack4(a2, o2);
                  o1 += 4;
                  o2 += 4;
                  int len = len1 < len2 ? len1 : len2;
                  while (--len >= 0) { 
                      diff = (char)Bytes.unpack2(a1, o1) - (char)Bytes.unpack2(a2, o2);
                      if (diff != 0) { 
                          return diff;
                      }
                      o1 += 2;
                      o2 += 2;
                  }
                  diff = len1 - len2;
                  break;
              }
              case ClassDescriptor.tpArrayOfByte:
              {
                  int len1 = Bytes.unpack4(a1, o1);
                  int len2 = Bytes.unpack4(a2, o2);
                  o1 += 4;
                  o2 += 4;
                  int len = len1 < len2 ? len1 : len2;
                  while (--len >= 0) { 
                      diff = a1[o1++] - a2[o2++];
                      if (diff != 0) { 
                          return diff;
                      }
                  }
                  diff = len1 - len2;
                  break;
              }
              default:
                Assert.failed("Invalid type");
            }
            if (diff != 0) { 
                return diff;
            }
        }
        return 0;
    }

    Object unpackByteArrayKey(Page pg, int pos) {
        int offs = BtreePage.firstKeyOffs + BtreePage.getKeyStrOffs(pg, pos);
        byte[] data = pg.data;
        Object values[] = new Object[types.length];

        for (int i = 0; i < types.length; i++) {
            Object v = null;
            switch (types[i]) { 
              case ClassDescriptor.tpBoolean:
                v = Boolean.valueOf(data[offs++] != 0);
                break;
              case ClassDescriptor.tpByte:
                v = new Byte(data[offs++]);
                break;
              case ClassDescriptor.tpShort:
                v = Short.valueOf(Bytes.unpack2(data, offs));
                offs += 2;
                break;
              case ClassDescriptor.tpChar:
                v = new Character((char)Bytes.unpack2(data, offs));
                offs += 2;
                break;
              case ClassDescriptor.tpInt:
                v = new Integer(Bytes.unpack4(data, offs));
                offs += 4;
                break;
              case ClassDescriptor.tpObject:
              {
                  int oid = Bytes.unpack4(data, offs);
                  v = oid == 0 ? null : ((StorageImpl)getStorage()).lookupObject(oid, null);
                  offs += 4;
                  break;
              }
              case ClassDescriptor.tpLong:
                v = new Long(Bytes.unpack8(data, offs));
                offs += 8;
                break;
              case ClassDescriptor.tpDate:
              {
                  long msec = Bytes.unpack8(data, offs);
                  v = msec == -1 ? null : new Date(msec);
                  offs += 8;
                  break;
              }
              case ClassDescriptor.tpFloat:
                v = new Float(Float.intBitsToFloat(Bytes.unpack4(data, offs)));
                offs += 4;
                break;
              case ClassDescriptor.tpDouble:
                v = new Double(Double.longBitsToDouble(Bytes.unpack8(data, offs)));
                offs += 8;
                break;
              case ClassDescriptor.tpString:
              {
                  int len = Bytes.unpack4(data, offs);
                  offs += 4;
                  char[] sval = new char[len];
                  for (int j = 0; j < len; j++) { 
                      sval[j] = (char)Bytes.unpack2(data, offs);
                      offs += 2;
                  }
                  v = new String(sval);
                  break;
              }
              case ClassDescriptor.tpArrayOfByte:
              {
                  int len = Bytes.unpack4(data, offs);
                  offs += 4;
                  byte[] bval = new byte[len];
                  System.arraycopy(data, offs, bval, 0, len);
                  offs += len;
                  break;
              }
              default:
                Assert.failed("Invalid type");
            }
            values[i] = v;
        }
        return values;
    }
            

    private Key convertKey(Key key) { 
        if (key == null) { 
            return null;
        }
        if (key.type != ClassDescriptor.tpArrayOfObject) { 
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }
        Object[] values = (Object[])key.oval;
        ByteBuffer buf = new ByteBuffer();
        int dst = 0;
        for (int i = 0; i < values.length; i++) { 
            Object v = values[i];
            switch (types[i]) {
              case ClassDescriptor.tpBoolean:
                buf.extend(dst+1);
                buf.arr[dst++] = (byte)(((Boolean)v).booleanValue() ? 1 : 0);
                break;
              case ClassDescriptor.tpByte:
                buf.extend(dst+1);
                buf.arr[dst++] = ((Number)v).byteValue();
                break;
              case ClassDescriptor.tpShort:
                buf.extend(dst+2);
                Bytes.pack2(buf.arr, dst, ((Number)v).shortValue());
                dst += 2;
                break;
              case ClassDescriptor.tpChar:
                buf.extend(dst+2);
                Bytes.pack2(buf.arr, dst, (v instanceof Number) ? ((Number)v).shortValue() : (short)((Character)v).charValue());
                dst += 2;
                break;
              case ClassDescriptor.tpInt:
                buf.extend(dst+4);
                Bytes.pack4(buf.arr, dst, ((Number)v).intValue());
                dst += 4;
                break;
              case ClassDescriptor.tpObject:
                buf.extend(dst+4);
                Bytes.pack4(buf.arr, dst, v == null ? 0 : ((IPersistent)v).getOid());
                dst += 4;
                break;
              case ClassDescriptor.tpLong:
                buf.extend(dst+8);
                Bytes.pack8(buf.arr, dst, ((Number)v).longValue());
                dst += 8;
                break;
              case ClassDescriptor.tpDate:
                buf.extend(dst+8);
                Bytes.pack8(buf.arr, dst, v == null ? -1 : ((Date)v).getTime());
                dst += 8;
                break;
              case ClassDescriptor.tpFloat:
                buf.extend(dst+4);
                Bytes.pack4(buf.arr, dst, Float.floatToIntBits(((Number)v).floatValue()));
                dst += 4;
                break;
              case ClassDescriptor.tpDouble:
                buf.extend(dst+8);
                Bytes.pack8(buf.arr, dst, Double.doubleToLongBits(((Number)v).doubleValue()));
                dst += 8;
                break;
              case ClassDescriptor.tpEnum:
                buf.extend(dst+4);
                Bytes.pack4(buf.arr, dst, ((Enum)v).ordinal());
                dst += 4;
                break;
              case ClassDescriptor.tpString:
              {
                  buf.extend(dst+4);
                  if (v != null) { 
                      String str = (String)v;
                      int len = str.length();
                      Bytes.pack4(buf.arr, dst, len);
                      dst += 4;
                      buf.extend(dst + len*2);
                      for (int j = 0; j < len; j++) { 
                          Bytes.pack2(buf.arr, dst, (short)str.charAt(j));
                          dst += 2;
                      }
                  } else { 
                      Bytes.pack4(buf.arr, dst, 0);
                      dst += 4;
                  }
                  break;
              }
              case ClassDescriptor.tpArrayOfByte:
              {
                  buf.extend(dst+4);
                  if (v != null) { 
                      byte[] arr = (byte[])v;
                      int len = arr.length;
                      Bytes.pack4(buf.arr, dst, len);
                      dst += 4;                          
                      buf.extend(dst + len);
                      System.arraycopy(arr, 0, buf.arr, dst, len);
                      dst += len;
                  } else { 
                      Bytes.pack4(buf.arr, dst, 0);
                      dst += 4;
                  }
                  break;
              }
              default:
                Assert.failed("Invalid type");
            }
        }
        return new Key(buf.toArray(), key.inclusion != 0);
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
}

