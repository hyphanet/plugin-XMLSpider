package plugins.XMLSpider.org.garret.perst.impl;

import java.lang.reflect.Field;

import plugins.XMLSpider.org.garret.perst.MultidimensionalComparator;
import plugins.XMLSpider.org.garret.perst.Storage;
import plugins.XMLSpider.org.garret.perst.StorageError;

/**
 * Implementation of multidimensional reflection comparator using reflection
 */
public class ReflectionMultidimensionalComparator<T> extends MultidimensionalComparator<T> 
{ 
    private String   className;
    private String[] fieldNames;
    private boolean  treateZeroAsUndefinedValue;

    transient private Class cls;
    transient private Field[] fields;
    transient private ClassDescriptor desc;

    public void onLoad()
    {
        cls = ClassDescriptor.loadClass(getStorage(), className);
        locateFields();
    }

    private final void locateFields() 
    {
        if (fieldNames == null) { 
            fields = cls.getDeclaredFields();
        } else { 
            fields = new Field[fieldNames.length];
            for (int i = 0; i < fields.length; i++) { 
                fields[i] = ClassDescriptor.locateField(cls, fieldNames[i]);
                if (fields[i] == null) { 
                    throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND, className + "." + fieldNames[i]);
                }
            }
        }
    }

    public ReflectionMultidimensionalComparator(Storage storage, Class cls, String[] fieldNames, boolean treateZeroAsUndefinedValue) 
    { 
        super(storage);
        this.cls = cls;
        this.fieldNames = fieldNames;
        this.treateZeroAsUndefinedValue = treateZeroAsUndefinedValue;
        className = ClassDescriptor.getClassName(cls); 
        locateFields();
    }

    ReflectionMultidimensionalComparator() {}
    
    private static boolean isZero(Object val) 
    { 
        return val instanceof Double || val instanceof Float 
            ? ((Number)val).doubleValue() == 0.0
            : val instanceof Number 
              ? ((Number)val).longValue() == 0 
              : false;
    }

    public int compare(T m1, T m2, int i)
    {
        try { 
            Comparable c1 = (Comparable)fields[i].get(m1);
            Comparable c2 = (Comparable)fields[i].get(m2);
            if (c1 == null && c2 == null) { 
                return EQ;
            } else if (c1 == null || (treateZeroAsUndefinedValue && isZero(c1))) { 
                return LEFT_UNDEFINED;
            } else if (c2 == null || (treateZeroAsUndefinedValue && isZero(c2))) { 
                return RIGHT_UNDEFINED;
            } else { 
                int diff = c1.compareTo(c2);
                return diff < 0 ? LT : diff == 0 ? EQ : GT;
            }
        } catch (IllegalAccessException x) { 
            throw new IllegalAccessError();
        }
    }

    public int getNumberOfDimensions() { 
        return fields.length;
    }

    public T cloneField(T obj, int i) { 
        if (desc == null) { 
            desc = ((StorageImpl)getStorage()).findClassDescriptor(cls);
        } 
        T clone = (T)desc.newInstance();
        try { 
            fields[i].set(clone, fields[i].get(obj));
            return clone;
        } catch (IllegalAccessException x) { 
            throw new IllegalAccessError();
        }
    }
}

