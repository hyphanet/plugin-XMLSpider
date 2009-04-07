//-< JSQLRuntimeException.java >-------------------------------------*--------*
// JSQL                       Version 1.04       (c) 1999  GARRET    *     ?  *
// (Java SQL)                                                        *   /\|  *
//                                                                   *  /  \  *
//                          Created:      9-Dec-2002  K.A. Knizhnik  * / [] \ *
//                          Last update:  9-Dec-2002  K.A. Knizhnik  * GARRET *
//-------------------------------------------------------------------*--------*
// Exception thown by JSQL at runtime
//-------------------------------------------------------------------*--------*

package plugins.XMLSpider.org.garret.perst;

/**
 * Exception thown by JSQL at runtime which should be ignored and boolean expression caused this
 * exption should be treated as false
 */
public class JSQLRuntimeException extends RuntimeException { 
    /**
     * Constructor of exception
     * @param target class of the target object in which field was not found
     * @param fieldName name of the locate field
     */
    public JSQLRuntimeException(String message, Class target, String fieldName) { 
        super(message);
        this.target  = target;
        this.fieldName = fieldName;
    }

    /**
     * Get class in which lookup was performed
     */
    public Class getTarget() { 
        return target;
    }

    /**
     * Get name of the field
     */
    public String getFieldName() { 
        return fieldName;
    }

    String fieldName;
    Class  target;
}



