//-< JSQLNoSuchFieldException.java >---------------------------------*--------*
// JSQL                       Version 1.04       (c) 1999  GARRET    *     ?  *
// (Java SQL)                                                        *   /\|  *
//                                                                   *  /  \  *
//                          Created:      7-Dec-2002  K.A. Knizhnik  * / [] \ *
//                          Last update:  9-Dec-2002  K.A. Knizhnik  * GARRET *
//-------------------------------------------------------------------*--------*
// Exception thown when field is not found during runtime lookup
//-------------------------------------------------------------------*--------*

package plugins.XMLSpider.org.garret.perst;

/**
 * Exception thown when field is not found during runtime lookup
 */
public class JSQLNoSuchFieldException extends JSQLRuntimeException { 
    /**
     * Constructor of exception
     * @param target class of the target object in which field was not found
     * @param fieldName name of the locate field
     */
    public JSQLNoSuchFieldException(Class target, String fieldName) { 
        super("Dynamic lookup failed for field ", target, fieldName);
    }
}



