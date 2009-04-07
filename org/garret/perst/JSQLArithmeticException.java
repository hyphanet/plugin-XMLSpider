//-< JSQLArithmeticException.java >----------------------------------*--------*
// JSQL                       Version 1.04       (c) 1999  GARRET    *     ?  *
// (Java SQL)                                                        *   /\|  *
//                                                                   *  /  \  *
//                          Created:     10-Dec-2002  K.A. Knizhnik  * / [] \ *
//                          Last update: 10-Dec-2002  K.A. Knizhnik  * GARRET *
//-------------------------------------------------------------------*--------*
// Exception thown in case of incorect operands for integer operations
//-------------------------------------------------------------------*--------*

package plugins.XMLSpider.org.garret.perst;

/**
 * Exception thown in case of incorect operands for integer operations
 */
public class JSQLArithmeticException extends JSQLRuntimeException { 
    /**
     * Constructor of exception
     */
    public JSQLArithmeticException(String msg) { 
        super(msg, null, null);
    }
}



