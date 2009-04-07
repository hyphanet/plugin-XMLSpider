//-< CodeGenerator.java >--------------------------------------------*--------*
// JSQL                       Version 1.04       (c) 1999  GARRET    *     ?  *
// (Java SQL)                                                        *   /\|  *
//                                                                   *  /  \  *
//                          Created:     23-Mar-2009  K.A. Knizhnik  * / [] \ *
//                          Last update: 23-Mar-2009  K.A. Knizhnik  * GARRET *
//-------------------------------------------------------------------*--------*
// Exception thrown by code generator
//-------------------------------------------------------------------*--------*

package plugins.XMLSpider.org.garret.perst;

/**
 * Exception thrown by code generator
 */
public class CodeGeneratorException extends RuntimeException { 
    public  CodeGeneratorException(String msg) { 
	super(msg);
    }
}
