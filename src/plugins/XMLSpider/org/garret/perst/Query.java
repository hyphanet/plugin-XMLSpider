package plugins.XMLSpider.org.garret.perst;

import java.util.Collection;
import java.util.Iterator;

/**
 * Class representing JSQL query. JSQL allows to select members of Perst collections 
 * using SQL like predicate. Almost all Perst collections have select() method 
 * which execute arbitrary JSQL query. But it is also possible to create Query instance explicitely, 
 * Using storage.createQuery class. In this case it is possible to specify query with parameters, 
 * once prepare query and then multiple times specify parameters and execute it. 
 * Also Query interface allows to specify <i>indices</i> and <i>resolvers</i>.
 * JSQL can use arbitrary Perst <code>GenericIndex</code> to perform fast selection if object
 * instead of sequeial search. And user provided <i>resolver</i> allows to substitute SQL joins.
 */
public interface Query<T> extends Iterable<T> 
{
    /**
     * Execute query
     * @param cls class of inspected objects
     * @param iterator iterator for sequential access to objects in the table
     * @param predicate selection crieria
     * @return iterator through selected objects. This iterator doesn't support remove() 
     * method.
     */
    public IterableIterator<T> select(Class cls, Iterator<T> iterator, String predicate) throws CompileError;


    /**
     * Execute query
     * @param className name of the class of inspected objects
     * @param iterator iterator for sequential access to objects in the table
     * @param predicate selection crieria
     * @return iterator through selected objects. This iterator doesn't support remove() 
     * method.
     */
    public IterableIterator<T> select(String className, Iterator<T> iterator, String predicate) throws CompileError;

    /**
     * Set value of query parameter
     * @param index parameters index (1 based)
     * @param value value of parameter (for scalar parameters instance f correspondendt wrapper class, 
     * for example <code>java.lang.Long</code>
     */
    public void setParameter(int index, Object value);

    /**
     * Set value of query parameter
     * @param index parameters index (1 based)
     * @param value value of integer parameter
     */
    public void setIntParameter(int index, long value);

    /**
     * Set value of query parameter
     * @param index parameters index (1 based)
     * @param value value of real parameter
     */
    public void setRealParameter(int index, double value);

    /**
     * Set value of query parameter
     * @param index parameters index (1 based)
     * @param value value of boolean parameter
     */
    public void setBoolParameter(int index, boolean value);

    /**
     * Prepare SQL statement
     * @param cls class of iterated objects
     * @param predicate selection crieria with '?' placeholders for parameter value
     */    
    public void prepare(Class cls, String predicate);

    /**
     * Prepare SQL statement
     * @param className name of the class of iterated objects
     * @param predicate selection crieria with '?' placeholders for parameter value
     */    
    public void prepare(String className, String predicate);

    /**
     * Execute prepared query
     * @param iterator iterator for sequential access to objects in the table
     * @return iterator through selected objects. This iterator doesn't support remove() 
     * method.
     */
    public IterableIterator<T> execute(Iterator<T> iterator);
            
    /**
     * Execute prepared query using iterator obtained from index registered by Query.setClassExtent method
     * @return iterator through selected objects. This iterator doesn't support remove() 
     * method.
     */
    public IterableIterator<T> execute();
            
    /**
     * Enable or disable reporting of runtime errors on console.
     * Runtime errors during JSQL query are reported in two ways:
     * <OL>
     * <LI>If query error reporting is enabled then message is  printed to System.err</LI>
     * <LI>If storage listener is registered, then JSQLRuntimeError of method listener is invoked</LI>
     * </OL>     
     * By default reporting to System.err is enabled.
     * @param enabled if <code>true</code> then reportnig is enabled
     */
    public void enableRuntimeErrorReporting(boolean enabled); 
    
    /**
     * Specify resolver. Resolver can be used to replaced SQL JOINs: given object ID, 
     * it will provide reference to the resolved object
     * @param original class which instances will have to be resolved
     * @param resolved class of the resolved object
     * @param resolver class implementing Resolver interface
     */
    public void setResolver(Class original, Class resolved, Resolver resolver);

    /**
     * Add index which can be used to optimize query execution (replace sequential search with direct index access)
     * @param key indexed field
     * @param index implementation of index
     */
    public void addIndex(String key, GenericIndex<T> index);

    /**
     * Set index provider for this query.
     * Available indices should be either registered using addIndex method, either 
     * should be accessible through index provider
     * @param indexProvider index provider
     */
    public void setIndexProvider(IndexProvider indexProvider);

    /**
     * Set class for which this query will be executed
     * @param cls queried class
     */
    public void setClass(Class cls);

    enum ClassExtentLockType { 
        None,
        Shared,
        Exclusive
    };
    
    /**
     * Set class extent used to obtain iterator through all instances of this class
     * @param set class extent
     * @param lock type of the lock which should be obtained for the set before query execution
     */
    public void setClassExtent(Collection<T> set, ClassExtentLockType lock);

    /**
     * Get query code generator for the specified class
     * @param cls class for which query is constructed
     * @return code generator for the specified class
     */
    public CodeGenerator getCodeGenerator(Class cls);

    /**
     * Get query code generator for class associated with the query by Query.setClass method
     * @return code generator for class associated with the query
     */
    public CodeGenerator getCodeGenerator();
}
