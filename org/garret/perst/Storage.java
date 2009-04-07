package plugins.XMLSpider.org.garret.perst;

import java.util.Iterator;

import plugins.XMLSpider.org.garret.perst.fulltext.FullTextIndex;
import plugins.XMLSpider.org.garret.perst.fulltext.FullTextSearchHelper;
import plugins.XMLSpider.org.garret.perst.impl.ThreadTransactionContext;

/**
 * Object storage
 */
public interface Storage { 
    /**
     * Constant specifying that page pool should be dynamically extended 
     * to conatins all database file pages
     */
    public static final int INFINITE_PAGE_POOL = 0;
    /**
     * Constant specifying default pool size
     */
    public static final int DEFAULT_PAGE_POOL_SIZE = 4*1024*1024;

    /**
     * Open the storage
     * @param filePath path to the database file
     * @param pagePoolSize size of page pool (in bytes). Page pool should contain
     * at least ten 4kb pages, so minimal page pool size should be at least 40Kb.
     * But larger page pool usually leads to better performance (unless it could not fit
     * in memory and cause swapping). Value 0 of this paremeter corresponds to infinite
     * page pool (all pages are cashed in memory). It is especially useful for in-memory
     * database, when storage is created with NullFile.
     * 
     */
    public void open(String filePath, long pagePoolSize);

    /**
     * Open the storage
     * @param file user specific implementation of IFile interface
     * @param pagePoolSize size of page pool (in bytes). Page pool should contain
     * at least ten 4kb pages, so minimal page pool size should be at least 40Kb.
     * But larger page pool ussually leads to better performance (unless it could not fit
     * in memory and cause swapping).
     */
    public void open(IFile file, long pagePoolSize);

    /**
     * Open the storage with default page pool size
     * @param file user specific implementation of IFile interface
     */ 
    public void open(IFile file);

    /**
     * Open the storage with default page pool size
     * @param filePath path to the database file
     */ 
    public void open(String filePath);

    /**
     * Open the encrypted storage
     * @param filePath path to the database file
     * @param pagePoolSize size of page pool (in bytes). Page pool should contain
     * at least then 4kb pages, so minimal page pool size should be at least 40Kb.
     * But larger page pool usually leads to better performance (unless it could not fit
     * in memory and cause swapping).
     * @param cipherKey cipher key
     */
    public void open(String filePath, long pagePoolSize, String cipherKey);

    /**
     * Check if database is opened
     * @return <code>true</code> if database was opened by <code>open</code> method, 
     * <code>false</code> otherwise
     */
    public boolean isOpened();
    
    /**
     * Get storage root. Storage can have exactly one root object. 
     * If you need to have several root object and access them by name (as is is possible 
     * in many other OODBMSes), you should create index and use it as root object.
     * @return root object or <code>null</code> if root is not specified (storage is not yet initialized)
     */
    public Object getRoot();
    
    /**
     * Set new storage root object.
     * Previous reference to the root object is rewritten but old root is not automatically deallocated.
     * @param root object to become new storage root. If it is not persistent yet, it is made
     * persistent and stored in the storage
     */
    public void setRoot(Object root);

    

    /**
     * Commit changes done by the last transaction. Transaction is started implcitlely with forst update
     * opertation.
     */
    public void commit();

    /**
     * Rollback changes made by the last transaction
     */
    public void rollback();


    /**
     * Backup current state of database
     * @param out output stream to which backup is done
     */
    public void backup(java.io.OutputStream out) throws java.io.IOException;

    /**
     * Exclusive per-thread transaction: each thread access database in exclusive mode
     */
    public static final int EXCLUSIVE_TRANSACTION   = 0;
    /**
     * Alias for EXCLUSIVE_TRANSACTION. In case of multiclient access, 
     * any transaction modifying database should be exclusive.
     */
    public static final int READ_WRITE_TRANSACTION = EXCLUSIVE_TRANSACTION;
    /**
     * Cooperative mode; all threads share the same transaction. Commit will commit changes made
     * by all threads. To make this schema work correctly, it is necessary to ensure (using locking)
     * that no thread is performing update of the database while another one tries to perform commit.
     * Also please notice that rollback will undo the work of all threads. 
     */
    public static final int COOPERATIVE_TRANSACTION = 1;
    /**
     * Alias for COOPERATIVE_TRANSACTION. In case of multiclient access, 
     * only read-only transactions can be executed in parallel.
     */
    public static final int READ_ONLY_TRANSACTION = COOPERATIVE_TRANSACTION;
    /**
     * Serializable per-thread transaction. Unlike exclusive mode, threads can concurrently access database, 
     * but effect will be the same as them work exclusively.
     * To provide such behavior, programmer should lock all access objects (or use hierarchical locking).
     * When object is updated, exclusive lock should be set, otherwise shared lock is enough.
     * Lock should be preserved until the end of transaction.
     */
    public static final int SERIALIZABLE_TRANSACTION = 2;

    /**
     * Read only transaction which can be started at replicastion slave node.
     * It runs concurrently with receiving updates from master node.
     */
    public static final int REPLICATION_SLAVE_TRANSACTION = 3;

    /** 
     * Begin per-thread transaction. Three types of per-thread transactions are supported: 
     * exclusive, cooperative and serializable. In case of exclusive transaction, only one 
     * thread can update the database. In cooperative mode, multiple transaction can work 
     * concurrently and commit() method will be invoked only when transactions of all threads
     * are terminated. Serializable transactions can also work concurrently. But unlike
     * cooperative transaction, the threads are isolated from each other. Each thread
     * has its own associated set of modified objects and committing the transaction will cause
     * saving only of these objects to the database. To synchronize access to the objects
     * in case of serializable transaction programmer should use lock methods
     * of IResource interface. Shared lock should be set before read access to any object, 
     * and exclusive lock - before write access. Locks will be automatically released when
     * transaction is committed (so programmer should not explicitly invoke unlock method)
     * In this case it is guaranteed that transactions are serializable.<br>
     * It is not possible to use <code>IPersistent.store()</code> method in
     * serializable transactions. That is why it is also not possible to use Index and FieldIndex
     * containers (since them are based on B-Tree and B-Tree directly access database pages
     * and use <code>store()</code> method to assign OID to inserted object. 
     * You should use <code>SortedCollection</code> based on T-Tree instead or alternative
     * B-Tree implemenataion (set "perst.alternative.btree" property).
     * @param mode <code>EXCLUSIVE_TRANSACTION</code>, <code>COOPERATIVE_TRANSACTION</code>, 
     * <code>SERIALIZABLE_TRANSACTION</code> or <code>REPLICATION_SLAVE_TRANSACTION</code>
     */
    public void beginThreadTransaction(int mode);
    
    /**
     * End per-thread transaction started by beginThreadTransaction method.<br>
     * If transaction is <i>exclusive</i>, this method commits the transaction and
     * allows other thread to proceed.<br>
     * If transaction is <i>serializable</i>, this method commits sll changes done by this thread
     * and release all locks set by this thread.<br>     
     * If transaction is <i>cooperative</i>, this method decrement counter of cooperative
     * transactions and if it becomes zero - commit the work
     */
    public void endThreadTransaction(); 

    /**
     * End per-thread cooperative transaction with specified maximal delay of transaction
     * commit. When cooperative transaction is ended, data is not immediately committed to the
     * disk (because other cooperative transaction can be active at this moment of time).
     * Instead of it cooperative transaction counter is decremented. Commit is performed
     * only when this counter reaches zero value. But in case of heavy load there can be a lot of
     * requests and so a lot of active cooperative transactions. So transaction counter never reaches zero value.
     * If system crash happens a large amount of work will be lost in this case. 
     * To prevent such scenario, it is possible to specify maximal delay of pending transaction commit.
     * In this case when such timeout is expired, new cooperative transaction will be blocked until
     * transaction is committed.
     * @param maxDelay maximal delay in milliseconds of committing transaction.  Please notice, that Perst could 
     * not force other threads to commit their cooperative transactions when this timeout is expired. It will only
     * block new cooperative transactions to make it possible to current transaction to complete their work.
     * If <code>maxDelay</code> is 0, current thread will be blocked until all other cooperative trasnaction are also finished
     * and changhes will be committed to the database.
     */
    public void endThreadTransaction(int maxDelay);
   
    /**
     * Rollback per-thread transaction. It is safe to use this method only for exclusive transactions.
     * In case of cooperative transactions, this method rollback results of all transactions.
     */
    public void rollbackThreadTransaction();

    /**
     * Create JSQL query. JSQL is object oriented subset of SQL allowing
     * to specify arbitrary prdicates for selecting members of Perst collections
     * @return created query object
     */     
    public <T> Query<T> createQuery();

    /**
     * Create new peristent list. Implementation of this list is based on B-Tree so it can efficiently
     * handle large number of objects but in case of very small list memory overhead is too high.
     * @return persistent object implementing list
     */
    public <T> IPersistentList<T> createList();

    /**
     * Create new scalable list of persistent objects.
     * This container can effciently handle small lists as well as large lists
     * When number of memers is small, Link class is used to store set members. 
     * When number of members exceeds some threshold, PersistentList (based on B-Tree)
     * is used instead.
     * @return scalable set implementation
     */
    public <T> IPersistentList<T> createScalableList();

    /**
     * Create new scalable list of persistent objects.
     * This container can effciently handle small lists as well as large lists
     * When number of memers is small, Link class is used to store set members. 
     * When number of members exceeds some threshold, PersistentList (based on B-Tree)
     * is used instead.
     * @param initialSize initial allocated size of the list
     * @return scalable set implementation
     */
    public <T> IPersistentList<T> createScalableList(int initialSize);

    /**
     * Create scalable persistent map.
     * This container can effciently handle both small and large number of members.
     * For small maps, implementation  uses sorted array. For large maps - B-Tree.
     * @param keyType map key type
     * @return scalable set implementation
     */
    public <K extends Comparable, V> IPersistentMap<K,V> createMap(Class keyType);

    /**
     * Create scalable persistent map.
     * This container can effciently handle both small and large number of members.
     * For small maps, implementation  uses sorted array. For large maps - B-Tree.
     * @param keyType map key type
     * @param initialSize initial allocated size of the list
     * @return scalable set implementation
     */
    public <K extends Comparable, V> IPersistentMap<K,V> createMap(Class keyType, int initialSize);

    /**
     * Create new peristent set. Implementation of this set is based on B-Tree so it can efficiently
     * handle large number of objects but in case of very small set memory overhead is too high.
     * @return persistent object implementing set
     */
    public <T> IPersistentSet<T> createSet();

    /**
     * Create new scalable set references to persistent objects.
     * This container can effciently store small number of references as well as very large
     * number references. When number of memers is small, Link class is used to store 
     * set members. When number of members exceeds some threshold, PersistentSet (based on B-Tree)
     * is used instead.
     * @return scalable set implementation
     */
    public <T> IPersistentSet<T> createScalableSet();

    /**
     * Create new scalable set references to persistent objects.
     * This container can effciently store small number of references as well as very large
     * number references. When number of memers is small, Link class is used to store 
     * set members. When number of members exceeds some threshold, PersistentSet (based on B-Tree)
     * is used instead.
     * @param initialSize initial size of the set
     * @return scalable set implementation
     */
    public <T> IPersistentSet<T> createScalableSet(int initialSize);

    /**
     * Create new index
     * @param type type of the index key (you should path here <code>String.class</code>, 
     * <code>int.class</code>, ...)
     * @param unique whether index is unique (duplicate value of keys are not allowed)
     * @return persistent object implementing index
     * @exception StorageError(StorageError.UNSUPPORTED_INDEX_TYPE) exception if 
     * specified key type is not supported by implementation.
     */
    public <T> Index<T> createIndex(Class type, boolean unique);

    /**
     * Create new compound index
     * @param types types of the index compound key components 
     * @param unique whether index is unique (duplicate value of keys are not allowed)
     * @return persistent object implementing compound index
     * @exception StorageError(StorageError.UNSUPPORTED_INDEX_TYPE) exception if 
     * specified key type is not supported by implementation.
     */
    public <T> Index<T> createIndex(Class[] types, boolean unique);

    /**
     * Create new multidimensional index
     * @param comparator multidimensinal comparator
     * @return multidimensional index
     */
    public <T> MultidimensionalIndex<T> createMultidimensionalIndex(MultidimensionalComparator<T> comparator);

    /**
     * Create new multidimensional index for specified fields of the class 
     * @param type class of objects included in this index
     * @param fieldNames name of the fields which are treated as index dimensions, 
     * if null then all declared fields of the class are used.
     * @param treateZeroAsUndefinedValue if value of scalar field in QBE object is 0 (default value) then assume 
     * that condition is not defined for this field
     * @return multidimensional index
     */
    public <T> MultidimensionalIndex<T> createMultidimensionalIndex(Class type, String[] fieldNames, boolean treateZeroAsUndefinedValue);

    /**
     * Create new think index (index with large number of duplicated keys)
     * @param type type of the index key (you should path here <code>String.class</code>, 
     * <code>int.class</code>, ...)
     * @return persistent object implementing index
     * @exception StorageError(StorageError.UNSUPPORTED_INDEX_TYPE) exception if 
     * specified key type is not supported by implementation.
     */
    public <T> Index<T> createThickIndex(Class type);

    /**
     * Create new bit index. Bit index is used to select object 
     * with specified set of (boolean) properties.
     * @return persistent object implementing bit index
     */
    public <T> BitIndex<T> createBitIndex();

    /**
     * Create new field index
     * @param type objects of which type (or derived from which type) will be included in the index
     * @param fieldName name of the index field. Field with such name should be present in specified class <code>type</code>
     * @param unique whether index is unique (duplicate value of keys are not allowed)
     * @return persistent object implementing field index
     * @exception StorageError(StorageError.INDEXED_FIELD_NOT_FOUND) if there is no such field in specified class,<BR> 
     * StorageError(StorageError.UNSUPPORTED_INDEX_TYPE) exception if type of specified field is not supported by implementation
     */
    public <T> FieldIndex<T> createFieldIndex(Class type, String fieldName, boolean unique);

    /**
     * Create new field index
     * @param type objects of which type (or derived from which type) will be included in the index
     * @param fieldName name of the index field. Field with such name should be present in specified class <code>type</code>
     * @param unique whether index is unique (duplicate value of keys are not allowed)
     * @return persistent object implementing field index
     * @exception StorageError(StorageError.INDEXED_FIELD_NOT_FOUND) if there is no such field in specified class,<BR> 
     * StorageError(StorageError.UNSUPPORTED_INDEX_TYPE) exception if type of specified field is not supported by implementation
     */
    public <T> FieldIndex<T> createFieldIndex(Class type, String fieldName, boolean unique, boolean caseInsensitive);

    /**
     * Create new mutlifield index
     * @param type objects of which type (or derived from which type) will be included in the index
     * @param fieldNames names of the index fields. Fields with such name should be present in specified class <code>type</code>
     * @param unique whether index is unique (duplicate value of keys are not allowed)
     * @return persistent object implementing field index
     * @exception StorageError(StorageError.INDEXED_FIELD_NOT_FOUND) if there is no such field in specified class,<BR> 
     * StorageError(StorageError.UNSUPPORTED_INDEX_TYPE) exception if type of specified field is not supported by implementation
     */
    public <T> FieldIndex<T> createFieldIndex(Class type, String[] fieldNames, boolean unique);

    /**
     * Create new mutlifield index
     * @param type objects of which type (or derived from which type) will be included in the index
     * @param fieldNames names of the index fields. Fields with such name should be present in specified class <code>type</code>
     * @param unique whether index is unique (duplicate value of keys are not allowed)
     * @param caseInsensitive whether index is case insensitive (ignored for non-string keys)
     * @return persistent object implementing field index
     * @exception StorageError(StorageError.INDEXED_FIELD_NOT_FOUND) if there is no such field in specified class,<BR> 
     * StorageError(StorageError.UNSUPPORTED_INDEX_TYPE) exception if type of specified field is not supported by implementation
     */
    public <T> FieldIndex<T> createFieldIndex(Class type, String[] fieldNames, boolean unique, boolean caseInsensitive);

    /**
     * Create new index optimized for access by element position.
     * @param type type of the index key (you should path here <code>String.class</code>, 
     * <code>int.class</code>, ...)
     * @param unique whether index is unique (duplicate value of keys are not allowed)
     * @return persistent object implementing index
     * @exception StorageError(StorageError.UNSUPPORTED_INDEX_TYPE) exception if 
     * specified key type is not supported by implementation.
     */
    public <T> Index<T> createRandomAccessIndex(Class type, boolean unique);

    /**
     * Create new compound index optimized for access by element position.
     * @param types types of the index compound key components 
     * @param unique whether index is unique (duplicate value of keys are not allowed)
     * @return persistent object implementing compound index
     * @exception StorageError(StorageError.UNSUPPORTED_INDEX_TYPE) exception if 
     * specified key type is not supported by implementation.
     */
    public <T> Index<T> createRandomAccessIndex(Class[] types, boolean unique);

    /**
     * Create new field index optimized for access by element position.
     * @param type objects of which type (or derived from which type) will be included in the index
     * @param fieldName name of the index field. Field with such name should be present in specified class <code>type</code>
     * @param unique whether index is unique (duplicate value of keys are not allowed)
     * @return persistent object implementing field index
     * @exception StorageError(StorageError.INDEXED_FIELD_NOT_FOUND) if there is no such field in specified class,<BR> 
     * StorageError(StorageError.UNSUPPORTED_INDEX_TYPE) exception if type of specified field is not supported by implementation
     */
    public <T> FieldIndex<T> createRandomAccessFieldIndex(Class type, String fieldName, boolean unique);

    /**
     * Create new field index optimized for access by element position.
     * @param type objects of which type (or derived from which type) will be included in the index
     * @param fieldName name of the index field. Field with such name should be present in specified class <code>type</code>
     * @param unique whether index is unique (duplicate value of keys are not allowed)
     * @param caseInsensitive whether index is case insensitive (ignored for non-string keys)
     * @return persistent object implementing field index
     * @exception StorageError(StorageError.INDEXED_FIELD_NOT_FOUND) if there is no such field in specified class,<BR> 
     * StorageError(StorageError.UNSUPPORTED_INDEX_TYPE) exception if type of specified field is not supported by implementation
     */
    public <T> FieldIndex<T> createRandomAccessFieldIndex(Class type, String fieldName, boolean unique, boolean caseInsensitive);

    /**
     * Create new mutlifield index optimized for access by element position.
     * @param type objects of which type (or derived from which type) will be included in the index
     * @param fieldNames names of the index fields. Fields with such name should be present in specified class <code>type</code>
     * @param unique whether index is unique (duplicate value of keys are not allowed)
     * @return persistent object implementing field index
     * @exception StorageError(StorageError.INDEXED_FIELD_NOT_FOUND) if there is no such field in specified class,<BR> 
     * StorageError(StorageError.UNSUPPORTED_INDEX_TYPE) exception if type of specified field is not supported by implementation
     */
    public <T> FieldIndex<T> createRandomAccessFieldIndex(Class type, String[] fieldNames, boolean unique);

    /**
     * Create new mutlifield index optimized for access by element position.
     * @param type objects of which type (or derived from which type) will be included in the index
     * @param fieldNames names of the index fields. Fields with such name should be present in specified class <code>type</code>
     * @param unique whether index is unique (duplicate value of keys are not allowed)
     * @param caseInsensitive whether index is case insensitive (ignored for non-string keys)
     * @return persistent object implementing field index
     * @exception StorageError(StorageError.INDEXED_FIELD_NOT_FOUND) if there is no such field in specified class,<BR> 
     * StorageError(StorageError.UNSUPPORTED_INDEX_TYPE) exception if type of specified field is not supported by implementation
     */
    public <T> FieldIndex<T> createRandomAccessFieldIndex(Class type, String[] fieldNames, boolean unique, boolean caseInsensitive);

     /**
     * Create new spatial index with integer coordinates
     * @return persistent object implementing spatial index
     */
    public <T> SpatialIndex<T> createSpatialIndex();

    /**
     * Create new R2 spatial index 
     * @return persistent object implementing spatial index
     */
    public<T>  SpatialIndexR2<T> createSpatialIndexR2();

    /**
     * Create new sorted collection
     * @param comparator comparator class specifying order in the collection
     * @param unique whether index is collection (members with the same key value are not allowed)
     * @return persistent object implementing sorted collection
     */
    public <T> SortedCollection<T> createSortedCollection(PersistentComparator<T> comparator, boolean unique);

    /**
     * Create one-to-many link.
     * @return new empty link, new members can be added to the link later.
     */
    public <T> Link<T> createLink();
    
    /**
     * Create one-to-many link with specified initialy alloced size.
     * @param initialSize initial size of array
     * @return new empty link, new members can be added to the link later.
     */
    public <T> Link<T> createLink(int initialSize);
    
    /**
     * Create relation object. Unlike link which represent embedded relation and stored
     * inside owner object, this Relation object is standalone persisitent object
     * containing references to owner and members of the relation
     * @param owner owner of the relation
     * @return object representing empty relation (relation with specified owner and no members), 
     * new members can be added to the link later.
     */
    public <M, O> Relation<M,O> createRelation(O owner);


    /**
     * Create new BLOB. Create object for storing large binary data.
     * @return empty BLOB
     */
    public Blob createBlob();

    /**
     * Create new random access BLOB. Create file-like object providing efficient random poistion access.
     * @return empty BLOB
     */
    public Blob createRandomAccessBlob();

    /**
     * Create new time series object. 
     * @param blockClass class derived from TimeSeries.Block
     * @param maxBlockTimeInterval maximal difference in milliseconds between timestamps 
     * of the first and the last elements in a block. 
     * If value of this parameter is too small, then most blocks will contains less elements 
     * than preallocated. 
     * If it is too large, then searching of block will be inefficient, because index search 
     * will select a lot of extra blocks which do not contain any element from the 
     * specified range.
     * Usually the value of this parameter should be set as
     * (number of elements in block)*(tick interval)*2. 
     * Coefficient 2 here is used to compencate possible holes in time series.
     * For example, if we collect stocks data, we will have data only for working hours.
     * If number of element in block is 100, time series period is 1 day, then
     * value of maxBlockTimeInterval can be set as 100*(24*60*60*1000)*2
     * @return new empty time series
     */
    public <T extends TimeSeries.Tick> TimeSeries<T> createTimeSeries(Class blockClass, long maxBlockTimeInterval);
   
    /**
     * Create PATRICIA trie (Practical Algorithm To Retrieve Information Coded In Alphanumeric)
     * Tries are a kind of tree where each node holds a common part of one or more keys. 
     * PATRICIA trie is one of the many existing variants of the trie, which adds path compression 
     * by grouping common sequences of nodes together.<BR>
     * This structure provides a very efficient way of storing values while maintaining the lookup time 
     * for a key in O(N) in the worst case, where N is the length of the longest key. 
     * This structure has it's main use in IP routing software, but can provide an interesting alternative 
     * to other structures such as hashtables when memory space is of concern.
     * @return created PATRICIA trie
     */
    public <T> PatriciaTrie<T> createPatriciaTrie();

    /**
     * Create full text search index
     * @param helper helper class which provides method for scanning, stemming and tuning query
     * @return full text search index
     */
    public FullTextIndex createFullTextIndex(FullTextSearchHelper helper);

    /**
     * Create full text search index with default helper
     * @return full text search index
     */
    public FullTextIndex createFullTextIndex();


    /**
     * Commit transaction (if needed) and close the storage
     */
    public void close();

    /**
     * Set threshold for initiation of garbage collection. By default garbage collection is disable (threshold is set to
     * Long.MAX_VALUE). If it is set to the value different from Long.MAX_VALUE, GC will be started each time when
     * delta between total size of allocated and deallocated objects exceeds specified threashold OR
     * after reaching end of allocation bitmap in allocator. 
     * @param allocatedDelta delta between total size of allocated and deallocated object since last GC 
     * or storage opening 
     */
    public void setGcThreshold(long allocatedDelta);

    /**
     * Explicit start of garbage collector
     * @return number of collected (deallocated) objects
     */
    public int gc();

    /**
     * Export database in XML format 
     * @param writer writer for generated XML document
     */
    public void exportXML(java.io.Writer writer) throws java.io.IOException;

    /**
     * Import data from XML file
     * @param reader XML document reader
     */
    public void importXML(java.io.Reader reader) throws XMLImportException;

    /**
     * Retrieve object by OID. This method should be used with care because
     * if object is deallocated, its OID can be reused. In this case
     * getObjectByOID will return reference to the new object with may be
     * different type.
     * @param oid object oid
     * @return reference to the object with specified OID
     */
    public Object getObjectByOID(int oid);

    /**
     * Explicitely make object peristent. Usually objects are made persistent
     * implicitlely using "persistency on reachability apporach", but this
     * method allows to do it explicitly. If object is already persistent, execution of
     * this method has no effect.
     * @param obj object to be made persistent
     * @return OID assigned to the object  
     */
    public int makePersistent(Object obj);

    /**
     * Set database property. This method should be invoked before opening database. 
     * Currently the following boolean properties are supported:
     * <TABLE><TR><TH>Property name</TH><TH>Parameter type</TH><TH>Default value</TH><TH>Description</TH></TR>
     * <TR><TD><code>perst.implicit.values</code></TD><TD>Boolean</TD><TD>false</TD>
     * <TD>Treate any class not derived from IPersistent as <i>value</i>. 
     * This object will be embedded inside persistent object containing reference to this object.
     * If this object is referenced from N persistent object, N instances of this object
     * will be stored in the database and after loading there will be N instances in memory. 
     * As well as persistent capable classes, value classes should have default constructor (constructor
     * with empty list of parameters) or has no constructors at all. For example <code>Integer</code>
     * class can not be stored as value in PERST because it has no such constructor. In this case 
     * serialization mechanism can be used (see below)
     * </TD></TR>
     * <TR><TD><code>perst.serialize.transient.objects</code></TD><TD>Boolean</TD><TD>false</TD>
     * <TD>Serialize any class not derived from IPersistent or IValue using standard Java serialization
     * mechanism. Packed object closure is stored in database as byte array. Latter the same mechanism is used
     * to unpack the objects. To be able to use this mechanism object and all objects referenced from it
     * should implement <code>java.io.Serializable</code> interface and should not contain references
     * to persistent objects. If such object is referenced from N persistent object, N instances of this object
     * will be stored in the database and after loading there will be N instances in memory.
     * </TD></TR>
     * <TR><TD><code>perst.object.cache.init.size</code></TD><TD>Integer</TD><TD>1319</TD>
     * <TD>Initial size of object cache
     * </TD></TR>
     * <TR><TD><code>perst.object.cache.kind</code></TD><TD>String</TD><TD>"lru"</TD>
     * <TD>Kind of object cache. The following values are supported:
     * "strong", "weak", "soft",  "pinned", "lru". <B>Strong</B> cache uses strong (normal) 
     * references to refer persistent objects. Thus none of loaded persistent objects
     * can be deallocated by GC. <B>Weak</B> cache usea weak references and
     * soft cache - <B>soft</B> references. The main difference between soft and weak references is
     * that garbage collector is not required to remove soft referenced objects immediately
     * when object is detected to be <i>soft referenced</i>, so it may improve caching of objects. 
     * But it also may increase amount of memory
     * used  by application, and as far as persistent object requires finalization
     * it can cause memory overflow even though garbage collector is required
     * to clear all soft references before throwing OutOfMemoryException.<br>
     * But Java specification says nothing about the policy used by GC for soft references
     * (except the rule mentioned above). Unlike it <B>lru</B> cache provide determined behavior, 
     * pinning most recently used objects in memory. Number of pinned objects is determined 
     * for lru cache by <code>perst.object.index.init.size</code> parameter (it can be 0).<br>
     * Pinned object cache pin in memory all modified objects while using weak referenced for 
     * non-modified objects. This kind of cache eliminate need in finalization mechanism - all modified
     * objects are kept in memory and are flushed to the disk only at the end of transaction. 
     * So the size of transaction is limited by amount of main memory. Non-modified objects are accessed only 
     * through weak references so them are not protected from GC and can be thrown away.    
     * </TD></TR>
     * <TR><TD><code>perst.object.index.init.size</code></TD><TD>Integer</TD><TD>1024</TD>
     * <TD>Initial size of object index (specifying large value increase initial size of database, but reduce
     * number of index reallocations)
     * </TD></TR>
     * <TR><TD><code>perst.extension.quantum</code></TD><TD>Long</TD><TD>1048576</TD>
     * <TD>Object allocation bitmap extension quantum. Memory is allocate by scanning bitmap. If there is no
     * large enough hole, then database is extended by the value of dbDefaultExtensionQuantum. 
     * This parameter should not be smaller than 64Kb.
     * </TD></TR>
     * <TR><TD><code>perst.gc.threshold</code></TD><TD>Long</TD><TD>Long.MAX_VALUE</TD>
     * <TD>Threshold for initiation of garbage collection. 
     * If it is set to the value different from Long.MAX_VALUE, GC will be started each time 
     * when delta between total size of allocated and deallocated objects exceeds specified threashold OR
     * after reaching end of allocation bitmap in allocator.
     * </TD></TR>
     * <TR><TD><code>perst.lock.file</code></TD><TD>Boolean</TD><TD>false</TD>
     * <TD>Lock database file to prevent concurrent access to the database by 
     * more than one application.
     * </TD></TR>
     * <TR><TD><code>perst.file.readonly</code></TD><TD>Boolean</TD><TD>false</TD>
     * <TD>Database file should be opened in read-only mode.
     * </TD></TR>
     * <TR><TD><code>perst.file.noflush</code></TD><TD>Boolean</TD><TD>false</TD>
     * <TD>Do not flush file during transaction commit. It will greatly increase performance because
     * eliminate synchronous write to the disk (when program has to wait until all changed
     * are actually written to the disk). But it can cause database corruption in case of 
     * OS or power failure (but abnormal termination of application itself should not cause
     * the problem, because all data which were written to the file, but is not yet saved to the disk is 
     * stored in OS file buffers and sooner or later them will be written to the disk)
     * </TD></TR>
     * <TR><TD><code>perst.alternative.btree</code></TD><TD>Boolean</TD><TD>false</TD>
     * <TD>Use aternative implementation of B-Tree (not using direct access to database
     * file pages). This implementation should be used in case of serialized per thread transctions.
     * New implementation of B-Tree will be used instead of old implementation
     * if "perst.alternative.btree" property is set. New B-Tree has incompatible format with 
     * old B-Tree, so you could not use old database or XML export file with new indices. 
     * Alternative B-Tree is needed to provide serializable transaction (old one could not be used).
     * Also it provides better performance (about 3 times comaring with old implementation) because
     * of object caching. And B-Tree supports keys of user defined types. 
     * </TD></TR>
     * <TR><TD><code>perst.background.gc</code></TD><TD>Boolean</TD><TD>false</TD>
     * <TD>Perform garbage collection in separate thread without blocking the main application.
     * </TD></TR>
     * <TR><TD><code>perst.string.encoding</code></TD><TD>String</TD><TD>null</TD>
     * <TD>Specifies encoding of storing strings in the database. By default Perst stores 
     * strings as sequence of chars (two bytes per char). If all strings in application are in 
     * the same language, then using encoding  can signifficantly reduce space needed
     * to store string (about two times). But please notice, that this option has influence
     * on all strings  stored in database. So if you already have some data in the storage
     * and then change encoding, then it will cause database crash.
     * </TD></TR>
     * <TR><TD><code>perst.replication.ack</code></TD><TD>Boolean</TD><TD>false</TD>
     * <TD>Request acknowledgement from slave that it receives all data before transaction
     * commit. If this option is not set, then replication master node just writes
     * data to the socket not warring whether it reaches slave node or not.
     * When this option is set to true, master not will wait during each transaction commit acknowledgement
     * from slave node. Please notice that this option should be either set or not set both
     * at slave and master node. If it is set only on one of this nodes then behavior of
     * the system is unpredicted. This option can be used both in synchronous and asynchronous replication
     * mode. The only difference is that in first case main application thread will be blocked waiting
     * for acknowledgment, while in the asynchronous mode special replication thread will be blocked
     * allowing thread performing commit to proceed.
     * </TD></TR>
     * <TR><TD><code>perst.concurrent.iterator</code></TD><TD>Boolean</TD><TD>false</TD>
     * <TD>By default iterator will throw ConcurrentModificationException if iterated collection
     * was changed outside iterator, when the value of this property is true then iterator will 
     * try to restore current position and continue iteration
     * </TD></TR>
     * <TR><TD><code>perst.slave.connection.timeout</code></TD><TD>Integer</TD><TD>60</TD>
     * <TD>Timeout in seconds during which mastr node will try to establish connection with slave node. 
     * If connection can not be established within specified time, then master will not perform 
     * replication to this slave node
     * </TD></TR>
     * <TR><TD><code>perst.force.store</code></TD><TD>Boolean</TD><TD>true</TD>
     * <TD>When the value of this parameter is true Storage.makePersistent method
     * cause immediate storing of object in the storage, otherwise object is assigned OID and is marked 
     * as modified. Storage.makePersistent method is mostly used when object is inserted in B-Tree.
     * If application put in index object referencing a large number of other objects which also has to 
     * be made persistent, then marking object as modified instead of immediate storing may cause
     * memory overflow because garbage collector and finalization threads will store objects
     * with less speed than application creates new ones.
     * But if object will be updated after been placed in B-Tree, then immediate store will just cause
     * cause extra overhead, because object has to be stored twice. 
     * </TD></TR>
     * <TR><TD><code>perst.page.pool.lru.limit</code></TD><TD>Long</TD><TD>1L << 60</TD>
     * <TD>Set boundary for caching database pages in page pool. 
     * By default Perst is using LRU algorithm for finding candidate for replacement.
     * But for example for BLOBs this strategy is not optimal and fetching BLOB can
     * cause flushing the whole page pool if LRU discipline is used. And with high
     * probability fetched BLOB pages will no be used any more. So it is preferable not
     * to cache BLOB pages at all (throw away such page immediately when it is not used any more).
     * This parameter in conjunction with custom allocator allows to disable caching
     * for BLOB objects. If you set value of "perst.page.lru.scope" property equal
     * to base address of custom allocator (which will be used to allocate BLOBs), 
     * then page containing objects allocated by this allocator will not be cached in page pool.
     * </TD></TR>
     * <TR><TD><code>perst.multiclient.support</code></TD><TD>Boolean</TD><TD>false</TD>
     * <TD>Supports access to the same database file by multiple applications.
     * In this case Perst will use file locking to synchronize access to the database file.
     * An application MUST wrap any access to the database with  beginThreadThreansaction/endThreadTransaction 
     * methods. For read only access use READ_ONLY_TRANSACTION mode and if transaction may modify database then
     * READ_WRITE_TRANSACTION mode should be used.
     * </TD></TR>
     * </TABLE>
     * @param name name of the property
     * @param value value of the property (for boolean properties pass <code>java.lang.Boolean.TRUE</code>
     * and <code>java.lang.Boolean.FALSE</code>
     */
    public void setProperty(String name, Object value);

   /**
     * Set database properties. This method should be invoked before opening database. 
     * For list of supported properties please see <code>setProperty</code> command. 
     * All not recognized properties are ignored.
     */
    public void setProperties(java.util.Properties props);


    /**
     * Get property value.
     * @param name property name
     * @return value of the property previously assigned by setProperty or setProperties method
     * or <code>null</code> if property was not set
     */
    public Object getProperty(String name);

    /**
     * Get all set properties
     * @return all properties set by setProperty or setProperties method
     */
    public java.util.Properties getProperties();
 
    /**
     * Merge results of several index searches. This method efficiently merge selections without loading objects themselve
     * @param selections selections to be merged
     * @return Iterator through merged result
     */
    public Iterator merge(Iterator[] selections);

    
    /**
     * Join results of several index searches. This method efficiently join selections without loading objects themselve
     * @param selections selections to be merged
     * @return Iterator through joineded result
     */
    public Iterator join(Iterator[] selections);
 
    /**
     * Set storage listener.
     * @param listener new storage listener (may be null)
     * @return previous storage listener
     */
    public StorageListener setListener(StorageListener listener);

    /**
     * Get database memory dump. This function returns hashmap which key is classes
     * of stored objects and value - MemoryUsage object which specifies number of instances
     * of particular class in the storage and total size of memory used by these instance.
     * Size of internal database structures (object index,* memory allocation bitmap) is associated with 
     * <code>Storage</code> class. Size of class descriptors  - with <code>java.lang.Class</code> class.
     * <p>This method traverse the storage as garbage collection do - starting from the root object
     * and recursively visiting all reachable objects. So it reports statistic only for visible objects.
     * If total database size is significantly larger than total size of all instances reported
     * by this method, it means that there is garbage in the database. You can explicitly invoke
     * garbage collector in this case.</p> 
     */
    public java.util.HashMap<Class,MemoryUsage> getMemoryDump();
    
    /**
     * Get total size of all allocated objects in the database
     */
    public long getUsedSize();

    /**
     * Get size of the database
     */
    public long getDatabaseSize();

 
    /**
     * Set class loader. This class loader will be used to locate classes for 
     * loaded class descriptors. If class loader is not specified or
     * it did find the class, then <code>Class.forName()</code> method
     * will be used to get class for the specified name.
     * @param loader class loader
     * @return previous class loader or null if not specified
     */
    public ClassLoader setClassLoader(ClassLoader loader);

    /**
     * Get class loader used to locate classes for 
     * loaded class descriptors.
     * @return class loader previously set by <code>setClassLoader</code>
     * method or <code>null</code> if not specified. 
     */
    public ClassLoader getClassLoader();

    /**
     * Register named class loader in the storage. Mechanism of named class loaders
     * allows to store in database association between class and its class loader.
     * All named class loaders should be registered before database open.
     * @param loader registered named class loader
     */
    public void registerClassLoader(INamedClassLoader loader);

    /**
     * Find registered class loaders by name
     * @param name class loader name
     * @return class loader with such name or numm if no class loader is found
     */
    public ClassLoader findClassLoader(String name);

    /**
     * Register custom allocator for specified class. Instances of this and derived classes 
     * will be allocated in the storage using specified allocator. 
     * @param cls class of the persistent object which instances will be allocated using this allocator
     * @param allocator custom allocator
     */
    public void registerCustomAllocator(Class cls, CustomAllocator allocator);

    /**
     * Create bitmap custom allocator
     * @param quantum size in bytes of allocation quantum. Should be power of two.
     * @param base base address for allocator (it should match offset of multifile segment)
     * @param extension size by which space mapped by allocator is extended each time when 
     * no suitable hole is found in bitmap (it should be large enough to improve allocation speed and locality 
     * of references)
     * @param limit maximal size of memory allocated by this allocator (pass Long.MAX_VALUE if you do not 
     * want to limit space)
     * @return created allocator
     */
    public CustomAllocator createBitmapAllocator(int quantum, long base, long extension, long limit);

    /**
     * This method is used internally by Perst to get transaction context associated with current thread.
     * But it can be also used by application to get transaction context, store it in some variable and
     * use in another thread. I will make it possible to share one transaction between multiple threads.
     * @return transaction context associated with current thread
     */     
    public ThreadTransactionContext getTransactionContext();

    /**
     * Associate transaction context with the thread
     * This method can be used by application to share the same transaction between multiple threads
     * @param ctx new transaction context 
     * @return transaction context previously associated with this thread
     */     
    public ThreadTransactionContext setTransactionContext(ThreadTransactionContext ctx);

    /**
     * Set custom serializer used fot packing/unpacking fields of persistent objects which types implemplemet 
     * CustomSerializable interface
     */
    public void setCustomSerializer(CustomSerializer serializer);

    /**
     * Clear database object cache. This method can be used with "strong" object cache to avoid memory overflow.
     * It is no valid to invoke this method when there are some uncommitted changes in the database
     * (some modified objects). Also all variables containing references to persistent object should be reset after
     * invocation of this method - it is not correct to accessed object directly though such variables, objects
     * has to be reloaded from the storage
     */
    public void clearObjectCache();

    /**
     * Get version of database format for this database. When new database is created it is
     * always assigned the current database format version
     * @return databasse format version
     */
    public int getDatabaseFormatVersion();


    /**
     * Store object in storage
     * @param obj stored object
     */
    public void store(Object obj);

    /**
     * Mark object as been modified     
     * @param obj modified object
     */
    public void modify(Object obj);

    /**
     * Load raw object
     * @param obj loaded object
     */
    public void load(Object obj);

    /**
     * Deallocaste object
     * @param obj deallocated object
     */
    public void deallocate(Object obj);

    /**
     * Get object identifier
     * @param obj inspected object
     */
    public int getOid(Object obj);

    // Internal methods

    public void deallocateObject(Object obj);

    public void storeObject(Object obj);

    public void storeFinalizedObject(Object obj);

    public void modifyObject(Object obj);

    public void loadObject(Object obj);

    public boolean lockObject(Object obj);

    public void throwObject(Object obj);
}


