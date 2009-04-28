package plugins.XMLSpider.org.garret.perst;

import java.lang.annotation.*;

/**
  * Annotation for marking indexable fields used by Database class to create table descriptors. 
  * Indices can be unique or allow duplicates.
  * If index is marked as unique and during transaction commit it is find out that there is already some other object
  * with this key, NotUniqueException will be thrown
  * Case insensitive attribute is meaningful only for string keys and if set cause ignoring case
  * of key values.
  */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Indexable {
    boolean unique() default false;
    boolean caseInsensitive() default false;
}