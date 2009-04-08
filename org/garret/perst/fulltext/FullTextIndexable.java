package plugins.XMLSpider.org.garret.perst.fulltext;

import java.lang.annotation.*;

/**
  * Annotation for marking full text indexable fields used by Database class to create table descriptors. 
  */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface FullTextIndexable {
}