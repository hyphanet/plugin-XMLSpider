package plugins.XMLSpider.org.garret.perst.fulltext;

import java.io.Reader;

import plugins.XMLSpider.org.garret.perst.IPersistent;

/**
 * Interface for classes which are able to extract text and its language themselves.
 */
public interface FullTextSearchable extends IPersistent
{
    /**
     * Get document text
     */
    Reader getText();

    /**
     * Get document language (null if unknown)
     */
    String getLanguage();
}