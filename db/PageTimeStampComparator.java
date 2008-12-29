/**
 * 
 */
package plugins.XMLSpider.db;

import plugins.XMLSpider.org.garret.perst.PersistentComparator;

final class PageTimeStampComparator extends PersistentComparator<Page> {
    @Override
    public int compareMemberWithKey(Page p1, Object key) {
    	if (key instanceof Page)
    		return compareMembers(p1, (Page) key);
    	else
    		return 0;
    }

    @Override
    public int compareMembers(Page p1, Page p2) {
    	if (p1.lastChange < p2.lastChange)
    		return -1;
    	if (p1.lastChange > p2.lastChange)
    		return 1;
    	return 0;
    }
}