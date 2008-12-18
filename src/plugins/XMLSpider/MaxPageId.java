/**
 * @author j16sdiz (1024D/75494252)
 */
package plugins.XMLSpider;

class MaxPageId {
	volatile long v;

	MaxPageId() {
	}

	MaxPageId(long v) {
		this.v = v;
	}
	
	synchronized long incrementAndGet() {
		return ++v;
	}
}