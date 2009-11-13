
package plugins.XMLSpider;

import freenet.pluginmanager.FredPluginTalker;
import freenet.pluginmanager.PluginNotFoundException;
import freenet.pluginmanager.PluginRespirator;
import freenet.pluginmanager.PluginTalker;
import freenet.support.Logger;
import freenet.support.SimpleFieldSet;
import freenet.support.api.Bucket;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.TreeMap;
import plugins.Library.index.TermEntryWriter;
import plugins.Library.index.TermPageEntry;

/**
 * Buffer which stores TermPageEntrys as they are found by the Spider. When the
 * buffer's estimated size gets up to bufferMax, the buffer is serialized into a
 * Bucket and sent to the Library
 * 
 *
 * @author MikeB
 */
public class LibraryBuffer implements FredPluginTalker {
	private PluginRespirator pr;

	private TreeMap<TermPageEntry, TermPageEntry> termPageBuffer = new TreeMap();

	private int bufferUsageEstimate = 0;
	private final int bufferMax = 2000000;



	/**
	 * Increments the estimate by specified amount and if the estimate is above the max send the buffer
	 * @param increment
	 */
	private void increaseEstimate(int increment) {
		bufferUsageEstimate += increment;
		if (bufferUsageEstimate > bufferMax)
			sendBuffer();
	}
	

	LibraryBuffer(PluginRespirator pr) {
		this.pr = pr;
	}

	/**
	 * Takes a TermPageEntry and either returns a TPE of the same term & page
	 * from the buffer or adds the TPE to the buffer and returns it.
	 *
	 * @param newTPE
	 * @return
	 */
	private synchronized TermPageEntry get(TermPageEntry newTPE) {
		TermPageEntry exTPE = termPageBuffer.get(newTPE);
		if(exTPE==null) {	// TPE is new
			increaseEstimate(newTPE.sizeEstimate());
			return newTPE;
		} else
			return exTPE;
	}

	/**
	 * Set the title of the
	 * @param termPageEntry
	 * @param s
	 */
	void setTitle(TermPageEntry termPageEntry, String s) {
		get(termPageEntry).title = s;
	}

	/**
	 * Puts a term position in the TermPageEntry and increments the bufferUsageEstimate
	 * @param tp
	 * @param position
	 */
	synchronized void addPos(TermPageEntry tp, int position) {
		try{
			//Logger.normal(this, "length : "+bufferUsageEstimate+", adding to "+tp);
			get(tp).pos.put(position, null);
			//Logger.normal(this, "length : "+bufferUsageEstimate+", increasing length "+tp);
			increaseEstimate(4);
		}catch(Exception e){
			Logger.error(this, "Exception adding", e);
		}
	}


	/**
	 * Emptys the buffer into a bucket and sends it to the Library plugin with the command "pushBuffer"
	 *
	 * FIXME : I think there is something wrong with the way it writes to the bucket, I may be using the wrong kind of buffer
	 */
	private void sendBuffer() {
		try {
			PluginTalker libraryTalker = pr.getPluginTalker(this, "plugins.Library.Main", "SpiderBuffer");
			Logger.normal(this, "Sending buffer of estimated size "+bufferUsageEstimate+" bytes to Library");
			SimpleFieldSet sfs = new SimpleFieldSet(true);
			sfs.putSingle("command", "pushBuffer");
			Bucket bucket = pr.getNode().clientCore.tempBucketFactory.makeBucket(3000000);
			Collection<TermPageEntry> buffer2;
			synchronized (this) {
				buffer2 = termPageBuffer.values();
				termPageBuffer = new TreeMap();
				bufferUsageEstimate = 0;
			}
			OutputStream os = bucket.getOutputStream();
			for (TermPageEntry termPageEntry : buffer2) {
				TermEntryWriter.getInstance().writeObject(termPageEntry, os);
			}
			os.close();
			bucket.setReadOnly();
			libraryTalker.send(sfs, bucket);
			Logger.normal(this, "Buffer successfully sent to Library, size = "+bucket.size());
		} catch (IOException ex) {
			Logger.error(this, "Could not make bucket to transfer buffer", ex);
		} catch (PluginNotFoundException ex) {
			Logger.error(this, "Couldn't connect buffer to Library", ex);
		}
	}

	public void onReply(String pluginname, String indentifier, SimpleFieldSet params, Bucket data) {
		// TODO maybe
	}

}
