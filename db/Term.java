/**
 * @author j16sdiz (1024D/75494252)
 */
package plugins.XMLSpider.db;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Set;

import plugins.XMLSpider.org.garret.perst.IPersistentSet;
import plugins.XMLSpider.org.garret.perst.Persistent;
import plugins.XMLSpider.org.garret.perst.Storage;

public class Term extends Persistent {
	/** MD5 of the term */
	String md5;
	/** Term */
	String word;
	
	/** Pages containing this Term */
	IPersistentSet<Page> pageSet;

	public Term(String word, Storage storage) {
		this.word = word;
		md5 = MD5(word);
		pageSet = storage.<Page> createScalableSet();
		
		storage.makePersistent(this);
	}

	public Term() {
	}
	
	public boolean addPage(Page page) {
		return pageSet.add(page);
	}

	public boolean removePage(Page page) {
		return pageSet.remove(page);
	}

	public Set<Page> getPages() {
		return pageSet;
	}

	public String getWord() {
		return word;
	}
	
	public String getMD5() {
		return md5;
	}
	
	@Override
	public boolean equals(Object o) {
		if (o == null)
			return false;
		if (getClass() != o.getClass())
			return false;
		Term t = (Term) o;
		return md5.equals(t.md5) && word.equals(t.word);
	}

	@Override
	public int hashCode() {
		return md5.hashCode() ^ word.hashCode();
	}
	
	/*
	 * calculate the md5 for a given string
	 */
	public static String MD5(String text) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] md5hash = new byte[32];
			byte[] b = text.getBytes("UTF-8");
			md.update(b, 0, b.length);
			md5hash = md.digest();
			return convertToHex(md5hash);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("UTF-8 not supported", e);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("MD5 not supported", e);
		}
	}

	public static String convertToHex(byte[] data) {
		StringBuilder buf = new StringBuilder();
		for (int i = 0; i < data.length; i++) {
			int halfbyte = (data[i] >>> 4) & 0x0F;
			int two_halfs = 0;
			do {
				if ((0 <= halfbyte) && (halfbyte <= 9))
					buf.append((char) ('0' + halfbyte));
				else
					buf.append((char) ('a' + (halfbyte - 10)));
				halfbyte = data[i] & 0x0F;
			} while (two_halfs++ < 1);
		}
		return buf.toString();
	}
}