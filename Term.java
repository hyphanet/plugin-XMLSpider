/**
 * @author j16sdiz (1024D/75494252)
 */
package plugins.XMLSpider;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

class Term {
	/** MD5 of the term */
	String md5;
	/** Term */
	String word;

	public Term(String word) {
		this.word = word;
		md5 = MD5(word);
	}

	public Term() {
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