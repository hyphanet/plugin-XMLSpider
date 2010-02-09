package plugins.XMLSpider;

import java.lang.Character.UnicodeBlock;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Locale;

/**
 * Search Tokenizer
 * 
 * Normalize and tokenize text blocks into words (for latin scripts),
 * or single-double-tokens (for CJK). 
 * 
 * @author SDiZ <sdiz+freenet@gmail.com>
 */
class SearchTokenizer implements Iterable<String>, Iterator<String> {
	private ArrayList<Mode> mode;
	private ArrayList<String> segments;
	private int nextPos;

	private Iterator<String> cjkTokenizer;

	enum Mode {
		UNDEF, LATIN, CJK
	};

	SearchTokenizer(String text) {
		// normalize
		text = normalize(text);

		// split code points, filter for letter or digit
		final int length = text.length();
		segments = new ArrayList<String>();
		mode = new ArrayList<Mode>();

		Mode curMode = Mode.UNDEF;

		StringBuilder sb = new StringBuilder();
		for (int offset = 0; offset < length;) {
			final int codepoint = text.codePointAt(offset);
			offset += Character.charCount(codepoint);

			if (Character.isLetterOrDigit(codepoint)) {
				boolean isCJK = isCJK(codepoint);
				boolean isNum = Character.isDigit(codepoint);

				// add seperator across CJK/latin margin
				if (isCJK) {
					if (curMode == Mode.LATIN && sb.length() != 0) {
						segments.add(sb.toString());
						mode.add(curMode);
						sb = new StringBuilder();
					}
					curMode = Mode.CJK;
				} else if (!isNum) {
					if (curMode == Mode.CJK && sb.length() != 0) {
						segments.add(sb.toString());
						mode.add(curMode);
						sb = new StringBuilder();
					}
					curMode = Mode.LATIN;
				}

				sb.append(Character.toChars(codepoint));
			} else if (sb.length() != 0) {
				// last code point is not 0, add a separator
				segments.add(sb.toString());
				mode.add(curMode);
				curMode = Mode.UNDEF;
				sb = new StringBuilder();
			}
		}

		if (sb.length() != 0) {
			segments.add(sb.toString());
			mode.add(curMode);
		}
	}

	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append("->[");

		Iterator<String> it = segments.iterator();
		if (it.hasNext())
			str.append(it.next());
		while (it.hasNext()) {
			str.append(',');
			str.append(it.next());
		}
		str.append("]");

		for (String s : this) {
			str.append(';');
			str.append(s);
		}

		return str.toString();
	}

	public Iterator<String> iterator() {
		return this;
	}

	public boolean hasNext() {
		return (cjkTokenizer != null && cjkTokenizer.hasNext()) ||
			(nextPos < segments.size());
	}

	public String next() {
		if (cjkTokenizer != null) {
			if (cjkTokenizer.hasNext())
				return cjkTokenizer.next();
			cjkTokenizer = null;
		}

		Mode curMode = mode.get(nextPos);
		String curSeg = segments.get(nextPos);
		nextPos++;

		switch (curMode) {
		case LATIN:
			return curSeg;

		case CJK:
			cjkTokenizer = cjkIterator(curSeg);
			assert cjkTokenizer.hasNext();
			return cjkTokenizer.next();

		default:
			assert false; // DOH!
		}
		return null;
	}

	// C1C2C3C4 -> C1, C1C2, C2, C2C3, C3, C3C4, C4
	private Iterator<String> cjkIterator(String cjkText) {
		ArrayList<String> cjkToken = new ArrayList<String>();

		String lastChar = null;
		int length = cjkText.length();
		for (int offset = 0; offset < length;) {
			final int codepoint = cjkText.codePointAt(offset);
			offset += Character.charCount(codepoint);

			String curChar = new String(Character.toChars(codepoint));

			if (lastChar != null)
				cjkToken.add(lastChar + curChar);
			if (isCJK(codepoint)) // skip number embedded in cjk
				cjkToken.add(curChar);
			lastChar = curChar;
		}

		return cjkToken.iterator();
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

	protected String normalize(String text) {
		// TODO: JAVA6: normalize to NFKC
		// Do upper case first for Turkish and friends
		return text.toUpperCase(Locale.US).toLowerCase(Locale.US);
	}

	protected boolean isCJK(int codePoint) {
		UnicodeBlock block = Character.UnicodeBlock.of(codePoint);
		return block == UnicodeBlock.CJK_COMPATIBILITY // CJK
			|| block == UnicodeBlock.CJK_COMPATIBILITY_FORMS // 
			|| block == UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS // 
			|| block == UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS_SUPPLEMENT // 
			|| block == UnicodeBlock.CJK_RADICALS_SUPPLEMENT // 
			|| block == UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION // 
			|| block == UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS // 
			|| block == UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A // 
			|| block == UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_B // 
			|| block == UnicodeBlock.BOPOMOFO // Chinese
			|| block == UnicodeBlock.BOPOMOFO_EXTENDED //
			|| block == UnicodeBlock.HANGUL_COMPATIBILITY_JAMO // Korean
			|| block == UnicodeBlock.HANGUL_JAMO //
			|| block == UnicodeBlock.HANGUL_SYLLABLES //
			|| block == UnicodeBlock.KANBUN // Japanese
			|| block == UnicodeBlock.HIRAGANA //
			|| block == UnicodeBlock.KANGXI_RADICALS //
			|| block == UnicodeBlock.KANNADA //
			|| block == UnicodeBlock.KATAKANA //
			|| block == UnicodeBlock.KATAKANA_PHONETIC_EXTENSIONS;
	}
}
