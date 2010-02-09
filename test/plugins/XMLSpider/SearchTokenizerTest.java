package plugins.XMLSpider;

import junit.framework.TestCase;

// use this to convert unicodes:
//   http://people.w3.org/rishida/scripts/uniview/conversion.php
public class SearchTokenizerTest extends TestCase {
	public void testEnglish() {
		String[] helloWorld = new String[] { "hello", "world" };
		compare("Hello World", helloWorld);
		compare("hello    world", helloWorld);
		compare("hello    world  ", helloWorld);
		compare("hello\nworld  ", helloWorld);
		compare("hello\n world  ", helloWorld);
		compare("hello, world  ", helloWorld);
		compare("hello, world! ", helloWorld);
		compare("hello, world!", helloWorld);
	}

	public void testCJK() {
		// Chinese
		compare("\u4E00\u4E8C\u4E09\u56DB", new String[] {
			"\u4E00",
			"\u4E00\u4E8C",
			"\u4E8C",
			"\u4E8C\u4E09",
			"\u4E09",
			"\u4E09\u56DB"
			});
		// Chinese Ext-B
		compare("\u6A39\uD84C\uDFB4\u5B50", new String[] {
			"\u6A39",
			"\u6A39\uD84C\uDFB4",
			"\uD84C\uDFB4",
			"\uD84C\uDFB4\u5B50",
			"\u5B50"
		});
		// Chinese
		compare("\u8d77\u521d\uff0c\u3000\u795e\u5275\u9020\u5929\u5730\u3002", new String[] {
			"\u8d77",
			"\u8d77\u521d",
			"\u521d",
			// comma
			// space
			"\u795e",
			"\u795e\u5275",
			"\u5275",
			"\u5275\u9020",
			"\u9020",
			"\u9020\u5929",
			"\u5929",
			"\u5929\u5730",
			"\u5730"
			// full stop
			});
		// Korean
		compare(
			"\ud0dc\ucd08\uc5d0\ud558\ub098\ub2d8\uc774\ucc9c\uc9c0\ub97c\ucc3d\uc870\ud558\uc2dc\ub2c8\ub77c!",
			new String[] {
				"\ud0dc",
				"\ud0dc\ucd08",
				"\ucd08",
				"\ucd08\uc5d0",
				"\uc5d0",
				"\uc5d0\ud558",
				"\ud558",
				"\ud558\ub098",
				"\ub098",
				"\ub098\ub2d8",
				"\ub2d8",
				"\ub2d8\uc774",
				"\uc774",
				"\uc774\ucc9c",
				"\ucc9c",
				"\ucc9c\uc9c0",
				"\uc9c0",
				"\uc9c0\ub97c",
				"\ub97c",
				"\ub97c\ucc3d",
				"\ucc3d",
				"\ucc3d\uc870",
				"\uc870",
				"\uc870\ud558",
				"\ud558",
				"\ud558\uc2dc",
				"\uc2dc",
				"\uc2dc\ub2c8",
				"\ub2c8",
				"\ub2c8\ub77c",
				"\ub77c"
			});
		// Japanese
		compare(
			"\u306f\u3058\u3081\u306b\u795e\u306f\u5929\u3068\u5730\u3068\u3092\u5275\u9020\u3055\u308c\u305f\u3002",
			new String[] {
				"\u306f",
				"\u306f\u3058",
				"\u3058",
				"\u3058\u3081",
				"\u3081",
				"\u3081\u306b",
				"\u306b",
				"\u306b\u795e",
				"\u795e",
				"\u795e\u306f",
				"\u306f",
				"\u306f\u5929",
				"\u5929",
				"\u5929\u3068",
				"\u3068",
				"\u3068\u5730",
				"\u5730",
				"\u5730\u3068",
				"\u3068",
				"\u3068\u3092",
				"\u3092",
				"\u3092\u5275",
				"\u5275",
				"\u5275\u9020",
				"\u9020",
				"\u9020\u3055",
				"\u3055",
				"\u3055\u308c",
				"\u308c",
				"\u308c\u305f",
				"\u305f"
			});
	}

	public void testMixed() {
		// Chinese-digit-Chinese
		compare("\u4E00" + "1" + "\u4E01",
			new String[] {
				"\u4E00",
				"\u4E00" + "1",
				"1" + "\u4E01",
				"\u4E01"
		});
		// Chinese-digit-latin
		compare("\u4E00" + "1" + "a", new String[] {
			"\u4E00",
			"\u4E00" + "1",
			"a"
		});
		// Chinese-latin-Chinese
		compare("\u4E00" + "a" + "\u4E01", new String[] {
			"\u4E00",
			"a",
			"\u4E01"
		});
		// latin-digit-latin
		compare("a1a", new String[] {
			"a1a"
		});
		// latin-digit-latin chinese-chinese
		compare("a1a\u4E00\u4E01",new String[] {
			"a1a",
			"\u4E00",
			"\u4E00\u4E01",
			"\u4E01"
		});
		// latin-digit-latin-chinese-chinese
		compare("a1\u4E00\u4E01", new String[] {
			"a1",
			"\u4E00",
			"\u4E00\u4E01",
			"\u4E01"
		});
		// latin-digit space chinese-chinese
		compare("a1 \u4E00\u4E01", new String[] {
			"a1",
			"\u4E00",
			"\u4E00\u4E01",
			"\u4E01"
		});
	}

	private void compare(String text, String[] token) {
		SearchTokenizer st = new SearchTokenizer(text);
		System.out.println("> " + text);

		if (token == null)
			return;

		for (int i = 0; i < token.length; i++) {
			assertTrue("@@ ", st.hasNext());
			String t = st.next();
			System.out.println("- " + t);
			assertEquals("Testing '" + text + "'", token[i], t);
		}
		if (st.hasNext())
			System.out.println("!! " + st.next());
		assertFalse("!! ", st.hasNext());
	}
}
