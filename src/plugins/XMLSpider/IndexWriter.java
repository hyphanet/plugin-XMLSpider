/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package plugins.XMLSpider;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Text;

import com.db4o.ObjectSet;
import com.db4o.query.Query;

import freenet.support.Logger;

/**
 * Write index to disk file
 */
class IndexWriter {
	//- Writing Index
	long tProducedIndex;
	private Vector<String> indices;
	private int match;
	private long time_taken;
	private XMLSpider xmlSpider;

	IndexWriter(XMLSpider xmlSpider) {
		this.xmlSpider = xmlSpider;
	}

	public synchronized void makeIndex() throws Exception {
		try {
			time_taken = System.currentTimeMillis();

			makeSubIndices();
			makeMainIndex();

			time_taken = System.currentTimeMillis() - time_taken;

			Logger.minor(this, "Spider: indexes regenerated - tProducedIndex="
			        + (System.currentTimeMillis() - tProducedIndex) + "ms ago time taken=" + time_taken + "ms");

			tProducedIndex = System.currentTimeMillis();
		} finally {
		}
	}

	/**
	 * generates the main index file that can be used by librarian for searching in the list of
	 * subindices
	 * 
	 * @param void
	 * @author swati
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 */
	private void makeMainIndex() throws IOException, NoSuchAlgorithmException {
		// Produce the main index file.
		Logger.minor(this, "Producing top index...");

		//the main index file 
		File outputFile = new File(XMLSpider.DEFAULT_INDEX_DIR + "index.xml");
		// Use a stream so we can explicitly close - minimise number of filehandles used.
		BufferedOutputStream fos = new BufferedOutputStream(new FileOutputStream(outputFile));
		StreamResult resultStream;
		resultStream = new StreamResult(fos);

		try {
			/* Initialize xml builder */
			Document xmlDoc = null;
			DocumentBuilderFactory xmlFactory = null;
			DocumentBuilder xmlBuilder = null;
			DOMImplementation impl = null;
			Element rootElement = null;

			xmlFactory = DocumentBuilderFactory.newInstance();

			try {
				xmlBuilder = xmlFactory.newDocumentBuilder();
			} catch (javax.xml.parsers.ParserConfigurationException e) {

				Logger.error(this, "Spider: Error while initializing XML generator: " + e.toString(), e);
				return;
			}

			impl = xmlBuilder.getDOMImplementation();
			/* Starting to generate index */
			xmlDoc = impl.createDocument(null, "main_index", null);
			rootElement = xmlDoc.getDocumentElement();

			/* Adding header to the index */
			Element headerElement = xmlDoc.createElement("header");

			/* -> title */
			Element subHeaderElement = xmlDoc.createElement("title");
			Text subHeaderText = xmlDoc.createTextNode(XMLSpider.indexTitle);

			subHeaderElement.appendChild(subHeaderText);
			headerElement.appendChild(subHeaderElement);

			/* -> owner */
			subHeaderElement = xmlDoc.createElement("owner");
			subHeaderText = xmlDoc.createTextNode(XMLSpider.indexOwner);

			subHeaderElement.appendChild(subHeaderText);
			headerElement.appendChild(subHeaderElement);

			/* -> owner email */
			if (XMLSpider.indexOwnerEmail != null) {
				subHeaderElement = xmlDoc.createElement("email");
				subHeaderText = xmlDoc.createTextNode(XMLSpider.indexOwnerEmail);

				subHeaderElement.appendChild(subHeaderText);
				headerElement.appendChild(subHeaderElement);
			}
			/*
			 * the max number of digits in md5 to be used for matching with the search query is
			 * stored in the xml
			 */
			Element prefixElement = xmlDoc.createElement("prefix");
			/* Adding word index */
			Element keywordsElement = xmlDoc.createElement("keywords");
			for (int i = 0; i < indices.size(); i++) {

				Element subIndexElement = xmlDoc.createElement("subIndex");
				subIndexElement.setAttribute("key", indices.elementAt(i));
				//the subindex element key will contain the bits used for matching in that subindex
				keywordsElement.appendChild(subIndexElement);
			}

			prefixElement.setAttribute("value", match + "");
			rootElement.appendChild(prefixElement);
			rootElement.appendChild(headerElement);
			rootElement.appendChild(keywordsElement);

			/* Serialization */
			DOMSource domSource = new DOMSource(xmlDoc);
			TransformerFactory transformFactory = TransformerFactory.newInstance();
			Transformer serializer;

			try {
				serializer = transformFactory.newTransformer();
			} catch (javax.xml.transform.TransformerConfigurationException e) {
				Logger.error(this, "Spider: Error while serializing XML (transformFactory.newTransformer()): "
				        + e.toString(), e);
				return;
			}

			serializer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			serializer.setOutputProperty(OutputKeys.INDENT, "yes");

			/* final step */
			try {
				serializer.transform(domSource, resultStream);
			} catch (javax.xml.transform.TransformerException e) {
				Logger.error(this, "Spider: Error while serializing XML (transform()): " + e.toString(), e);
				return;
			}
		} finally {
			fos.close();
		}

		//The main xml file is generated 
		//As each word is generated enter it into the respective subindex
		//The parsing will start and nodes will be added as needed 

	}

	/**
	 * Generates the subindices. Each index has less than {@code MAX_ENTRIES} words. The original
	 * treemap is split into several sublists indexed by the common substring of the hash code of
	 * the words
	 * 
	 * @throws Exception
	 */
	private void makeSubIndices() throws Exception {
		Logger.normal(this, "Generating index...");

		Query query = xmlSpider.db.query();
		query.constrain(Term.class);
		query.descend("md5").orderAscending();
		@SuppressWarnings("unchecked")
		ObjectSet<Term> termSet = query.execute();

		indices = new Vector<String>();
		int prefix = (int) ((Math.log(termSet.size()) - Math.log(XMLSpider.MAX_ENTRIES)) / Math.log(16)) - 1;
		if (prefix <= 0)
			prefix = 1;
		match = 1;
		Vector<Term> list = new Vector<Term>();

		Term term0 = termSet.get(0);
		String str = term0.md5;
		String currentPrefix = str.substring(0, prefix);
		list.add(term0);

		int i = 0;
		for (Term term : termSet) {
			String key = term.md5;
			//create a list of the words to be added in the same subindex
			if (key.startsWith(currentPrefix)) {
				i++;
				list.add(term);
			} else {
				//generate the appropriate subindex with the current list
				generateSubIndex(prefix, list);
				str = key;
				currentPrefix = str.substring(0, prefix);
				list = new Vector<Term>();
				list.add(term);
			}
		}

		generateSubIndex(prefix, list);
	}

	private void generateSubIndex(int p, List<Term> list) throws Exception {
		boolean logMINOR = Logger.shouldLog(Logger.MINOR, this);
		/*
		 * if the list is less than max allowed entries in a file then directly generate the xml
		 * otherwise split the list into further sublists and iterate till the number of entries per
		 * subindex is less than the allowed value
		 */
		if (logMINOR)
			Logger.minor(this, "Generating subindex for " + list.size() + " entries with prefix length " + p);

		try {
			if (list.size() == 0)
				return;
			if (list.size() < XMLSpider.MAX_ENTRIES) {
				generateXML(list, p);
				return;
			}
		} catch (TooBigIndexException e) {
			// Handle below
		}
		if (logMINOR)
			Logger.minor(this, "Too big subindex for " + list.size() + " entries with prefix length " + p);
		//prefix needs to be incremented
		if (match <= p)
			match = p + 1;
		int prefix = p + 1;
		int i = 0;
		String str = list.get(i).md5;
		int index = 0;
		while (i < list.size()) {
			Term term = list.get(i);
			String key = term.md5;
			if ((key.substring(0, prefix)).equals(str.substring(0, prefix))) {
				i++;
			} else {
				generateSubIndex(prefix, list.subList(index, i));
				index = i;
				str = key;
			}
		}
		generateSubIndex(prefix, list.subList(index, i));
	}

	private class TooBigIndexException extends Exception {
		private static final long serialVersionUID = -6172560811504794914L;
	}

	/**
	 * generates the xml index with the given list of words with prefix number of matching bits in
	 * md5
	 * 
	 * @param list
	 *            list of the words to be added in the index
	 * @param prefix
	 *            number of matching bits of md5
	 * @throws Exception
	 */
	protected void generateXML(List<Term> list, int prefix) throws TooBigIndexException, Exception {
		String p = list.get(0).md5.substring(0, prefix);
		indices.add(p);
		File outputFile = new File(XMLSpider.DEFAULT_INDEX_DIR + "index_" + p + ".xml");
		BufferedOutputStream fos = new BufferedOutputStream(new FileOutputStream(outputFile));
		StreamResult resultStream;
		resultStream = new StreamResult(fos);

		try {
			/* Initialize xml builder */
			Document xmlDoc = null;
			DocumentBuilderFactory xmlFactory = null;
			DocumentBuilder xmlBuilder = null;
			DOMImplementation impl = null;
			Element rootElement = null;
			xmlFactory = DocumentBuilderFactory.newInstance();

			try {
				xmlBuilder = xmlFactory.newDocumentBuilder();
			} catch (javax.xml.parsers.ParserConfigurationException e) {
				Logger.error(this, "Spider: Error while initializing XML generator: " + e.toString(), e);
				return;
			}

			impl = xmlBuilder.getDOMImplementation();
			/* Starting to generate index */
			xmlDoc = impl.createDocument(null, "sub_index", null);
			rootElement = xmlDoc.getDocumentElement();

			/* Adding header to the index */
			Element headerElement = xmlDoc.createElement("header");
			/* -> title */
			Element subHeaderElement = xmlDoc.createElement("title");
			Text subHeaderText = xmlDoc.createTextNode(XMLSpider.indexTitle);
			subHeaderElement.appendChild(subHeaderText);
			headerElement.appendChild(subHeaderElement);

			Element filesElement = xmlDoc.createElement("files"); /* filesElement != fileElement */
			Element EntriesElement = xmlDoc.createElement("entries");
			EntriesElement.setNodeValue(list.size() + "");
			EntriesElement.setAttribute("value", list.size() + "");

			/* Adding word index */
			Element keywordsElement = xmlDoc.createElement("keywords");
			Vector<Long> fileid = new Vector<Long>();
			for (int i = 0; i < list.size(); i++) {
				Element wordElement = xmlDoc.createElement("word");
				Term term = list.get(i);
				wordElement.setAttribute("v", term.word);

				Query query = xmlSpider.db.query();
				query.constrain(TermPosition.class);

				query.descend("word").constrain(term.word);
				@SuppressWarnings("unchecked")
				ObjectSet<TermPosition> set = query.execute();

				for (TermPosition termPos : set) {
					synchronized (termPos) {
						Page page = xmlSpider.getPageById(termPos.pageId);

						synchronized (page) {

							/*
							 * adding file information uriElement - lists the id of the file
							 * containing a particular word fileElement - lists the id,key,title of
							 * the files mentioned in the entire subindex
							 */
							Element uriElement = xmlDoc.createElement("file");
							Element fileElement = xmlDoc.createElement("file");
							uriElement.setAttribute("id", Long.toString(page.id));
							fileElement.setAttribute("id", Long.toString(page.id));
							fileElement.setAttribute("key", page.uri);
							fileElement.setAttribute("title", page.pageTitle != null ? page.pageTitle : page.uri);

							/* Position by position */
							int[] positions = termPos.positions;

							StringBuilder positionList = new StringBuilder();

							for (int k = 0; k < positions.length; k++) {
								if (k != 0)
									positionList.append(',');
								positionList.append(positions[k]);
							}
							uriElement.appendChild(xmlDoc.createTextNode(positionList.toString()));
							wordElement.appendChild(uriElement);
							if (!fileid.contains(page.id)) {
								fileid.add(page.id);
								filesElement.appendChild(fileElement);
							}
						}
					}
				}
				keywordsElement.appendChild(wordElement);
			}
			rootElement.appendChild(EntriesElement);
			rootElement.appendChild(headerElement);
			rootElement.appendChild(filesElement);
			rootElement.appendChild(keywordsElement);

			/* Serialization */
			DOMSource domSource = new DOMSource(xmlDoc);
			TransformerFactory transformFactory = TransformerFactory.newInstance();
			Transformer serializer;

			try {
				serializer = transformFactory.newTransformer();
			} catch (javax.xml.transform.TransformerConfigurationException e) {
				Logger.error(this, "Spider: Error while serializing XML (transformFactory.newTransformer()): "
				        + e.toString(), e);
				return;
			}
			serializer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			serializer.setOutputProperty(OutputKeys.INDENT, "yes");
			/* final step */
			try {
				serializer.transform(domSource, resultStream);
			} catch (javax.xml.transform.TransformerException e) {
				Logger.error(this, "Spider: Error while serializing XML (transform()): " + e.toString(), e);
				return;
			}
		} finally {
			fos.close();
		}
		if (outputFile.length() > XMLSpider.MAX_SUBINDEX_UNCOMPRESSED_SIZE && list.size() > 1) {
			outputFile.delete();
			throw new TooBigIndexException();
		}

		if (Logger.shouldLog(Logger.MINOR, this))
			Logger.minor(this, "Spider: indexes regenerated.");
	}

	public void generateSubIndex(String filename) {
		//		generates the new subIndex
		File outputFile = new File(filename);
		BufferedOutputStream fos;
		try {
			fos = new BufferedOutputStream(new FileOutputStream(outputFile));
		} catch (FileNotFoundException e1) {
			Logger.error(this, "Cannot open " + filename + " writing index : " + e1, e1);
			return;
		}
		try {
			StreamResult resultStream;
			resultStream = new StreamResult(fos);

			/* Initialize xml builder */
			Document xmlDoc = null;
			DocumentBuilderFactory xmlFactory = null;
			DocumentBuilder xmlBuilder = null;
			DOMImplementation impl = null;
			Element rootElement = null;

			xmlFactory = DocumentBuilderFactory.newInstance();

			try {
				xmlBuilder = xmlFactory.newDocumentBuilder();
			} catch (javax.xml.parsers.ParserConfigurationException e) {
				/* Will (should ?) never happen */
				Logger.error(this, "Spider: Error while initializing XML generator: " + e.toString(), e);
				return;
			}

			impl = xmlBuilder.getDOMImplementation();

			/* Starting to generate index */

			xmlDoc = impl.createDocument(null, "sub_index", null);
			rootElement = xmlDoc.getDocumentElement();

			/* Adding header to the index */
			Element headerElement = xmlDoc.createElement("header");

			/* -> title */
			Element subHeaderElement = xmlDoc.createElement("title");
			Text subHeaderText = xmlDoc.createTextNode(XMLSpider.indexTitle);

			subHeaderElement.appendChild(subHeaderText);
			headerElement.appendChild(subHeaderElement);

			/* -> owner */
			subHeaderElement = xmlDoc.createElement("owner");
			subHeaderText = xmlDoc.createTextNode(XMLSpider.indexOwner);

			subHeaderElement.appendChild(subHeaderText);
			headerElement.appendChild(subHeaderElement);

			/* -> owner email */
			if (XMLSpider.indexOwnerEmail != null) {
				subHeaderElement = xmlDoc.createElement("email");
				subHeaderText = xmlDoc.createTextNode(XMLSpider.indexOwnerEmail);

				subHeaderElement.appendChild(subHeaderText);
				headerElement.appendChild(subHeaderElement);
			}

			Element filesElement = xmlDoc.createElement("files"); /* filesElement != fileElement */

			Element EntriesElement = xmlDoc.createElement("entries");
			EntriesElement.setNodeValue("0");
			EntriesElement.setAttribute("value", "0");
			//all index files are ready
			/* Adding word index */
			Element keywordsElement = xmlDoc.createElement("keywords");

			rootElement.appendChild(EntriesElement);
			rootElement.appendChild(headerElement);
			rootElement.appendChild(filesElement);
			rootElement.appendChild(keywordsElement);

			/* Serialization */
			DOMSource domSource = new DOMSource(xmlDoc);
			TransformerFactory transformFactory = TransformerFactory.newInstance();
			Transformer serializer;

			try {
				serializer = transformFactory.newTransformer();
			} catch (javax.xml.transform.TransformerConfigurationException e) {
				Logger.error(this, "Spider: Error while serializing XML (transformFactory.newTransformer()): "
				        + e.toString(), e);
				return;
			}

			serializer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			serializer.setOutputProperty(OutputKeys.INDENT, "yes");

			/* final step */
			try {
				serializer.transform(domSource, resultStream);
			} catch (javax.xml.transform.TransformerException e) {
				Logger.error(this, "Spider: Error while serializing XML (transform()): " + e.toString(), e);
				return;
			}
		} finally {
			try {
				fos.close();
			} catch (IOException e) {
				// Ignore
			}
		}

		if (Logger.shouldLog(Logger.MINOR, this))
			Logger.minor(this, "Spider: indexes regenerated.");
	}
}