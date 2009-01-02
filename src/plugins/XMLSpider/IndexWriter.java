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
import java.util.Set;
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

import plugins.XMLSpider.db.Config;
import plugins.XMLSpider.db.Page;
import plugins.XMLSpider.db.Term;
import plugins.XMLSpider.db.TermPosition;
import plugins.XMLSpider.org.garret.perst.Storage;
import freenet.support.Logger;

/**
 * Write index to disk file
 */
public class IndexWriter {
	//- Writing Index
	public long tProducedIndex;
	private Vector<String> indices;
	private int match;
	private long time_taken;
	private XMLSpider xmlSpider;
	private boolean logMINOR = Logger.shouldLog(Logger.MINOR, this);

	IndexWriter(XMLSpider xmlSpider) {
		this.xmlSpider = xmlSpider;
	}

	public synchronized void makeIndex() throws Exception {
		logMINOR = Logger.shouldLog(Logger.MINOR, this);
		xmlSpider.db.beginThreadTransaction(Storage.COOPERATIVE_TRANSACTION);
		try {
			time_taken = System.currentTimeMillis();

			Config config = xmlSpider.getConfig();

			File indexDir = new File(config.getIndexDir());
			if (!indexDir.exists() && !indexDir.isDirectory() && !indexDir.mkdirs()) {
				Logger.error(this, "Cannot create index directory: " + indexDir);
				return;
			}

			makeSubIndices(config);
			makeMainIndex(config);

			time_taken = System.currentTimeMillis() - time_taken;

			if (logMINOR)
				Logger.minor(this, "Spider: indexes regenerated - tProducedIndex="
				        + (System.currentTimeMillis() - tProducedIndex) + "ms ago time taken=" + time_taken + "ms");

			tProducedIndex = System.currentTimeMillis();
		} finally {
			xmlSpider.db.endThreadTransaction();
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
	private void makeMainIndex(Config config) throws IOException, NoSuchAlgorithmException {
		// Produce the main index file.
		if (logMINOR)
			Logger.minor(this, "Producing top index...");

		//the main index file 
		File outputFile = new File(config.getIndexDir() + "index.xml");
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
			Text subHeaderText = xmlDoc.createTextNode(config.getIndexTitle());

			subHeaderElement.appendChild(subHeaderText);
			headerElement.appendChild(subHeaderElement);

			/* -> owner */
			subHeaderElement = xmlDoc.createElement("owner");
			subHeaderText = xmlDoc.createTextNode(config.getIndexOwner());

			subHeaderElement.appendChild(subHeaderText);
			headerElement.appendChild(subHeaderElement);

			/* -> owner email */
			if (config.getIndexOwnerEmail() != null) {
				subHeaderElement = xmlDoc.createElement("email");
				subHeaderText = xmlDoc.createTextNode(config.getIndexOwnerEmail());

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
	private void makeSubIndices(Config config) throws Exception {
		Logger.normal(this, "Generating index...");

		List<Term> termList = xmlSpider.getRoot().getTermList();
		int termCount = xmlSpider.getRoot().getTermCount();

		indices = new Vector<String>();
		int prefix = (int) ((Math.log(termCount) - Math.log(config.getIndexMaxEntries())) / Math.log(16)) - 1;
		if (prefix <= 0)
			prefix = 1;
		match = 1;
		Vector<Term> list = new Vector<Term>();

		Term term0 = termList.get(0);
		String currentPrefix = term0.getMD5().substring(0, prefix);

		int i = 0;
		for (Term term : termList) {
			String key = term.getMD5();
			//create a list of the words to be added in the same subindex
			if (key.startsWith(currentPrefix)) {
				i++;
				list.add(term);
			} else {
				//generate the appropriate subindex with the current list
				generateSubIndex(config, prefix, list);

				// next list
				currentPrefix = key.substring(0, prefix);
				list = new Vector<Term>();
				list.add(term);
			}
		}

		generateSubIndex(config, prefix, list);
	}

	private void generateSubIndex(Config config, int p, List<Term> list) throws Exception {
		/*
		 * if the list is less than max allowed entries in a file then directly generate the xml
		 * otherwise split the list into further sublists and iterate till the number of entries per
		 * subindex is less than the allowed value
		 */
		if (logMINOR)
			Logger.minor(this, "Generating subindex for " + list.size() + " entries with prefix ("
			        + list.get(0).getMD5().substring(0, p) + ")");

		try {
			if (list.size() == 0)
				return;
			if (list.size() < config.getIndexMaxEntries()) {
				generateXML(config, list, p);
				return;
			}
		} catch (TooBigIndexException e) {
			// Handle below
		}
		if (logMINOR)
			Logger.minor(this, "Too big subindex for " + list.size() + " entries with prefix ("
			        + list.get(0).getMD5().substring(0, p) + ")");
		//prefix needs to be incremented
		if (match <= p)
			match = p + 1;
		int prefix = p + 1;
		int i = 0;
		String str = list.get(i).getMD5();
		int index = 0;
		while (i < list.size()) {
			Term term = list.get(i);
			String key = term.getMD5();
			if ((key.substring(0, prefix)).equals(str.substring(0, prefix))) {
				i++;
			} else {
				generateSubIndex(config, prefix, list.subList(index, i));
				index = i;
				str = key;
			}
		}
		generateSubIndex(config, prefix, list.subList(index, i));
	}

	private static class TooBigIndexException extends Exception {
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
	protected void generateXML(Config config, List<Term> list, int prefix) throws TooBigIndexException, Exception {
		String p = list.get(0).getMD5().substring(0, prefix);
		indices.add(p);
		File outputFile = new File(config.getIndexDir() + "index_" + p + ".xml");
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
			Text subHeaderText = xmlDoc.createTextNode(config.getIndexTitle());
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
				wordElement.setAttribute("v", term.getWord());

				Set<Page> pages = term.getPages();

				for (Page page : pages) {
					TermPosition termPos = page.getTermPosition(term);
					
					synchronized (termPos) {
						synchronized (page) {
							/*
							 * adding file information uriElement - lists the id of the file
							 * containing a particular word fileElement - lists the id,key,title of
							 * the files mentioned in the entire subindex
							 */
							Element uriElement = xmlDoc.createElement("file");
							Element fileElement = xmlDoc.createElement("file");
							uriElement.setAttribute("id", Long.toString(page.getId()));
							fileElement.setAttribute("id", Long.toString(page.getId()));
							fileElement.setAttribute("key", page.getURI());
							fileElement.setAttribute("title", page.getPageTitle() != null ? page.getPageTitle() : page
							        .getURI());

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
							if (!fileid.contains(page.getId())) {
								fileid.add(page.getId());
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
		if (outputFile.length() > config.getIndexSubindexMaxSize() && list.size() > 1) {
			outputFile.delete();
			throw new TooBigIndexException();
		}

		if (logMINOR)
			Logger.minor(this, "Spider: indexes regenerated.");
	}

	protected void generateSubIndex(Config config, String filename) {
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
			Text subHeaderText = xmlDoc.createTextNode(config.getIndexTitle());

			subHeaderElement.appendChild(subHeaderText);
			headerElement.appendChild(subHeaderElement);

			/* -> owner */
			subHeaderElement = xmlDoc.createElement("owner");
			subHeaderText = xmlDoc.createTextNode(config.getIndexOwner());

			subHeaderElement.appendChild(subHeaderText);
			headerElement.appendChild(subHeaderElement);

			/* -> owner email */
			if (config.getIndexOwnerEmail() != null) {
				subHeaderElement = xmlDoc.createElement("email");
				subHeaderText = xmlDoc.createTextNode(config.getIndexOwnerEmail());

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

		if (logMINOR)
			Logger.minor(this, "Spider: indexes regenerated.");
	}
}