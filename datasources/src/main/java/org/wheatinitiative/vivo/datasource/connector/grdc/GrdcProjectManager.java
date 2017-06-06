package org.wheatinitiative.vivo.datasource.connector.grdc;   
   
   
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.hp.hpl.jena.rdf.model.Model;
	
	
// A class to manage the projects from Grdc.
    
public class GrdcProjectManager {
	
    public static final Log log = LogFactory.getLog(Grdc.class);


	public GrdcProjectManager ( ) {
		
	}
	
    
    public List<List<String>> htmlDataRetrieval( Document doc ) {
    	
    	// A method to retrieve data from child nodes of HTML.
    	
    	Elements tbody_element = null;
    	tbody_element = doc.getElementsByClass("calevent__tbody");
    	if ( tbody_element.size() != 1 )
    	{
    		throw new RuntimeException("Expected one class=\"calevent__tbody\" but the size is: " + tbody_element.size() );
    	}
    	
    	Elements children = tbody_element.get(0).children();
    	
    	List<List<String>> retrievedElements = new ArrayList<List<String>>();
    	
    	for (Element element : children)
    	{
    		List<String> row = new ArrayList<String>();
    		
    		for (  Element child : element.children()  )
    		{	
    			row.add( child.text() );
    		}
    			retrievedElements.add(row);
    	}
    	
		return retrievedElements;
		
    }
    
    
    public Model processingOfData( Model model, Document doc ) {
    	
    	List<List<String>> htmlElements = htmlDataRetrieval( doc );
    	
    	// Distinguish every element, create the right triples and add them to the model
    	
    	
    	/*
    	// Some test code follows..
		List<Element> elements = doc.getElementsByClass( "result__link" );
		log.debug("Size before = " + elements.size());
        for (Element elem : elements) {
        	model.add( OWL.Thing, RDFS.label, elem.attr("href") );
        }
    	log.debug("Size after = " + model.size());
    	*/
    	
    	return model;
    }
    
    
}