package org.wheatinitiative.vivo.datasource.connector.grdc;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.utils.URIBuilder;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.ConnectorDataSource;
import org.wheatinitiative.vivo.datasource.util.IteratorWithSize;
import org.wheatinitiative.vivo.datasource.util.http.HttpUtils;
import org.wheatinitiative.vivo.datasource.util.xml.rdf.RdfUtils;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDFS;

public class Grdc extends ConnectorDataSource implements DataSource {
	
    public static final Log log = LogFactory.getLog(Grdc.class);
    
    
    private static final String GRDC_TBOX_NS = "https://grdc.com.au/";
    private static final String GRDC_ABOX_NS = GRDC_TBOX_NS + "individual/";
    private static final String NAMESPACE_ETC = GRDC_ABOX_NS + "n";
    private static final String SPARQL_RESOURCE_DIR = "/grdc/sparql/";
	
    
    private String service_URI;
    private URL grdcUrl;
    private List<String> queryTerms;
    private List<String> SearchTypesArray;
    
    private static final int SEARCH_RESULTS_PER_PAGE = 20;
    private boolean done = false;
    private HttpUtils httpUtils = new HttpUtils();
    private RdfUtils rdfUtils = new RdfUtils();
    private Map<String, Model> initialResultsCache;
    private Map<String, Integer> totalForQueryTerm;
    private Iterator<String> queryTermIterator;
    private String currentQueryTerm;
    private int currentResultForQueryTerm = 1;
    
    
    public Grdc() {
    	try {
			this.grdcUrl = new URL("https://grdc.com.au/search");
		} catch (MalformedURLException e) {
			log.error(e, e);
		}
    }
    
    /*
    protected void GrdcModelIterator() {
    	 this.queryTerms = queryTerms;
    	 this.queryTermIterator = queryTerms.iterator();
    	 initialResultsCache = fetchInitialResults();
    	 totalForQueryTerm = getTotalsForQueryTerms(initialResultsCache);
    }
    */
    
    protected class GrdcModelIterator implements IteratorWithSize<Model>{
    	
    	protected Boolean tmp_boolean = true;
    	
		public boolean hasNext() {
			return tmp_boolean;
		}
		
		public Model next() {
			Model tmp_model = ModelFactory.createDefaultModel();
	    	List<String> queries = Arrays.asList("wheat", "ble");
	    	List<String> SearchTypesArray = Arrays.asList("Project Summary", "Final Report");
	    	for ( String query : queries ) {
	    		tmp_model.add( fetchResults( query, SearchTypesArray, 1) );
	    	}
	    	tmp_boolean = false;
	    	return tmp_model;
		}
		
		public Integer size() {
			// TODO Auto-generated method stub
			return null;
		}
       
    }

    
    
    private Model fetchResults(String queryTerm, List<String> SearchTypesArray, int startingResult) {

        Document doc;
        Model model = ModelFactory.createDefaultModel();
        try {
        	
        	URIBuilder grdcURI = new URIBuilder(grdcUrl.toString());
        	grdcURI.addParameter("query", queryTerm);
        	
        	for (  String SearchType : SearchTypesArray  ) {
        		grdcURI.addParameter("f.Type|ctype", SearchType);
        	}
        	
        	doc = Jsoup.parse(grdcURI.build().toURL(), 20000);
        }
        catch (URISyntaxException | IOException e ) {
        	log.error(e, e);
        	throw new RuntimeException(e);
        }
        
        
        //  
        
        
    	// We can add the different type of results in different files
    	// For example: Grdc_projects.java, Grdc_publication.java and so on..
		
		
    	Elements proj = null;
    	proj = doc.getElementsByClass("project__name");
    	if ( proj != null )
    	{
    		//	We have a project!
    		//	Go do staff for projects.
    		//	We should check elements with elements to decide if we are getting a project,
    		//											a publication or sth else, but later..
    		
    		 
    		htmlDataRetrieval( Document doc, Elements HtmlElements) ;
    		
        }
    	
		
		List<Element> elements = doc.getElementsByClass( "result__link" );
		log.debug("Size before = " + elements.size());
        for (Element elem : elements) {
        	model.add( OWL.Thing, RDFS.label, elem.attr("href") );
        }
    	log.debug("Size after = " + model.size());
        return model;
    }
    
    
    // A method to retrieve data from child nodes of HTML.
    
    //Put the following along with every other method to make a project model, inside the Project.java
    
    public List<List<String>> htmlDataRetrieval( Document doc, Elements HtmlElements) {
    	
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
    	
    	
    	// Change the method's type and the return statement depending on what we want to return. 
		return retrievedElements;


    }
    	
        

    
    
	@Override
	protected IteratorWithSize<Model> getSourceModelIterator() {
        return new GrdcModelIterator( );
	}
    
    
	@Override
	protected Model filter(Model model) {
		// TODO Auto-generated method stub
		return model;
	}
	
	
	@Override
	protected Model mapToVIVO(Model model) {
		// TODO Auto-generated method stub
		return model;
	}
	
    
    
}