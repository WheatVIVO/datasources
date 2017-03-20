package org.wheatinitiative.vivo.datasource.connector.cordis;


// Java imports
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

// Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// Wheatvivo imports
import org.apache.http.client.utils.URIBuilder;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.ConnectorDataSource;
import org.wheatinitiative.vivo.datasource.connector.prodinra.Prodinra;
import org.wheatinitiative.vivo.datasource.util.IteratorWithSize;
import org.wheatinitiative.vivo.datasource.util.http.HttpUtils;
import org.wheatinitiative.vivo.datasource.util.xml.XmlToRdf;
import org.wheatinitiative.vivo.datasource.util.xml.rdf.RdfUtils;

// Jena imports
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.util.ResourceUtils;


public class Cordis extends ConnectorDataSource implements DataSource {

	
	public static final Log log = LogFactory.getLog(Cordis.class);
	
    private static final String CORDIS_TBOX_NS = "http://cordis.europa.eu/";
    private static final String CORDIS_ABOX_NS = CORDIS_TBOX_NS + "individual/";
    private static final String NAMESPACE_ETC = CORDIS_ABOX_NS + "n";
    private static final String SPARQL_RESOURCE_DIR = "/cordis/sparql/";
	
	@Override
	protected Model filter(Model model) {
		return model;
	}
	
	
	@Override
	protected IteratorWithSize<Model> getSourceModelIterator() {
		return new CordisModelIterator(
				this.getConfiguration().getServiceURI(), 
				this.getConfiguration().getQueryTerms()
				);
	}
	
	
	private class CordisModelIterator implements IteratorWithSize<Model> {

		private String service_URI;
		private List<String> queryTerms;
		private final int SearchResultsPerPage = 20;
		private int CurrentPage = 0;
		
		private Model cachedResult = null;
		private boolean done = false;
	    private HttpUtils httpUtils = new HttpUtils();
	    private XmlToRdf xmlToRdf = new XmlToRdf();
	    private RdfUtils rdfUtils = new RdfUtils();
		
		
	    // Constructor
		public CordisModelIterator(String serviceURI, List<String> queryTerms) {
			this.service_URI = serviceURI;
			this.queryTerms = queryTerms;
		}
		
		public boolean hasNext() {
			
			return ( (cachedResult != null || !done) && !this.queryTerms.isEmpty() );
			
		}

		public Model next() {	// URL
			CurrentPage++ ;
			URIBuilder uriB;
			try {
				
				String request = null;
				String response = null;
				
				for (String qTerm : this.queryTerms)
				{
						uriB = new URIBuilder( this.service_URI );
						uriB.addParameter( "q", qTerm );
						uriB.addParameter( "format", "xml" );
						uriB.addParameter( "p", Integer.toString(CurrentPage, 10) );
						uriB.addParameter( "num", Integer.toString(SearchResultsPerPage, 10) );
				
						request = uriB.build().toString() ;
						response = httpUtils.getHttpResponse(request);
				}
				
				Model m = xmlToRdf.toRDF(response);
				
				if ( CurrentPage == 20 /*sum_of_pages*/ )
				{
					done = true;
				}
				
				return m;
				
			} catch (URISyntaxException e) {
				throw new RuntimeException(e);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

		}

		public Integer size() {
			
			return null;
		}
	}
	
	
	
	@Override
	protected Model mapToVIVO(Model model) {
		
		return rdfUtils.renameBNodes(model, NAMESPACE_ETC, model);

	}

}
