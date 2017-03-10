package org.wheatinitiative.vivo.datasource.connector.cordis;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.http.client.utils.URIBuilder;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.connector.ConnectorDataSource;
import org.wheatinitiative.vivo.datasource.util.IteratorWithSize;
import org.wheatinitiative.vivo.datasource.util.http.HttpUtils;
import org.wheatinitiative.vivo.datasource.util.xml.XmlToRdf;
import org.wheatinitiative.vivo.datasource.util.xml.rdf.RdfUtils;

import com.hp.hpl.jena.rdf.model.Model;

public class Cordis extends ConnectorDataSource implements DataSource {

	
    private static final String PRODINRA_TBOX_NS = "http://cordis.europa.eu/";
    private static final String PRODINRA_ABOX_NS = PRODINRA_TBOX_NS + "individual/";
    private static final String NAMESPACE_ETC = PRODINRA_ABOX_NS + "n";
	
	
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

		private String serv_URI;
		private List<String> qurTerm;
		private final int SearchResultsPerPage = 20;
		private int CurrentPage = 0;
		
		private Model cachedResult = null;
		private boolean done = false;
	    private HttpUtils httpUtils = new HttpUtils();
	    private XmlToRdf xmlToRdf = new XmlToRdf();
	    private RdfUtils rdfUtils = new RdfUtils();
		
		
		public CordisModelIterator(String serviceURI, List<String> queryTerms) {
			serv_URI = serviceURI;
			qurTerm = queryTerms;
		}

		public boolean hasNext() {
			
			return ( (cachedResult != null || !done) && !qurTerm.isEmpty() );
			
		}

		public Model next() {	// URL
			CurrentPage++ ;
			URIBuilder uriB;
			try {
				uriB = new URIBuilder(serv_URI);
				uriB.addParameter( "q", qurTerm.get(0));
				uriB.addParameter( "format", "xml" );
				uriB.addParameter( "p", Integer.toString(CurrentPage, 10));
				uriB.addParameter( "num", Integer.toString(SearchResultsPerPage, 10));
				
				String request = uriB.build().toString() ;
				String response = httpUtils.getHttpResponse(request);
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
