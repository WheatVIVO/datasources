package org.wheatinitiative.vivo.datasource;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.connector.cornell.Cornell;
import org.wheatinitiative.vivo.datasource.connector.cordis.Cordis;
import org.wheatinitiative.vivo.datasource.connector.orcid.OrcidConnector;
import org.wheatinitiative.vivo.datasource.connector.prodinra.Prodinra;
import org.wheatinitiative.vivo.datasource.connector.rcuk.Rcuk;
import org.wheatinitiative.vivo.datasource.connector.tamu.Tamu;
import org.wheatinitiative.vivo.datasource.connector.usda.Usda;
import org.wheatinitiative.vivo.datasource.connector.wheatinitiative.WheatInitiative;

import com.hp.hpl.jena.rdf.model.Model;

public class LaunchIngest {

    private static final Log log = LogFactory.getLog(LaunchIngest.class);
    
    public static void main(String[] args) {
        if(args.length < 3) {
            System.out.println("Usage: LaunchIngest " 
                    + "cordis|rcuk|prodinra|usda|wheatinitiative|cornell|tamu outputfile " 
                    + "queryTerm ... [queryTermN] [limit]");
            return;
        } 
        List<String> queryTerms = new LinkedList<String>(
                Arrays.asList(args));
        String connectorName  = queryTerms.remove(0);
        String outputFileName = queryTerms.remove(0);
        int limit = getLimit(queryTerms);
        if(limit < Integer.MAX_VALUE) {
            log.info("Retrieving a limit of " + limit + " records");
        }
        DataSource connector = getConnector(connectorName);
        connector.getConfiguration().setQueryTerms(queryTerms);
        connector.getConfiguration().setEndpointParameters(null);
        connector.getConfiguration().setLimit(limit);
        connector.run();
        Model result = connector.getResult();
        if(result == null) {
            log.warn("result is null");
        } else {
            File outputFile = new File(outputFileName);
            FileOutputStream fos;
            try {
                log.info("Writing output to " + outputFile);
                fos = new FileOutputStream(outputFile);
                result.write(fos, "N3");
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    private static DataSource getConnector(String connectorName) {
        DataSource connector = null;
        if ("cornell".equals(connectorName)) {
        	connector = new Cornell();
        	connector.getConfiguration().setServiceURI(
        			"http://vivo.cornell.edu/");
        } else if ("cordis".equals(connectorName)) {
        	connector = new Cordis();
        	connector.getConfiguration().setServiceURI(
        			"http://cordis.europa.eu/search/result_en");
        } else if("rcuk".equals(connectorName)) {
            connector = new Rcuk();
            connector.getConfiguration().setServiceURI(
                    "http://http://gtr.rcuk.ac.uk/gtr/api/");
        } else if ("prodinra".equals(connectorName)) {
            connector = new Prodinra();
            connector.getConfiguration().setServiceURI(
                    "http://oai.prodinra.inra.fr/ft");
        } else if ("usda".equals(connectorName)) {
            connector = new Usda();
            connector.getConfiguration().setServiceURI(
                    "http://vivo.usda.gov/");
        } else if ("wheatinitiative".equals(connectorName)) {
            connector = new WheatInitiative();
            connector.getConfiguration().setServiceURI(
                    "http://www.wheatinitiative.org/administration/users/csv");
        } else if ("orcid".equals(connectorName)) {
            connector = new OrcidConnector();
        } else if ("tamu".equals(connectorName)) {
        	connector = new Tamu();
            connector.getConfiguration().setServiceURI(
                    "http://scholars.library.tamu.edu/vivo/");
        } else {
            throw new RuntimeException("Connector not found: " 
                    + connectorName);
        }
        return connector;
    }
    
    private static int getLimit(List<String> queryTerms) {
        if(!queryTerms.isEmpty()) {
            String possibleLimit = queryTerms.get(queryTerms.size() - 1);            
            try {
                int limit = Integer.parseInt(possibleLimit, 10);
                queryTerms.remove(queryTerms.size() - 1);
                return limit;
            } catch (NumberFormatException e) {
                // no limit argument present; move on
            }
        }
        return Integer.MAX_VALUE;
    }
    
}
