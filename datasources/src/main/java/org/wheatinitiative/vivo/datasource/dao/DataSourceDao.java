package org.wheatinitiative.vivo.datasource.dao;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wheatinitiative.vivo.datasource.DataSourceDescription;
import org.wheatinitiative.vivo.datasource.DataSourceUpdateFrequency;
import org.wheatinitiative.vivo.datasource.SparqlEndpointParams;

import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;

public class DataSourceDao {

    public static final String ADMIN_APP_TBOX = 
            "http://vivo.wheatinitiative.org/ontology/adminapp/";
    public static final String DATASOURCE = ADMIN_APP_TBOX + "DataSource";
    public static final String MERGESOURCE = ADMIN_APP_TBOX + "MergeDataSource";
    public static final String PUBLISHSOURCE = ADMIN_APP_TBOX + "PublishDataSource";
    public static final String SPARQLENDPOINT = ADMIN_APP_TBOX + "SparqlEndpoint";
    public static final String USESSPARQLENDPOINT = ADMIN_APP_TBOX + "usesSparqlEndpoint";
    public static final String USESQUERYTERMSET = ADMIN_APP_TBOX + "usesQueryTermSet";
    public static final String QUERYTERM = ADMIN_APP_TBOX + "queryTerm";
    public static final String DEPLOYMENTURI = ADMIN_APP_TBOX + "deploymentURI";
    public static final String PRIORITY = ADMIN_APP_TBOX + "priority";
    public static final String LASTUPDATE = ADMIN_APP_TBOX + "lastUpdate";
    public static final String NEXTUPDATE = ADMIN_APP_TBOX + "nextUpdate";
    public static final String UPDATEFREQUENCY = ADMIN_APP_TBOX + "updateFrequency";
    public static final String SCHEDULEAFTER = ADMIN_APP_TBOX + "scheduleAfter";
    public static final String SERVICEURI = ADMIN_APP_TBOX + "serviceURI";
    public static final String ENDPOINTURI = ADMIN_APP_TBOX + "endpointURI";
    public static final String ENDPOINTUPDATEURI = ADMIN_APP_TBOX + "endpointUpdateURI";
    public static final String ENDPOINTUSERNAME = ADMIN_APP_TBOX + "username";
    public static final String ENDPOINTPASSWORD = ADMIN_APP_TBOX + "password";
    public static final String GRAPHURI = ADMIN_APP_TBOX + "graphURI";
    
    private static final Log log = LogFactory.getLog(DataSourceDao.class);
    
    private ModelConstructor modelConstructor;
    private final String dataSourcesQuery = getDataSourcesQuery();
    private final String mergeSourcesQuery = getDataSourcesQuery(MERGESOURCE);
    private final String publishSourcesQuery = getDataSourcesQuery(PUBLISHSOURCE);
    
    public DataSourceDao(ModelConstructor modelConstructor) {
        this.modelConstructor = modelConstructor;
    }
    
    public String getDataSourcesQuery() {
        return getDataSourcesQuery(DATASOURCE);
    }
    
    public String getAllDataSourcesQuery() {
        return getDataSourcesQuery(null);
    }
    
    public String getDataSourcesQuery(String dataSourceSubclass) {
        return "CONSTRUCT { \n" +
                "    ?dataSource ?p ?o . \n" +
                "    ?endpoint ?endpointP ?endpointO . \n" +
                "    ?queryTermSet ?queryTermP ?queryTermO \n" +
                "} WHERE { \n" +
                ((dataSourceSubclass != null) ? 
                "    ?dataSource a <" + dataSourceSubclass + "> . \n" : "") +
                ((DATASOURCE.equals(dataSourceSubclass)) ? 
                        "    FILTER NOT EXISTS { \n" +
                        "        ?dataSource a ?subclass . \n" +
                        "        ?subclass <" + RDFS.subClassOf + "> "
                                + "<" + DATASOURCE + "> \n" +
                        "     } \n"
                    : "") +
                "    ?dataSource ?p ?o . \n" +
                "    OPTIONAL { \n" +
                "        ?dataSource <" + USESSPARQLENDPOINT +"> ?endpoint . \n" +
                "        ?endpoint ?endpointP ?endpointO \n" +
                "    } \n" +
                "    OPTIONAL { \n" +
                "        ?dataSource <" + USESQUERYTERMSET +"> ?queryTermSet . \n" +
                "        ?queryTermSet ?queryTermP ?queryTermO \n" +
                "    } \n" +
                "} \n";        
    }
    
    String DATASOURCE_BY_GRAPH = "CONSTRUCT { \n" +
            "    ?dataSource ?p ?o . \n" +
            "    ?endpoint ?endpointP ?endpointO . \n" +
            "    ?queryTermSet ?queryTermP ?queryTermO \n" +
            "} WHERE { \n" +
            "    ?dataSource <" + GRAPHURI + "> ?graphURI . \n" +
            "    ?dataSource ?p ?o . \n" +
            "    OPTIONAL { \n" +
            "        ?dataSource <" + USESSPARQLENDPOINT +"> ?endpoint . \n" +
            "        ?endpoint ?endpointP ?endpointO \n" +
            "    } \n" +
            "    OPTIONAL { \n" +
            "        ?dataSource <" + USESQUERYTERMSET +"> ?queryTermSet . \n" +
            "        ?queryTermSet ?queryTermP ?queryTermO \n" +
            "    } \n" +
            "} \n";
    
    public List<DataSourceDescription> listDataSources() { 
        return listDataSources(construct(dataSourcesQuery));
    }
    
    public List<DataSourceDescription> listMergeDataSources() {
        return listDataSources(construct(mergeSourcesQuery));
    }
     
    public List<DataSourceDescription> listPublishDataSources() {
        return listDataSources(construct(publishSourcesQuery));
    }
      
    private List<DataSourceDescription> listDataSources(Model model) {
        List<DataSourceDescription> dataSources = new ArrayList<DataSourceDescription>();
        ResIterator resIt = model.listResourcesWithProperty(
                RDF.type, model.getResource(DATASOURCE));
        while(resIt.hasNext()) {
            Resource res = resIt.next();
            if(res.isURIResource()) {
                dataSources.add(this.getDataSource(res.getURI(), model));
            }
        }
        DataSourcePriorityComparator comp = new DataSourcePriorityComparator();
        Collections.sort(dataSources, comp);
        return dataSources;
    }
    
    private Model construct(String queryStr) {
        return modelConstructor.construct(queryStr);
    }

    public DataSourceDescription getDataSource(String URI) {
        String dataSourceQuery = getAllDataSourcesQuery()
                .replaceAll("\\?dataSource", "<" + URI + ">");
        return getDataSource(URI, construct(dataSourceQuery));
    }
    
    private DataSourceDescription getDataSource(String URI, Model model) {
        DataSourceDescription ds = new DataSourceDescription();
        ds.getConfiguration().setURI(URI);
        ds.getConfiguration().setName(getStringValue(URI, RDFS.label.getURI(), model));
        ds.getConfiguration().setDeploymentURI(getStringValue(URI, DEPLOYMENTURI, model));
        ds.setLastUpdate(getStringValue(URI, LASTUPDATE, model));
        ds.setNextUpdate(getStringValue(URI, NEXTUPDATE, model));
        ds.getConfiguration().setPriority(getIntValue(URI, PRIORITY, model));
        ds.getConfiguration().setResultsGraphURI(getStringValue(URI, GRAPHURI, model)); 
        ds.getConfiguration().setServiceURI(getStringValue(URI, SERVICEURI, model));        
        ds.setUpdateFrequency(DataSourceUpdateFrequency.valueByURI(
                getURIValue(URI, UPDATEFREQUENCY, model)));
        ds.setScheduleAfterURI(getURIValue(URI, SCHEDULEAFTER, model));
        StmtIterator endpit = model.listStatements(model.getResource(URI), 
                model.getProperty(USESSPARQLENDPOINT), (RDFNode) null);
        try {
            while(endpit.hasNext()) {
                Statement endps = endpit.next();
                if(endps.getObject().isURIResource()) {
                    String endpoint = endps.getObject().asResource().getURI();
                    SparqlEndpointParams endpointParams = new SparqlEndpointParams();
                    endpointParams.setEndpointURI(getStringValue(endpoint, ENDPOINTURI, model));
                    endpointParams.setEndpointUpdateURI(getStringValue(endpoint, ENDPOINTUPDATEURI, model));
                    endpointParams.setUsername(getStringValue(endpoint, ENDPOINTUSERNAME, model));
                    endpointParams.setPassword(getStringValue(endpoint, ENDPOINTPASSWORD, model));
                    ds.getConfiguration().setEndpointParameters(endpointParams); 
                    break;
                }                   
            }
        } finally {
            endpit.close();
        }
        StmtIterator qtsit = model.listStatements(model.getResource(URI), 
                model.getProperty(USESQUERYTERMSET), (RDFNode) null);
        List<String> queryTerms = new ArrayList<String>();
        try {
            while(qtsit.hasNext()) {
                Statement qts = qtsit.next();
                if(qts.getObject().isURIResource()) {
                    StmtIterator queryTermIt = qts.getObject().asResource()
                            .listProperties(model.getProperty(QUERYTERM));
                    try {
                        while(queryTermIt.hasNext()) {
                            Statement queryTermStmt = queryTermIt.next();
                            if(queryTermStmt.getObject().isLiteral()) {
                                queryTerms.add(queryTermStmt.getObject()
                                        .asLiteral().getLexicalForm());
                            }
                        }
                    } finally {
                        queryTermIt.close();
                    }
                }                   
            }
        } finally {
            endpit.close();
            ds.getConfiguration().setQueryTerms(queryTerms);
        }
        return ds;
    }
    
    private String getStringValue(String subjectURI, String propertyURI, 
            Model model) {
        StmtIterator sit = model.listStatements(model.getResource(subjectURI), 
                model.getProperty(propertyURI), (RDFNode) null);
        try {
            while(sit.hasNext()) {
                Statement stmt = sit.next();
                if(stmt.getObject().isLiteral()) {
                    return stmt.getObject().asLiteral().getLexicalForm();
                }
            }
            return null;
        } finally {
            sit.close();
        }
    }
    
    private String getURIValue(String subjectURI, String propertyURI, 
            Model model) {
        StmtIterator sit = model.listStatements(model.getResource(subjectURI), 
                model.getProperty(propertyURI), (Resource) null);
        try {
            while(sit.hasNext()) {
                Statement stmt = sit.next();
                if(stmt.getObject().isURIResource()) {
                    return stmt.getObject().asResource().getURI();
                }
            }
            return null;
        } finally {
            sit.close();
        }
    }
    
    private Date getDateValue(String subjectURI, String propertyURI, 
            Model model) {
        StmtIterator sit = model.listStatements(model.getResource(subjectURI), 
                model.getProperty(propertyURI), (RDFNode) null);
        try {
            while(sit.hasNext()) {
                Statement stmt = sit.next();
                if(stmt.getObject().isLiteral()) {
                    Literal lit = stmt.getObject().asLiteral();
                    Object obj = lit.getValue();
                    if(obj instanceof XSDDateTime) {
                        XSDDateTime dateTime = (XSDDateTime) obj;
                        return dateTime.asCalendar().getTime();
                    }
                }
            }
            return null;
        } finally {
            sit.close();
        }
    }
    
    private int getIntValue(String subjectURI, String propertyURI, 
            Model model) {
        StmtIterator sit = model.listStatements(model.getResource(subjectURI), 
                model.getProperty(propertyURI), (RDFNode) null);
        try {
            while(sit.hasNext()) {
                Statement stmt = sit.next();
                if(stmt.getObject().isLiteral()) {
                    Literal lit = stmt.getObject().asLiteral();
                    Object obj = lit.getValue();
                    if(obj instanceof Integer) {
                        Integer intg = (Integer) obj;
                        return intg;
                    }
                }
            }
            return Integer.MAX_VALUE;
        } finally {
            sit.close();
        }
    }

    public DataSourceDescription getDataSourceByGraphURI(String graphURI) {
        String dataSourceQuery = DATASOURCE_BY_GRAPH
                .replaceAll("\\?graphURI", "\"" + graphURI + "\"^^<" + 
                        XSD.anyURI.getURI() + ">");
        log.debug(dataSourceQuery);
        List<DataSourceDescription> dataSources = listDataSources(
                construct(dataSourceQuery));
        if(dataSources.isEmpty()) {
            return null;
        } else {
            return dataSources.get(0);
        }
    }
    
    
    private class DataSourcePriorityComparator implements Comparator<DataSourceDescription> {

        public int compare(DataSourceDescription o1, DataSourceDescription o2) {
            if(o1 == null && o2 != null) {
                return 1;
            } else if (o2 == null && o1 != null) {
                return -1;
            } else if (o1 == null && o1 == null) {
                return 0;
            } else {
                return o1.getConfiguration().getPriority() 
                        - o2.getConfiguration().getPriority();
            }
        }
        
    }
    
}

