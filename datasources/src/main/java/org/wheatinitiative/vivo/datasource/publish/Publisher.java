package org.wheatinitiative.vivo.datasource.publish;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.DataSourceBase;
import org.wheatinitiative.vivo.datasource.DataSourceDescription;
import org.wheatinitiative.vivo.datasource.SparqlEndpointParams;
import org.wheatinitiative.vivo.datasource.VivoVocabulary;
import org.wheatinitiative.vivo.datasource.dao.DataSourceDao;
import org.wheatinitiative.vivo.datasource.postmerge.PostmergeDataSource;
import org.wheatinitiative.vivo.datasource.util.indexinginference.IndexingInference;
import org.wheatinitiative.vivo.datasource.util.sparql.SparqlEndpoint;

/**
 * A data source that takes a WheatVIVO admin app's SPARQL endpoint as an input
 * and publishes data to a public WheatVIVO's endpoint, rewriting individuals
 * to use the highest-priority URI and dropping lower-priority duplicate values
 * for functional properties
 * @author Brian Lowe
 *
 */
public class Publisher extends DataSourceBase implements DataSource {

    private static final Log log = LogFactory.getLog(Publisher.class);
    private static final String GRAPH_FILTER = 
            "    FILTER(!regex(str(?g),\"filegraph\")) \n" +
                    "    FILTER(!regex(str(?g),\"application\")) \n" +
                    "    FILTER(!regex(str(?g),\"tbox\")) \n" +
                    "    FILTER(!regex(str(?g),\"kb-inf\")) \n";
    private static final String KB2 = 
            "http://vitro.mannlib.cornell.edu/default/vitro-kb-2";
    private static final String ADMINAPP_ASSERTIONS = 
            "http://vitro.mannlib.cornell.edu/default/adminappkb2";
    // number of individuals to process before writing results
    private static final int BATCH_SIZE = 2500;
    private static final int PAUSE_BETWEEN_BATCHES = 0 * 1000; // ms
    private static final String DATETIMEVALUE = "http://vivoweb.org/ontology/core#dateTimeValue";
    private static final String DATETIMEINTERVAL = "http://vivoweb.org/ontology/core#dateTimeInterval";
    private static final String HASCONTACTINFO = "http://purl.obolibrary.org/obo/ARG_2000028";
    private static final String POSTMERGE_GRAPH = "http://vitro.mannlib.cornell.edu/a/graph/postmerge";
    Model sameAsTriples = ModelFactory.createDefaultModel();
    Map<String, String> homeGraphCache = new HashMap<String, String>();
    

    private DataSourceDao dataSourceDao;
    private String timestamp;

    protected DataSourceDao getDataSourceDao() {
        if(dataSourceDao == null) {
            this.dataSourceDao = new DataSourceDao(getSparqlEndpoint());
        }
        return this.dataSourceDao;
    }

    @Override
    protected void runIngest() throws InterruptedException {
        Date currentDateTime = Calendar.getInstance().getTime();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        this.timestamp = "--" + df.format(currentDateTime);
        SparqlEndpoint sourceEndpoint = getSourceEndpoint();
        SparqlEndpoint destinationEndpoint = getSparqlEndpoint();
        IndexingInference inf = null;
        if(destinationEndpoint.getSparqlEndpointParams().getEndpointURI() != null) {
            inf = new IndexingInference(destinationEndpoint);    
        }        
        if(inf != null && inf.isAvailable()) {
            inf.unregisterReasoner();
            inf.unregisterSearchIndexer();
            log.info("Unregistered reasoner and search indexer");
        } else {
            log.warn("IndexingInferenceService not available on destination endpoint");
        }
        boolean errorOccurred = false;
        try {
            Set<String> functionalPropertyURIs = getFunctionalPropertyURIs();  
            log.info("Getting graph preference list");
            List<String> graphURIPreferenceList = getGraphURIPreferenceList(
                    sourceEndpoint);
            log.info("Graph URI preference list: " + graphURIPreferenceList);
            log.info("Caching sameAs triples");
            sameAsTriples = getSameAsTriples(sourceEndpoint);
            log.info(sameAsTriples.size() + " cached sameAs triples");
            log.info("Caching home graphs");
            homeGraphCache = getHomeGraphMap(sourceEndpoint, graphURIPreferenceList);
            log.info(homeGraphCache.size() + " cached home graphs");
            log.info("Starting to publish");
            this.getStatus().setMessage("publishing individuals");
            // iterate through data source graphs in order of priority
            Map<String, Model> buffer = new HashMap<String, Model>();
            int individualCount = 0;
            Set<String> completedIndividuals = new HashSet<String>();
            try {
                for(String graphURI : graphURIPreferenceList) {
                    if(graphURI == null) {
                        continue;
                    }                     
                    IndividualURIIterator indIt = new IndividualURIIterator(
                            sourceEndpoint, graphURI);
                    while(indIt.hasNext()) {
                        long start = System.currentTimeMillis();
                        individualCount++;
                        String individualURI = indIt.next();
                        if(completedIndividuals.contains(individualURI)) {
                            if(!indIt.hasNext()) {
                                flushBufferToDestination(buffer, this.timestamp);
                            }
                            continue;
                        }
                        List<String> sameAsURIs = getSameAsURIList(individualURI, sourceEndpoint);
                        List<Quad> individualQuads = getIndividualQuads(individualURI, 
                                sourceEndpoint);
//                        for (String sameAsURI : sameAsURIs) {
//                            sameAsCache.put(sameAsURI, individualURI);
//                        }
                        Map<String, Model> quadStore = new HashMap<String, Model>();   
                        log.debug("Adding " + individualURI + " to store");
                        addQuadsToStore(
                                rewrite(individualQuads, graphURIPreferenceList, 
                                        sourceEndpoint), quadStore);
                        report(quadStore);
                        log.debug("same as URIs " + sameAsURIs);
                        // don't waste memory recording an individual if we won't 
                        // encounter it again later
                        if(quadStore.keySet().size() <= 1 && !sameAsURIs.isEmpty()) {
                            completedIndividuals.add(individualURI);
                        }
                        for (String sameAsURI : sameAsURIs) {
                            log.debug("Adding " + sameAsURI + " to store");
                            completedIndividuals.add(sameAsURI);
                            List<Quad> sameAsQuads = getIndividualQuads(
                                    sameAsURI, sourceEndpoint);
                            addQuadsToStore(
                                    rewrite(sameAsQuads, graphURIPreferenceList, 
                                            sourceEndpoint), quadStore);
                            report(quadStore);
                        }
                        dedupFunctionalProperties(quadStore, functionalPropertyURIs, 
                                graphURIPreferenceList);  
                        filterIrrelevantGraphs(quadStore, graphURIPreferenceList);
                        // TODO filter non-VIVO predicate namespaces?
                        addQuadStoreToBuffer(quadStore, buffer);
                        long duration = System.currentTimeMillis() - start;
                        if (duration > 1000) {
                            log.info(duration + " ms to process individual " + individualURI);
                        }
                        this.getStatus().setProcessedRecords(completedIndividuals.size());
                        if(individualCount % BATCH_SIZE == 0 || !indIt.hasNext()) {
                            flushBufferToDestination(buffer, this.timestamp);    
                        }
                    }
                }
            } catch(Throwable t) {
                log.error(t, t);
                errorOccurred = true;
            }
            if(!errorOccurred) {
                this.getStatus().setMessage("clearing old data");
                cleanUpDestination(sourceEndpoint, destinationEndpoint, timestamp);
            }
        } finally {
            if(inf != null && inf.isAvailable()) {
                inf.registerReasoner();
                log.info("Registered reasoner");
                inf.registerSearchIndexer();
                log.info("Registered search indexer");
                inf.recompute();
                log.info("Recomputing inferences");
                this.getStatus().setMessage("Recomputing inferences");
                do {
                    Thread.sleep(10000);
                } while (inf.isReasonerIsRecomputing());
                // indexing kicked off after requested inference recompute
                this.getStatus().setMessage("Rebuilding search index");
                do {
                    Thread.sleep(10000);
                } while (inf.isReasonerIsRecomputing());
                if(!errorOccurred) {
                    this.getStatus().setMessage("augmenting data via additional construct queries");
                    PostmergeDataSource postmerge = new PostmergeDataSource();
                    postmerge.setConfiguration(this.getConfiguration());
                    postmerge.getConfiguration().setResultsGraphURI(POSTMERGE_GRAPH);
                    postmerge.runIngest();
                }
            } else {
                log.warn("IndexingInferenceService not available on destination endpoint");
            }
        }
        log.info("ending");
    }

    public String getTimestamp() {
        return this.timestamp;
    }

    private void report(Map<String, Model> quadStore) {
        if(log.isDebugEnabled()) {
            for(String graph : quadStore.keySet()) {
                log.debug(graph + " has " + quadStore.get(graph).size() + " triples.");
            }
        }
    }

    private List<String> getGraphURIPreferenceList(
            SparqlEndpoint sourceEndpoint) {
        List<String> graphURIPreferenceList = new ArrayList<String>();
        graphURIPreferenceList.add(KB2);
        graphURIPreferenceList.add(ADMINAPP_ASSERTIONS);
        for(DataSourceDescription dataSource : new DataSourceDao(
                sourceEndpoint).listDataSources()) {
            String graphURI = 
                    dataSource.getConfiguration().getResultsGraphURI();
            List<String> sourceGraphURIs = this.getGraphsWithBaseURI(graphURI, sourceEndpoint);
            if(sourceGraphURIs.size() == 0) {
                continue;
            }
            Collections.sort(sourceGraphURIs);
            String lastestVersionURI = sourceGraphURIs.get(sourceGraphURIs.size() - 1);
            graphURI = lastestVersionURI;
            if(graphURI != null) {
                graphURIPreferenceList.add(graphURI);
            } else {
                throw new RuntimeException("Graph URI is null for " + 
                        dataSource.getConfiguration().getName());
            }
        }
        return graphURIPreferenceList;
    }

    Map<String, List<String>> uriSetCache = new HashMap<String, List<String>>();

    /**
     * 
     * @return list via reasoned transitive closure of owl:sameAs statements
     * from endpoint
     */
    private List<String> getSameAsURIList(String individualURI, SparqlEndpoint endpoint) {
        if(uriSetCache.containsKey(individualURI)) {
            return uriSetCache.get(individualURI);            
        } else {
            Model m = getSameAsModel(individualURI, endpoint);
            NodeIterator nit = m.listObjects();
            List<String> uris = new ArrayList<String>();
            while(nit.hasNext()) {
                RDFNode node = nit.next();
                if(node.isURIResource()) {
                    uris.add(node.asResource().getURI());    
                }
            }
            if(!uris.contains(individualURI)) {
                uris.add(individualURI);
            }    
            uriSetCache.put(individualURI, uris);
            return uris;
        }
    }

    /**
     * 
     * @return OntModel with owl:sameAs statements
     * from endpoint
     */
    private Model getSameAsModel(String individualURI, SparqlEndpoint endpoint) {
        String sameAsQuery = "CONSTRUCT { \n" +
                "    <" + individualURI + "> <" + OWL.sameAs.getURI() + "> ?ind2 \n" +
                "} WHERE { \n" +
                "    <" + individualURI + "> <" + OWL.sameAs.getURI() + "> ?z . \n" +
                "    ?ind2 <" + OWL.sameAs.getURI() + "> ?z \n" +
                "    FILTER (?ind2 != <" + individualURI + ">) \n" +
                "} \n";
        //return endpoint.construct(sameAsQuery);
        QueryExecution qe = QueryExecutionFactory.create(sameAsQuery, sameAsTriples);
        try {
            return qe.execConstruct();
        } finally {
            qe.close();
        }
    }

    private static Map<String, String> sameAsCache = new HashMap<String, String>();

    private String getSameAs(String individualURI, 
            List<String> graphURIPreferenceList, SparqlEndpoint endpoint) {
        if(sameAsCache.containsKey(individualURI)) {
            log.debug("Returning sameAs for " + individualURI + " from cache");
            return sameAsCache.get(individualURI);
        }
        long start = System.currentTimeMillis();
        //long start = System.currentTimeMillis();
        String uriToMapTo = individualURI;
        List<String> sameAsURIs = getSameAsURIList(individualURI, endpoint);
        String currentGraph = getHomeGraph(uriToMapTo, endpoint, graphURIPreferenceList);
        for(String sameAsURI : sameAsURIs) {            
            String candidateGraph = getHomeGraph(sameAsURI, endpoint, graphURIPreferenceList);
            boolean isHigherPriority = isHigherPriorityThan(candidateGraph, currentGraph, 
                    graphURIPreferenceList);
            if(log.isDebugEnabled()) {
                log.debug(candidateGraph + (isHigherPriority ? " IS higher than " : " IS NOT higher than ") + currentGraph);
            }
            boolean currentHasId = uriToMapTo.contains("cv-") && !uriToMapTo.contains("cv-t-");
            boolean candidateHasId = sameAsURI.contains("cv-") && !sameAsURI.contains("cv-t-");
            if(isHigherPriorityThan(candidateGraph, currentGraph, 
                    graphURIPreferenceList)) {
                uriToMapTo = sameAsURI;
                currentGraph = candidateGraph;
            } else if ( (candidateGraph == null && currentGraph == null)
                    || (candidateGraph != null && candidateGraph.equals(currentGraph))) {
                if(!currentHasId && candidateHasId) {
                    uriToMapTo = sameAsURI;
                } else if(!currentHasId && (sameAsURI.compareTo(uriToMapTo) < 0)) {
                    // pick alphabetically-lower URIs within equal-level graphs
                    uriToMapTo = sameAsURI;
                }
            }                
        }
        //if(System.currentTimeMillis() - start > 2) {
        sameAsCache.put(individualURI, uriToMapTo);
        for(String sameAsURI : sameAsURIs) {
            if(!sameAsCache.containsKey(sameAsURI)) {
              sameAsCache.put(sameAsURI, uriToMapTo);
            }
        }
        //}
        long duration = System.currentTimeMillis() - start;
        if(duration > 10000) {
            log.info(duration + " to find sameAs for " + individualURI);    
        }        
        log.debug("sameAs:");
        log.info(individualURI + " ===> " + uriToMapTo);
        return uriToMapTo;
    }

    private String getHomeGraph(String individualURI, SparqlEndpoint endpoint,
            List<String> graphURIPreferenceList) {
//        String graphURI = null;
//        String homeGraphQuery = "SELECT DISTINCT ?graph WHERE { \n" +
//                //                "    { <" + individualURI + "> <" + OWL.sameAs.getURI() + "> ?something } \n" +
//                //                "    UNION \n" +
//                //                "    { ?something2 <" + OWL.sameAs.getURI() + "> <" + individualURI + "> } \n" +
//                "    GRAPH ?graph { <" + individualURI + "> a ?typeDeclaration } \n" +
//                "} \n";
//        ResultSet homeGraphRs = endpoint.getResultSet(homeGraphQuery);
//        while(homeGraphRs.hasNext()) {
//            QuerySolution qsoln = homeGraphRs.next();
//            RDFNode g = qsoln.get("graph");
//            if(!g.isURIResource()) {
//                continue;
//            }
//            String graph = g.asResource().getURI();
//            if(graphURI == null) {
//                graphURI = graph;
//            } else {
//                if(isHigherPriorityThan(graph, graphURI, 
//                        graphURIPreferenceList)) {
//                    graphURI = graph;
//                }
//            }
//        }
        String graphURI = homeGraphCache.get(individualURI);
        log.debug("Home graph for " + individualURI + " is " + graphURI);
        return graphURI;
    }  

    /**
     * @param graphURI1
     * @param graphURI2
     * @return true if graphURI1 is higher priority than graphURI2, else false
     */
    private boolean isHigherPriorityThan(String graphURI1, String graphURI2, 
            List<String> graphURIPreferenceList) {
        if(graphURI2 == null && graphURI1 != null) {
            return true;
        } else if (graphURI1 == null) {
            return false;
        }
        int graphURI1position = getListPosition(graphURI1, graphURIPreferenceList);
        int graphURI2position = getListPosition(graphURI2, graphURIPreferenceList);
        return(graphURI1position < graphURI2position);
    }

    /**
     * 
     * @param graphURI
     * @param graphURIlist
     * @return integer position of graphURI in graphURIlist, or MAXINT if not 
     * found
     */
    private int getListPosition(String graphURI, List<String> graphURIlist) {
        int i = 0;
        Iterator<String> it = graphURIlist.iterator();
        while(it.hasNext()) {
            String s = it.next();
            if(s.equals(graphURI)) {
                return i;
            }
            i++;
        }
        return Integer.MAX_VALUE;
    }

    private void filterIrrelevantGraphs(Map<String, Model> quadStore, 
            List<String> graphURIPreferenceList) {
        for(String graphURI : quadStore.keySet()) {
            if(!graphURIPreferenceList.contains(graphURI)) {
                quadStore.get(graphURI).removeAll();
            }
        }
    }

    protected void dedupFunctionalProperties(Map<String, Model> quadStore, 
            Set<String> functionalPropertyURIs, 
            List<String> graphURIPreferenceList) {
        Set<String> completedProperties = new HashSet<String>();
        for(String graphURI : graphURIPreferenceList) {
            Model m = quadStore.get(graphURI);
            if(m == null) {
                continue;
            }
            Set<Property> predicateSet = getPredicatesInUse(m);
            for(Property predicate : predicateSet) {
                if(completedProperties.contains(predicate.getURI())) {
                    log.debug("Removing all " + predicate.getURI() + " in " + graphURI);
                    m.removeAll(null, predicate, (RDFNode) null);
                } else if(functionalPropertyURIs.contains(predicate.getURI())) {
                    LinkedList<Statement> duplicates = new LinkedList<Statement>();
                    StmtIterator sit = m.listStatements(
                            null, predicate, (RDFNode) null);
                    while(sit.hasNext()) {
                        duplicates.add(sit.next());
                    }
                    Collections.sort(duplicates, new StatementSorter());
                    // drop the first from the duplicate list since we want
                    // to retain it in the model
                    duplicates.poll();
                    log.debug("Removing " + duplicates + " " + predicate.getURI() + " from " + graphURI);
                    m.remove(duplicates);
                    completedProperties.add(predicate.getURI());
                    log.debug("Completed property " + predicate.getURI() + " on graph " + graphURI);
                }
            }
        }
    }

    private class StatementSorter implements Comparator<Statement> {

        public int compare(Statement stmt1, Statement stmt2) {
            return getObjectAsString(stmt1).compareTo(getObjectAsString(stmt2));
        }

        private String getObjectAsString(Statement stmt) {
            if(stmt.getObject().isLiteral()) {
                return stmt.getObject().asLiteral().toString();
            } else if(stmt.getObject().isURIResource()) {
                return stmt.getObject().asResource().getURI();
            } else {
                return "";
            }
        }

    }

    private Set<Property> getPredicatesInUse(Model model) {
        Set<Property> predSet = new HashSet<Property>();
        // we could issue a query if iterating is too slow
        StmtIterator sit = model.listStatements();
        while(sit.hasNext()) {
            Statement s = sit.next();
            predSet.add(s.getPredicate());
        }
        return predSet;
    }

    private void cleanUpDestination(SparqlEndpoint sourceEndpoint, 
            SparqlEndpoint destinationEndpoint, String timestamp) {
        // After main loop, clear any destination graphs that do not 
        // exist in the source (except kb2).
        // Because the timestamp of publishing has been appended to the graphs
        // in the destination endpoint, we will also append the same timestamp
        // to the source graphs for comparison.  That way we will only clear
        // out old versions of the graphs and not the same graphs we just 
        // wrote to the destination.
        List<String> sourceGraphURIs = getGraphURIsInEndpoint(sourceEndpoint);
        for(int i = 0; i < sourceGraphURIs.size(); i++) {
            sourceGraphURIs.set(i, sourceGraphURIs.get(i) + timestamp);
        }
        sourceGraphURIs.add(ADMINAPP_ASSERTIONS + timestamp);
        List<String> destinationGraphURIs = getGraphURIsInEndpoint(destinationEndpoint);
        for(String destGraphURI : destinationGraphURIs) {
            if(!sourceGraphURIs.contains(destGraphURI)) {
                if(!KB2.equals(destGraphURI)) {
                    log.info("Clearing destination graph " + destGraphURI);
                    destinationEndpoint.clearGraph(destGraphURI);
                }
            }
        }
    }

    private List<String> getGraphURIsInEndpoint(SparqlEndpoint endpoint) {
        List<String> graphURIs = new ArrayList<String>();
        String graphQuery = "SELECT DISTINCT ?g WHERE { \n" +
                "    GRAPH ?g { ?s ?p ?o } \n" +
                GRAPH_FILTER + 
                "} \n";
        ResultSet rs = endpoint.getResultSet(graphQuery);
        while(rs.hasNext()) {
            QuerySolution qsoln = rs.next();
            graphURIs.add(qsoln.get("g").asResource().getURI());
        }
        return graphURIs;
    }

    private void addQuadStoreToBuffer(Map<String, Model> quadStore, 
            Map<String, Model> buffer) {
        for(String graphURI : quadStore.keySet()) {
            Model source = quadStore.get(graphURI);
            Model destination = buffer.get(graphURI);
            if(destination == null) {
                destination = ModelFactory.createDefaultModel();
                buffer.put(graphURI, destination);
            }
            destination.add(source);
        }
    }

    /**
     * Write a buffer of quads to the destination SPARQL endpoint, appending
     * the current timestamp to the end of each graph URI.
     * @param buffer
     * @param timestamp
     */
    private void flushBufferToDestination(Map<String, Model> buffer, 
            String timestamp) {
        writeQuadStoreToDestination(buffer, timestamp);
        buffer.clear();
    }

    /**
     * Pause to let the remote VIVO catch up with search index updates, etc.
     */
    private void pause() {
        try {
            Thread.sleep(PAUSE_BETWEEN_BATCHES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Write a quad store to the destination SPARQL endpoint, appending
     * an optional timestamp string to the end of each graph URI.
     * @param quadStore
     * @param timestamp may be null.
     */
    private void writeQuadStoreToDestination(Map<String, Model> quadStore, 
            String timestamp) {
        for(String graphURI : quadStore.keySet()) {
            Model m = quadStore.get(graphURI);
            if(m.size() == 0) {
                continue;
            }
            if(timestamp != null) {
                graphURI += timestamp;
            }
            log.info("Writing " + m.size() + " triples to graph " + graphURI);
            long start = System.currentTimeMillis();
            getSparqlEndpoint().writeModel(m, graphURI);
            log.info(System.currentTimeMillis() - start + " ms to write.");
            pause();
        }
    }

    private void addQuadsToStore(List<Quad> quads, Map<String, Model> store) {
        for(Quad quad : quads) {
            Model m = store.get(quad.getGraphURI());
            if(m == null) {
                m = ModelFactory.createDefaultModel();
                store.put(quad.getGraphURI(), m);
            }
            m.add(m.getResource(quad.getSubjectURI()), 
                    m.getProperty(quad.getPredicateURI()),
                    quad.getObject());
        }
    }

    /**
     * Rewrite all subjects in the list of quads with the supplied URI
     * @return list of rewritten quads
     */
    private List<Quad> rewrite(List<Quad> quads, 
            List<String> graphURIPreferenceList, SparqlEndpoint endpoint) {
        List<Quad> rewrittenQuads = new ArrayList<Quad>();
        for(Quad quad : quads) {
            String subject = quad.getSubjectURI();
            String rewrittenSubject = getSameAs(subject, graphURIPreferenceList, endpoint);
            if(rewrittenSubject != null) {
                subject = rewrittenSubject;
            }
            RDFNode object = quad.getObject();
            if(object.isURIResource() && !RDF.type.getURI().equals(quad.getPredicateURI())) {
                String rewrittenObject = getSameAs(object.asResource().getURI(),
                        graphURIPreferenceList, endpoint);
                if(rewrittenObject != null) {
                    object = ResourceFactory.createResource(rewrittenObject);
                }
            }
            rewrittenQuads.add(new Quad(quad.getGraphURI(), subject, 
                    quad.getPredicateURI(), object));
        }
        return rewrittenQuads;
    }

    private List<Quad> getIndividualQuads(String individualURI, 
            SparqlEndpoint sparqlEndpoint) {
        String quadsQuery = "SELECT ?g ?s ?p ?o WHERE { \n" +
                "    GRAPH ?g { <" + individualURI + "> ?p ?o } \n" +
                "    BIND(<" + individualURI + "> AS ?s) \n" +
                "    FILTER(?p != <" + OWL.sameAs.getURI() + ">) \n" +
                GRAPH_FILTER +
                "} ORDER BY ?p ?o";
        log.debug(quadsQuery);
        List<Quad> quads = new ArrayList<Quad>();
        long start = System.currentTimeMillis();
        ResultSet quadResults = sparqlEndpoint.getResultSet(quadsQuery);
        log.debug(System.currentTimeMillis() - start + " to do query");
        while(quadResults.hasNext()) {
            QuerySolution quadSoln = quadResults.next();            
            String graphURI = quadSoln.get("g").asResource().getURI();
            if(KB2.equals(graphURI)) {
                graphURI = ADMINAPP_ASSERTIONS;
            }
            String subjectURI = quadSoln.get("s").asResource().getURI();
            String predicateURI = quadSoln.get("p").asResource().getURI();
            RDFNode object = quadSoln.get("o");
            quads.add(new Quad(graphURI, subjectURI, predicateURI, object));
        }
        log.debug(System.currentTimeMillis() - start + " to get individual quads");
        return quads;
    }

    private class IndividualURIIterator implements Iterator<String> {

        private static final int INDIVIDUAL_BATCH_SIZE = 100000;
        int individualOffset = 0;

        Queue<String> currentBatch = null;

        private SparqlEndpoint sparqlEndpoint;
        private String graphURI;

        public IndividualURIIterator(SparqlEndpoint sparqlEndpoint) {
            this.sparqlEndpoint = sparqlEndpoint;        
        }

        public IndividualURIIterator(SparqlEndpoint sparqlEndpoint, String graphURI) {
            this(sparqlEndpoint);
            this.graphURI = graphURI;
        }

        public boolean hasNext() {
            if(currentBatch == null || currentBatch.isEmpty()) {
                fetchNextBatch();
                return !currentBatch.isEmpty();
            } else {
                return true;
            }
        }

        public String next() {
            if(!hasNext()) {
                throw new RuntimeException("No more elements");
            } else {
                return currentBatch.poll();
            }
        }

        private void fetchNextBatch() {
            String individualsBatch = "SELECT DISTINCT ?s WHERE { \n" +
                    ((graphURI != null) ? "GRAPH <" + graphURI + "> { \n" : "") +
                    "        ?s a ?o \n" +
                    ((graphURI != null) ? "} \n" : "") +
                    "} ORDER BY ?s LIMIT " + INDIVIDUAL_BATCH_SIZE + 
                    " OFFSET " + individualOffset;
            individualOffset += INDIVIDUAL_BATCH_SIZE;
            Queue<String> batch = new LinkedList<String>();
            ResultSet rs = sparqlEndpoint.getResultSet(individualsBatch);
            while(rs.hasNext()) {
                QuerySolution soln = rs.next();
                RDFNode s = soln.get("s");
                if(s.isURIResource()) {
                    batch.add(s.asResource().getURI());
                }
            }
            this.currentBatch = batch;
        }        
    }

    private class Quad {

        private String graphURI;
        private String subjectURI;
        private String predicateURI;
        private RDFNode object;

        public Quad(String graphURI, 
                String subjectURI, String predicateURI, RDFNode object) {
            this.graphURI = graphURI;
            this.subjectURI = subjectURI;
            this.predicateURI = predicateURI;
            this.object = object;
        }

        public String getGraphURI() {
            return this.graphURI;
        }

        public String getSubjectURI() {
            return this.subjectURI;
        }

        public String getPredicateURI() {
            return this.predicateURI;
        }

        public RDFNode getObject() {
            return this.object;
        }

    }

    protected SparqlEndpoint getSourceEndpoint() {
        String sourceServiceURI = this.getConfiguration().getServiceURI();
        if(!sourceServiceURI.endsWith("/")) {
            sourceServiceURI += "/";
        }
        SparqlEndpointParams params = new SparqlEndpointParams();
        params.setEndpointURI(sourceServiceURI + "api/sparqlQuery");
        params.setEndpointUpdateURI(sourceServiceURI + "api/sparqlUpdate");
        params.setUsername(
                this.getConfiguration().getEndpointParameters().getUsername());
        params.setPassword(
                this.getConfiguration().getEndpointParameters().getPassword());
        return new SparqlEndpoint(params);
    }

    protected Set<String> getFunctionalPropertyURIs() {
        Set<String> funcPropSet = new HashSet<String>();
        String funcPropsQuery = "SELECT DISTINCT ?funcProp WHERE { \n" +
                "    ?funcProp a <" + OWL.FunctionalProperty.getURI() + "> . \n" +
                "} \n";
        ResultSet rs = getSparqlEndpoint().getResultSet(funcPropsQuery);
        while(rs.hasNext()) {
            QuerySolution qsoln = rs.next();
            funcPropSet.add(qsoln.getResource("funcProp").getURI());
        }
        // Also treat rdfs:label as a functional property
        funcPropSet.add(RDFS.label.getURI());
        funcPropSet.add(DATETIMEVALUE);
        funcPropSet.add(DATETIMEINTERVAL);
        funcPropSet.add(HASCONTACTINFO);
        funcPropSet.add(VivoVocabulary.VIVO + "rank");
        funcPropSet.add("http://www.w3.org/2006/vcard/ns#country>");       
        funcPropSet.add("http://www.w3.org/2006/vcard/ns#email>");
        funcPropSet.add("http://www.w3.org/2006/vcard/ns#familyName");
        funcPropSet.add("http://www.w3.org/2006/vcard/ns#givenName");
        funcPropSet.add("http://www.w3.org/2006/vcard/ns#hasAddress");
        funcPropSet.add("http://www.w3.org/2006/vcard/ns#hasName");
        funcPropSet.add("http://www.w3.org/2006/vcard/ns#hasTitle");
        funcPropSet.add("http://www.w3.org/2006/vcard/ns#locality");
        funcPropSet.add("http://www.w3.org/2006/vcard/ns#postalCode");
        funcPropSet.add("http://www.w3.org/2006/vcard/ns#region");
        funcPropSet.add("http://www.w3.org/2006/vcard/ns#streetAddress");
        funcPropSet.add("<http://www.w3.org/2006/vcard/ns#telephone");
        funcPropSet.add("<http://www.w3.org/2006/vcard/ns#title");
        funcPropSet.add("<http://www.w3.org/2006/vcard/ns#url");
        return funcPropSet;
    }
    
    private Model getSameAsTriples(SparqlEndpoint sourceEndpoint) {
        String queryStr = "PREFIX owl:      <http://www.w3.org/2002/07/owl#>\n"
                + "CONSTRUCT {\n"
                + "  ?x owl:sameAs ?y\n"
                + "} WHERE {\n"
                + "  ?x owl:sameAs ?y\n"
                + "  FILTER(?x != ?y)\n"
                + "}";
        return sourceEndpoint.construct(queryStr);
    }
    
    /**
     * @param endpoint
     * @return map of individual URI to URI of the highest-priority graph in 
     * which it has a type declaration
     */ 
    private Map<String, String> getHomeGraphMap(SparqlEndpoint endpoint, 
            List<String> graphURIPreferenceList) {
        Map<String, String> individualToGraphMap = new HashMap<String, String>();
        String homeGraphQuery = "SELECT ?ind ?graph WHERE { \n" +
                "    GRAPH ?graph { ?ind a ?typeDeclaration } \n" +
                "    FILTER(?graph != <http://vitro.mannlib.cornell.edu/default/vitro-kb-inf>) \n" +
                "} \n";
        ResultSet homeGraphRs = endpoint.getResultSet(homeGraphQuery);
        while(homeGraphRs.hasNext()) {
            QuerySolution qsoln = homeGraphRs.next();
            RDFNode n = qsoln.get("ind");
            if(!n.isURIResource()) {
                continue;
            }
            String ind = n.asResource().getURI();
            RDFNode g = qsoln.get("graph");
            if(!g.isURIResource()) {
                continue;
            }
            String graph = g.asResource().getURI();
            String currentGraph = individualToGraphMap.get(ind);
            if(currentGraph == null) {
                individualToGraphMap.put(ind, graph);
            } else {
                if(isHigherPriorityThan(graph, currentGraph, 
                        graphURIPreferenceList)) {
                    individualToGraphMap.put(ind, graph);
                }
            }

        }
        return individualToGraphMap;
    }

    //    private List<String> getSameAsURIs(String individualURI, 
    //            Map<String, Model> quadStore) {
    //        List<String> sameAsURIs = new ArrayList<String>();
    //        for(Model model : quadStore.values()) {
    //            NodeIterator nit = model.listObjectsOfProperty(
    //                    model.getResource(individualURI), OWL.sameAs);
    //            while(nit.hasNext()) {
    //                RDFNode node = nit.next();
    //                if(node.isURIResource()) {
    //                    sameAsURIs.add(node.asResource().getURI());
    //                }
    //            }
    //        }
    //        return sameAsURIs;
    //    }

    @Override
    public Model getResult() {
        // This method is not used with this data source.
        return ModelFactory.createDefaultModel();
    }

}
