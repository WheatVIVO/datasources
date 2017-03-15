package org.wheatinitiative.vivo.datasource.publish;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.DataSourceBase;
import org.wheatinitiative.vivo.datasource.DataSourceDescription;
import org.wheatinitiative.vivo.datasource.SparqlEndpointParams;
import org.wheatinitiative.vivo.datasource.dao.DataSourceDao;
import org.wheatinitiative.vivo.datasource.util.sparql.SparqlEndpoint;

import com.hp.hpl.jena.ontology.Individual;
import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
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
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDFS;

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
    // number of individuals to process before writing results
    private static final int BATCH_SIZE = 500;
    
    private DataSourceDao dataSourceDao;
    private Set<String> completedIndividuals = new HashSet<String>();
    
    protected DataSourceDao getDataSourceDao() {
        if(dataSourceDao == null) {
            this.dataSourceDao = new DataSourceDao(getSparqlEndpoint());
        }
        return this.dataSourceDao;
    }
    
    @Override
    protected void runIngest() {
        SparqlEndpoint sourceEndpoint = getSourceEndpoint();
        Set<String> functionalPropertyURIs = getFunctionalPropertyURIs();      
        emptyDestination();
        log.info("building maps");
        List<String> graphURIPreferenceList = new ArrayList<String>();
        graphURIPreferenceList.add(KB2);
        for(DataSourceDescription dataSource : new DataSourceDao(
                sourceEndpoint).listDataSources()) {
            graphURIPreferenceList.add(
                    dataSource.getConfiguration().getResultsGraphURI());
        }
        OntModel sameAsModel = getSameAsModel(sourceEndpoint);
        Map<String, String> sameAsMap = getSameAsMap(graphURIPreferenceList, 
                sourceEndpoint, sameAsModel);
        log.info("Starting to publish");        
        // iterate through data source graphs in order of priority
        Map<String, Model> buffer = new HashMap<String, Model>();
        int individualCount = 0;
        for(String graphURI : graphURIPreferenceList) {
            if(graphURI == null) {
                continue;
            }
            IndividualURIIterator indIt = new IndividualURIIterator(
                    sourceEndpoint, graphURI);
            while(indIt.hasNext()) {
                individualCount++;
                String individualURI = indIt.next();
                if(completedIndividuals.contains(individualURI)) {
                    continue;
                }
                List<Quad> individualQuads = getIndividualQuads(individualURI, 
                        sourceEndpoint);
                Map<String, Model> quadStore = new HashMap<String, Model>();                
                addQuadsToStore(rewrite(individualQuads, sameAsMap), quadStore);
                List<String> sameAsURIs = getSameAsURIs(individualURI, 
                        sameAsModel);
                // don't waste memory recording an individual if we won't 
                // encounter it again later
                if(quadStore.keySet().size() <= 1 && !sameAsURIs.isEmpty()) {
                    completedIndividuals.add(individualURI);
                }
                for (String sameAsURI : sameAsURIs) {
                    completedIndividuals.add(sameAsURI);
                    List<Quad> sameAsQuads = getIndividualQuads(
                            sameAsURI, sourceEndpoint);
                    addQuadsToStore(rewrite(
                            sameAsQuads, sameAsMap), quadStore);
                }
                dedupFunctionalProperties(quadStore, functionalPropertyURIs, 
                        graphURIPreferenceList);  
                filterIrrelevantGraphs(quadStore, graphURIPreferenceList);
                // TODO filter non-VIVO predicate namespaces?
                addQuadStoreToBuffer(quadStore, buffer);
                if(individualCount % BATCH_SIZE == 0 || !indIt.hasNext()) {
                    flushBufferToDestination(buffer);    
                }
            }
        }
        log.info("ending");
    }
    
    /**
     * 
     * @return OntModel with reasoned transitive closure of owl:sameAs statements
     * from endpoint
     */
    private OntModel getSameAsModel(SparqlEndpoint endpoint) {
        OntModel ontModel = ModelFactory.createOntologyModel(
                OntModelSpec.OWL_MEM_MINI_RULE_INF);
        String sameAsQuery = "CONSTRUCT { \n" +
                "    ?ind1 a <" + OWL.Thing + "> . \n" +
                "    ?ind2 a <" + OWL.Thing + "> . \n" +
                "    ?ind1 <" + OWL.sameAs.getURI() + "> ?ind2 \n" +
                "} WHERE { \n" +
                "    ?ind1 <" + OWL.sameAs.getURI() + "> ?ind2 \n" +
                "} \n";
        ontModel.add(endpoint.construct(sameAsQuery));
        return ontModel;
    }
    
    private Map<String, String> getSameAsMap(
            List<String> graphURIPreferenceList, SparqlEndpoint endpoint, 
            OntModel sameAsModel) {
        Map<String, String> homeGraphMap = getHomeGraphMap(
                endpoint, graphURIPreferenceList);        
        Map<String, String> sameAsMap = new HashMap<String, String>();
        Iterator<Individual> indIt = sameAsModel.listIndividuals();
        while(indIt.hasNext()) {
            Individual ind = indIt.next();
            if(ind.isAnon()) {
                continue;
            }
            String uriToMapTo = ind.getURI();
            StmtIterator sit = sameAsModel.listStatements(
                    ind, OWL.sameAs, (Resource) null);
            while(sit.hasNext()) {
                Statement stmt = sit.next();
                if(stmt.getObject().isAnon()) {
                    continue;
                }
                String sameAsURI = stmt.getObject().asResource().getURI();
                String currentGraph = homeGraphMap.get(uriToMapTo);
                String candidateGraph = homeGraphMap.get(sameAsURI);
                if(this.isHigherPriorityThan(candidateGraph, currentGraph, 
                        graphURIPreferenceList)) {
                    uriToMapTo = sameAsURI;
                } else {
                }
            }
            sameAsMap.put(ind.getURI(), uriToMapTo);
        }
        log.info("sameAs:");
        for(String uri : sameAsMap.keySet()) {
            log.info(uri + " ===> " + sameAsMap.get(uri));
        }
        return sameAsMap;
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
                   "    { ?ind <" + OWL.sameAs.getURI() + "> ?something } \n" +
                   "    UNION \n" +
                   "    { ?something2 <" + OWL.sameAs.getURI() + "> ?ind } \n" +
                   "    GRAPH ?graph { ?ind a ?typeDeclaration } \n" +
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
    
    /**
     * @param graphURI1
     * @param graphURI2
     * @return true if graphURI1 is higher priority than graphURI2, else false
     */
    private boolean isHigherPriorityThan(String graphURI1, String graphURI2, 
            List<String> graphURIPreferenceList) {
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
    
    private void dedupFunctionalProperties(Map<String, Model> quadStore, 
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
                    log.debug("Removing " + duplicates.size() + " " + predicate.getURI() + " from " + graphURI);
                    m.remove(duplicates);
                    completedProperties.add(predicate.getURI());
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
    
    private void emptyDestination() {
        String graphQuery = "SELECT DISTINCT ?g WHERE { \n" +
                            "    GRAPH ?g { ?s ?p ?o } \n" +
                            GRAPH_FILTER + 
                            "} \n";
        ResultSet rs = getSparqlEndpoint().getResultSet(graphQuery);
        while(rs.hasNext()) {
            QuerySolution qsoln = rs.next();
            String graphURI = qsoln.get("g").asResource().getURI();
            log.info("Clearing destination graph " + graphURI);
            getSparqlEndpoint().clearGraph(graphURI);
        }
                            
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
    
    private void flushBufferToDestination(Map<String, Model> buffer) {
        writeQuadStoreToDestination(buffer);
        buffer.clear();
    }
    
    private void writeQuadStoreToDestination(Map<String, Model> quadStore) {
        for(String graphURI : quadStore.keySet()) {
            Model m = quadStore.get(graphURI);
            log.info("Writing " + m.size() + " triples to graph " + graphURI);
            long start = System.currentTimeMillis();
            getSparqlEndpoint().writeModel(m, graphURI);
            log.info(System.currentTimeMillis() - start + " ms to write.");
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
    private List<Quad> rewrite(List<Quad> quads, Map<String, String> sameAsMap) {
        List<Quad> rewrittenQuads = new ArrayList<Quad>();
        for(Quad quad : quads) {
            String subject = quad.getSubjectURI();
            String rewrittenSubject = sameAsMap.get(subject);
            if(rewrittenSubject != null) {
                subject = rewrittenSubject;
            }
            RDFNode object = quad.getObject();
            if(object.isURIResource()) {
                String rewrittenObject = sameAsMap.get(object.asResource().getURI());
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
            String subjectURI = quadSoln.get("s").asResource().getURI();
            String predicateURI = quadSoln.get("p").asResource().getURI();
            RDFNode object = quadSoln.get("o");
            quads.add(new Quad(graphURI, subjectURI, predicateURI, object));
        }
        log.debug(System.currentTimeMillis() - start + " to get individual quads");
        return quads;
    }
    
    private class IndividualURIIterator implements Iterator<String> {

        private static final int INDIVIDUAL_BATCH_SIZE = 10000;
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
                    "        ?s ?p ?o \n" +
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

    private SparqlEndpoint getSourceEndpoint() {
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
        return funcPropSet;
    }
    
    private List<String> getSameAsURIs(String individualURI, 
            OntModel sameAsModel) {
        List<String> sameAsURIs = new ArrayList<String>();
        NodeIterator nit = sameAsModel.listObjectsOfProperty(
                sameAsModel.getResource(individualURI), OWL.sameAs);
        while(nit.hasNext()) {
            RDFNode node = nit.next();
            if(node.isURIResource()) {
                sameAsURIs.add(node.asResource().getURI());
            }
        }
        return sameAsURIs;        
    }
    
    private List<String> getSameAsURIs(String individualURI, 
            Map<String, Model> quadStore) {
        List<String> sameAsURIs = new ArrayList<String>();
        for(Model model : quadStore.values()) {
            NodeIterator nit = model.listObjectsOfProperty(
                    model.getResource(individualURI), OWL.sameAs);
            while(nit.hasNext()) {
                RDFNode node = nit.next();
                if(node.isURIResource()) {
                    sameAsURIs.add(node.asResource().getURI());
                }
            }
        }
        return sameAsURIs;
    }
    
    @Override
    public Model getResult() {
        // TODO Auto-generated method stub
        return null;
    }

}