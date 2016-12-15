/* $This file is distributed under the terms of the license in /doc/license.txt$ */

package org.wheatinitiative.vitro.webapp.ontology.update;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jena.iri.IRI;
import org.apache.jena.iri.IRIFactory;
import org.wheatinitiative.vivo.datasource.util.classpath.ClasspathUtils;
import org.wheatinitiative.vivo.datasource.util.xml.rdf.RdfUtils;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryParseException;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

/**
 * Performs knowledge base updates necessary to align with a
 * new ontology version.
 */
public class KnowledgeBaseUpdater {

    private final Log log = LogFactory.getLog(KnowledgeBaseUpdater.class);
    private RdfUtils rdfUtils = new RdfUtils();
    
    private UpdateSettings settings;
    
    public KnowledgeBaseUpdater(UpdateSettings settings) {
        this.settings = settings;
    }
    
    public boolean update() throws IOException {   
                
//        if (this.logger == null) {
//            this.logger = new SimpleChangeLogger(settings.getLogFile(), settings.getErrorLogFile());
//        }
            
        long startTime = System.currentTimeMillis();
        log.info("Performing any necessary data migration");
        //logger.log("Started knowledge base migration");
        
        boolean changesPerformed = false;
        
        try {
             changesPerformed = performUpdate();
        } catch (Exception e) {
             //logger.logError(e.getMessage());
             log.error(e,e);
        }

//        if (!logger.errorsWritten()) {
//            assertSuccess(servletContext);
//            logger.logWithDate("Finished knowledge base migration");
//        }
//        
//        record.writeChanges();
//        logger.closeLogs();

        long elapsedSecs = (System.currentTimeMillis() - startTime)/1000;       
        log.info("Finished checking knowledge base in " + elapsedSecs + " second" + (elapsedSecs != 1 ? "s" : ""));
        
        // The following was removed because it forced a recompute even if only
        // annotation values changed:
        // return record.hasRecordedChanges();
        
        return changesPerformed;
    }
    
    // returns true if ontology changes were found
    private boolean performUpdate() throws Exception {
        
        List<AtomicOntologyChange> rawChanges = getAtomicOntologyChanges();
        AtomicOntologyChangeLists changes = null;
        changes = new AtomicOntologyChangeLists(rawChanges,settings.getNewTBoxModel(),settings.getOldTBoxModel());
        // update ABox data any time
        log.debug("performing SPARQL CONSTRUCT additions");
        performSparqlConstructs(settings.getSparqlConstructAdditionsDir(), settings.getABoxModel(), ADD);
        log.debug("performing SPARQL CONSTRUCT retractions");
        performSparqlConstructs(settings.getSparqlConstructDeletionsDir(), settings.getABoxModel(), RETRACT);
        
        log.info("\tchecking the abox");
        updateABox(changes);
        
        log.debug("performing post-processing SPARQL CONSTRUCT additions");
        performSparqlConstructs(settings.getSparqlConstructAdditionsDir() + "/post/", 
                settings.getABoxModel(), ADD);
        
        log.debug("performing post-processing SPARQL CONSTRUCT retractions");
        performSparqlConstructs(settings.getSparqlConstructDeletionsDir() + "/post/", 
                settings.getABoxModel(), RETRACT);
        
        return !rawChanges.isEmpty();

    }
    
    
    
    private static final boolean ADD = true;
    private static final boolean RETRACT = !ADD;
    
    /**
     * Performs a set of arbitrary SPARQL CONSTRUCT queries on the 
     * data, for changes that cannot be expressed as simple property
     * or class additions, deletions, or renamings.
     * Blank nodes created by the queries are given random URIs.
     * @param sparqlConstructDir
     * @param readModel
     * @param writeModel
     * @param add (add = true; retract = false)
     */
    private void performSparqlConstructs(String sparqlConstructDir, Model aboxModel,
            boolean add)   throws IOException {
        ClasspathUtils utils = new ClasspathUtils();
        List<String> sparqlFiles = new ArrayList<String>();
        try {
            sparqlFiles = utils.listFilesInDirectory(sparqlConstructDir);
            log.debug("Using SPARQL CONSTRUCT directory " + sparqlConstructDir);
        } catch (Exception e) {
            String logMsg = this.getClass().getName() + 
                    "performSparqlConstructs() expected to find a directory " +
                    " at " + sparqlConstructDir + ". Unable to execute " +
                    " SPARQL CONSTRUCTS.";
            //logger.logError(logMsg);
            log.error(logMsg);
            return;
        }
        Collections.sort(sparqlFiles); // queries may depend on being run in a certain order
        for (String sparqlFile : sparqlFiles) {      
            Model anonModel = ModelFactory.createDefaultModel();
            try {
                log.debug("\t\tprocessing SPARQL construct query from file " + sparqlFile);
                
                anonModel = ModelFactory.createDefaultModel();
                String queryStr = utils.loadQuery(sparqlFile);
                try {
                    QueryExecution qe = QueryExecutionFactory.create(queryStr, aboxModel);
                    try {
                        qe.execConstruct(anonModel);
                    } finally {
                        qe.close();
                    }
                } catch (QueryParseException e) {
                    // we don't really know anymore what might be a directory
                    log.info("Skipping SPARQL file " + sparqlFile);
                    log.debug(e, e);
                }
                
                long num = anonModel.size();
                if (num > 0) {
                    String logMsg = (add ? "Added " : "Removed ") + num + 
                            " statement"  + ((num > 1) ? "s" : "") + 
                            " using the SPARQL construct query from file " + 
                            sparqlFile;
                    log.info(logMsg);
                    log.info(logMsg);
                }
        
            } catch (Exception e) {
                log.error(this.getClass().getName() + 
                        ".performSparqlConstructs() unable to execute " +
                        "query at " + sparqlFile + ". Error message is: " + e.getMessage());
                log.error(e,e);
            }
        
            if(!add) {
                StmtIterator sit = anonModel.listStatements();
                while (sit.hasNext()) {
                    Statement stmt = sit.nextStatement();
                    // Skip statements with blank nodes (unsupported) to avoid 
                    // excessive deletion.  In the future, the whole updater 
                    // could be modified to change whole graphs at once through
                    // the RDFService, but right now this whole thing is statement
                    // based.
                    if (stmt.getSubject().isAnon() || stmt.getObject().isAnon()) {
                        continue;
                    }
                    Model writeModel = aboxModel;
                    if (writeModel.contains(stmt)) {
                        writeModel.remove(stmt);
                    }
                }            
                //record.recordRetractions(anonModel);
                //log.info("removed " + anonModel.size() + " statements from SPARQL CONSTRUCTs");
            } else {
                Model writeModel = aboxModel;
                Model dedupeModel = aboxModel;
                Model additions = rdfUtils.renameBNodes(
                        anonModel, settings.getDefaultNamespace() + "n", dedupeModel);
                additions = stripBadURIs(additions);
                Model actualAdditions = ModelFactory.createDefaultModel();
                StmtIterator stmtIt = additions.listStatements();      
                while (stmtIt.hasNext()) {
                    Statement stmt = stmtIt.nextStatement();
                    if (!writeModel.contains(stmt)) {
                        actualAdditions.add(stmt);
                    }
                }      
                writeModel.add(actualAdditions);
                //log.info("added " + actualAdditions.size() + " statements from SPARQL CONSTRUCTs");
                //record.recordAdditions(actualAdditions);
            }
            
        }
    }
    
    private Model stripBadURIs(Model additions) {
        Model badURITriples = ModelFactory.createDefaultModel();
        StmtIterator stmtIt = additions.listStatements();
        while (stmtIt.hasNext()) {
            String[] uris = new String[3];
            Statement stmt = stmtIt.nextStatement();
            if(stmt.getSubject().isURIResource()) {
                uris[0] = stmt.getSubject().getURI();
            }
            uris[1] = stmt.getPredicate().getURI();
            if(stmt.getObject().isURIResource()) {
                uris[2] = ((Resource) stmt.getObject()).getURI();
            }
            for (int i = 0; i < 3; i++) {
                String uri = uris[i];
                if (uri != null) {
                    IRIFactory factory = IRIFactory.jenaImplementation();
                    IRI iri = factory.create(uri);
                    if (iri.hasViolation(false)) {
                        badURITriples.add(stmt);
                        log.error("Discarding added triple " + stmt + " because " +
                                  "it includes one or more invalid URIs.");
                        break;
                    } 
                }
            }
        }
        additions.remove(badURITriples);
        return additions;
    }
    
    private List<AtomicOntologyChange> getAtomicOntologyChanges() 
            throws IOException {
        return (new OntologyChangeParser()).parseFile(settings.getDiffFile());
    }
    
    private void updateABox(AtomicOntologyChangeLists changes) 
            throws IOException {    
        try {
            ABoxUpdater aboxUpdater = new ABoxUpdater(settings);
        aboxUpdater.processPropertyChanges(changes.getAtomicPropertyChanges());
        aboxUpdater.processClassChanges(changes.getAtomicClassChanges());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * A class that allows to access two different ontology change lists,
     * one for class changes and the other for property changes.  The 
     * constructor will split a list containing both types of changes.
     * @author bjl23
     *
     */
    private class AtomicOntologyChangeLists {
        
        private List<AtomicOntologyChange> atomicClassChanges = 
                new ArrayList<AtomicOntologyChange>();

        private List<AtomicOntologyChange> atomicPropertyChanges =
                new ArrayList<AtomicOntologyChange>();
        
        public AtomicOntologyChangeLists (
                List<AtomicOntologyChange> changeList, OntModel newTboxModel,
                OntModel oldTboxModel) throws IOException {
            
            Iterator<AtomicOntologyChange> listItr = changeList.iterator();
            
            while(listItr.hasNext()) {
                AtomicOntologyChange changeObj = listItr.next();
                if (changeObj.getSourceURI() != null){
                    log.debug("triaging " + changeObj);
                    if (oldTboxModel.getOntProperty(changeObj.getSourceURI()) != null){
                         atomicPropertyChanges.add(changeObj);
                         log.debug("added to property changes");
                    } else if (oldTboxModel.getOntClass(changeObj.getSourceURI()) != null) {
                         atomicClassChanges.add(changeObj);
                         log.debug("added to class changes");
                    } else if ("Prop".equals(changeObj.getNotes())) {
                         atomicPropertyChanges.add(changeObj);
                    } else if ("Class".equals(changeObj.getNotes())) {
                         atomicClassChanges.add(changeObj);
                    } else{
                        Model wtf = ModelFactory.createDefaultModel();
                        wtf.add(
                         oldTboxModel.listStatements(oldTboxModel.getResource(changeObj.getSourceURI()), null, (RDFNode) null)
                         );
                         log.warn("WARNING: Source URI is neither a Property" +
                                    " nor a Class. " + "Change Object skipped for sourceURI: " + changeObj.getSourceURI());
                    }
                    
                } else if(changeObj.getDestinationURI() != null){
                    
                    if (newTboxModel.getOntProperty(changeObj.getDestinationURI()) != null) {
                        atomicPropertyChanges.add(changeObj);
                    } else if(newTboxModel.getOntClass(changeObj.
                        getDestinationURI()) != null) {
                        atomicClassChanges.add(changeObj);
                    } else{
                        log.warn("WARNING: Destination URI is neither a Property" +
                                " nor a Class. " + "Change Object skipped for destinationURI: " + changeObj.getDestinationURI());
                    }
                } else{
                    log.warn("WARNING: Source and Destination URI can't be null. " + "Change Object skipped" );
                }
            }
            //logger.log("Property and Class change Object lists have been created");
        }
        
        public List<AtomicOntologyChange> getAtomicClassChanges() {
            return atomicClassChanges;
        }

        public List<AtomicOntologyChange> getAtomicPropertyChanges() {
            return atomicPropertyChanges;
        }       
        
    }   
    
   
    
}