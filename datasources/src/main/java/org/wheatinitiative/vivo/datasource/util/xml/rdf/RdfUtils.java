package org.wheatinitiative.vivo.datasource.util.xml.rdf;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.shared.Lock;
import com.hp.hpl.jena.util.ResourceUtils;
import com.hp.hpl.jena.util.iterator.ClosableIterator;

public class RdfUtils {

    private Random random = new Random(System.currentTimeMillis());
    
    /**
     * From edu.cornell.mannlib.vitro.webapp.utils.jena.JenaIngestUtils.
     * Returns a new copy of the input model with blank nodes renamed with 
     * namespaceEtc plus a random integer.
     * Will prevent URI collisions with supplied dedupModel 
     * @param namespaceEtc first (non-random) part of URI
     * @return model with all blank nodes renamed
     */
    public Model renameBNodes(Model inModel, String namespaceEtc, Model dedupModel) {
        Model outModel = ModelFactory.createDefaultModel();
        OntModel dedupUnionModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM); // we're not using OWL here, just the OntModel submodel infrastructure
        dedupUnionModel.addSubModel(outModel);
        if (dedupModel != null) {
            dedupUnionModel.addSubModel(dedupModel);
        }
        // the dedupUnionModel is so we can guard against reusing a URI in an 
        // existing model, as well as in the course of running this process
        inModel.enterCriticalSection(Lock.READ);
        Set<String> doneSet = new HashSet<String>();
        try {
            outModel.add(inModel);
            ClosableIterator closeIt = inModel.listSubjects();
            try {
                for (Iterator it = closeIt; it.hasNext();) {
                    Resource res = (Resource) it.next();
                    if (res.isAnon() && !(doneSet.contains(res.getId()))) {
                        // now we do something hacky to get the same resource in the outModel, since there's no getResourceById();
                        ClosableIterator closfIt = outModel.listStatements(res,(Property)null,(RDFNode)null);
                        Statement stmt = null;
                        try {
                            if (closfIt.hasNext()) {
                                stmt = (Statement) closfIt.next();
                            }
                        } finally {
                            closfIt.close();
                        }
                        if (stmt != null) {
                            Resource outRes = stmt.getSubject();
                            ResourceUtils.renameResource(outRes,getNextURI(namespaceEtc,dedupUnionModel));
                            doneSet.add(res.getId().toString());
                        }
                    }
                }
            } finally {
                closeIt.close();
            }
            closeIt = inModel.listObjects();
            try {
                for (Iterator it = closeIt; it.hasNext();) {
                    RDFNode rdfn = (RDFNode) it.next();
                    if (rdfn.isResource()) {
                        Resource res = (Resource) rdfn;
                        if (res.isAnon() && !(doneSet.contains(res.getId()))) {
                            // now we do something hacky to get the same resource in the outModel, since there's no getResourceById();
                            ClosableIterator closfIt = outModel.listStatements((Resource)null,(Property)null,res);
                            Statement stmt = null;
                            try {
                                if (closfIt.hasNext()) {
                                    stmt = (Statement) closfIt.next();
                                }
                            } finally {
                                closfIt.close();
                            }
                            if (stmt != null) {
                                Resource outRes = stmt.getSubject();
                                ResourceUtils.renameResource(outRes,getNextURI(namespaceEtc, dedupUnionModel));
                                doneSet.add(res.getId().toString());
                            }
                        }
                    }
                }
            } finally {
                closeIt.close();
            }
        } finally {
            inModel.leaveCriticalSection();
        }
        return outModel;
    }
    
    /**
     * From edu.cornell.mannlib.vitro.webapp.utils.jena.JenaIngestUtils. 
     * Generates a URI based on a concatenation of a "namespaceEtc" prefix
     * and a random integer. 
     * @param namespaceEtc first (non-random) part of URI
     * @param model in which URI should be unique
     * @return randomly-generated URI
     */
    private String getNextURI(String namespaceEtc, Model model) {
        String nextURI = null;
        boolean duplicate = true;
        while (duplicate) {
            nextURI = namespaceEtc+random.nextInt(9999999);
            Resource res = ResourceFactory.createResource(nextURI);
            duplicate = false;
            ClosableIterator closeIt = model.listStatements(res, (Property)null, (RDFNode)null);
            try {
                if (closeIt.hasNext()) {
                    duplicate = true;
                }
            } finally {
                closeIt.close();
            }
            if (duplicate == false) {
                closeIt = model.listStatements((Resource)null, (Property)null, res);
                try {
                    if (closeIt.hasNext()) {
                        duplicate = true;
                    }
                } finally {
                    closeIt.close();
                }
            }
        }
        return nextURI;
    }
    
}
