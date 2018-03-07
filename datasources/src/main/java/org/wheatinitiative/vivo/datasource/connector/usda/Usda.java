package org.wheatinitiative.vivo.datasource.connector.usda;

import org.wheatinitiative.vivo.datasource.DataSource;
import org.wheatinitiative.vivo.datasource.VivoVocabulary;
import org.wheatinitiative.vivo.datasource.connector.VivoDataSource;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

public class Usda extends VivoDataSource implements DataSource {

    private static final String USDA_VIVO_URL = "http://vivo.usda.gov";
   
    @Override 
    protected String getRemoteVivoURL() {
        return USDA_VIVO_URL;
    }
    
    @Override
    protected Model mapToVIVO(Model model) {        
        // USDA VIVO is still using ontology version 1.5
        Model updated = updateToOneSix(model);
        return updateRoleRealization(updated);
    }
    
    // USDA's VIVO seems to use vivo:roleContributesTo in an unexpected way
    protected Model updateRoleRealization(Model model) {
        Property roleContributesTo = model.getProperty(VivoVocabulary.VIVO + "roleContributesTo");
        Property contributingRole = model.getProperty(VivoVocabulary.VIVO + "contributingRole");
        Model retractions = ModelFactory.createDefaultModel();
        Model additions = ModelFactory.createDefaultModel();
        StmtIterator sit = model.listStatements(null, roleContributesTo, (Resource) null);
        while(sit.hasNext()) {
            Statement stmt = sit.next();
            retractions.add(stmt);
            additions.add(stmt.getSubject(), VivoVocabulary.REALIZED_IN, stmt.getObject());
        }
        sit = model.listStatements(null, contributingRole, (Resource) null);
        while(sit.hasNext()) {
            Statement stmt = sit.next();
            retractions.add(stmt);
            additions.add(stmt.getSubject(), VivoVocabulary.REALIZES, stmt.getObject());
        }
        model = model.difference(retractions);
        model = model.add(additions);
        return model;
    }
    
    
}
