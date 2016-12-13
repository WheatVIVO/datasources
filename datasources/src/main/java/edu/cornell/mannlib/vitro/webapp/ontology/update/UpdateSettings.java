/* $This file is distributed under the terms of the license in /doc/license.txt$ */

package edu.cornell.mannlib.vitro.webapp.ontology.update;

import java.net.URI;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.rdf.model.Model;

public class UpdateSettings {

    private URI dataDir;
    private URI sparqlConstructAdditionsDir;
    private URI sparqlConstructAdditionsPass2Dir;
    private URI sparqlConstructDeletionsDir;
    private URI askUpdatedQueryFile;
    private URI successAssertionsFile;
    private String successRDFFormat = "N3";
    private URI diffFile;
    private URI logFile;
    private URI errorLogFile;
    private URI addedDataFile;
    private URI removedDataFile;
    private URI qualifiedPropertyConfigFile;
    private String defaultNamespace;
    private OntModel oldTBoxModel;
    private OntModel newTBoxModel;
    private OntModel oldTBoxAnnotationsModel;
    private OntModel newTBoxAnnotationsModel;
    private Model aboxModel;
    //display model tbox and display model display metadata
    private OntModel oldDisplayModelTboxModel;
    private OntModel oldDisplayModelDisplayMetadataModel;
    private OntModel newDisplayModelTboxModel;
    private OntModel newDisplayModelDisplayMetadataModel;
    private OntModel displayModel;
    private OntModel newDisplayModelFromFile;
    private OntModel loadedAtStartupDisplayModel;
    private OntModel oldDisplayModelVivoListViewConfig;
    
    public URI getDataDir() {
        return dataDir;
    }
    public void setDataDir(URI dataDir) {
        this.dataDir = dataDir;
    }
    public URI getSparqlConstructAdditionsDir() {
        return sparqlConstructAdditionsDir;
    }
    public void setSparqlConstructAdditionsPass2Dir(URI sparqlConstructAdditionsDir) {
        this.sparqlConstructAdditionsPass2Dir = sparqlConstructAdditionsDir;
    }
    public URI getSparqlConstructAdditionsPass2Dir() {
        return sparqlConstructAdditionsPass2Dir;
    }
    public void setSparqlConstructAdditionsDir(URI sparqlConstructAdditionsDir) {
        this.sparqlConstructAdditionsDir = sparqlConstructAdditionsDir;
    }
    public URI getSparqlConstructDeletionsDir() {
        return sparqlConstructDeletionsDir;
    }
    public void setSparqlConstructDeletionsDir(URI sparqlConstructDeletionsDir) {
        this.sparqlConstructDeletionsDir = sparqlConstructDeletionsDir;
    }
    public URI getAskUpdatedQueryFile() {
        return askUpdatedQueryFile;
    }
    public void setAskUpdatedQueryFile(URI askQueryFile) {
        this.askUpdatedQueryFile = askQueryFile;
    }
    public URI getSuccessAssertionsFile() {
        return successAssertionsFile;
    }
    public void setSuccessAssertionsFile(URI successAssertionsFile) {
        this.successAssertionsFile = successAssertionsFile;
    }
    public String getSuccessRDFFormat() {
        return successRDFFormat;
    }
    public void setSuccessRDFFormat(String successRDFFormat) {
        this.successRDFFormat = successRDFFormat;
    }
    public URI getDiffFile() {
        return diffFile;
    }
    public void setDiffFile(URI diffFile) {
        this.diffFile = diffFile;
    }
    public URI getLogFile() {
        return logFile;
    }
    public void setLogFile(URI logFile) {
        this.logFile = logFile;
    }
    public URI getErrorLogFile() {
        return errorLogFile;
    }
    public void setErrorLogFile(URI errorLogFile) {
        this.errorLogFile = errorLogFile;
    }
    public URI getAddedDataFile() {
        return addedDataFile;
    }
    public void setAddedDataFile(URI addedDataFile) {
        this.addedDataFile = addedDataFile;
    }
    public URI getRemovedDataFile() {
        return removedDataFile;
    }
    public void setRemovedDataFile(URI removedDataFile) {
        this.removedDataFile = removedDataFile;
    }
    public URI getQualifiedPropertyConfigFile() {
        return qualifiedPropertyConfigFile;
    }
    public void setQualifiedPropertyConfigFile(URI qualifiedPropertyConfigFile) {
        this.qualifiedPropertyConfigFile = qualifiedPropertyConfigFile;
    }
    public String getDefaultNamespace() {
        return defaultNamespace;
    }
    public void setDefaultNamespace(String defaultNamespace) {
        this.defaultNamespace = defaultNamespace;
    }
    public OntModel getOldTBoxModel() {
        return oldTBoxModel;
    }
    public void setOldTBoxModel(OntModel oldTBoxModel) {
        this.oldTBoxModel = oldTBoxModel;
    }
    public OntModel getNewTBoxModel() {
        return newTBoxModel;
    }
    public void setNewTBoxModel(OntModel newTBoxModel) {
        this.newTBoxModel = newTBoxModel;
    }
    public OntModel getOldTBoxAnnotationsModel() {
        return oldTBoxAnnotationsModel;
    }
    public void setOldTBoxAnnotationsModel(OntModel oldTBoxAnnotationsModel) {
        this.oldTBoxAnnotationsModel = oldTBoxAnnotationsModel;
    }
    public OntModel getNewTBoxAnnotationsModel() {
        return newTBoxAnnotationsModel;
    }
    public void setNewTBoxAnnotationsModel(OntModel newTBoxAnnotationsModel) {
        this.newTBoxAnnotationsModel = newTBoxAnnotationsModel;
    }
    public Model getABoxModel() {
        return this.aboxModel;
    }
    public void setABoxModel(Model aboxModel) {
        this.aboxModel = aboxModel;
    }
    
    //Old and new display model methods
    public void setOldDisplayModelTboxModel(OntModel oldDisplayModelTboxModel) {
        this.oldDisplayModelTboxModel = oldDisplayModelTboxModel;
    }
    
    public void setNewDisplayModelTboxModel(OntModel newDisplayModelTboxModel) {
        this.newDisplayModelTboxModel = newDisplayModelTboxModel;
    }
    
    public void setOldDisplayModelDisplayMetadataModel(OntModel oldDisplayModelDisplayMetadataModel) {
        this.oldDisplayModelDisplayMetadataModel = oldDisplayModelDisplayMetadataModel;
    }
    
    public void setNewDisplayModelDisplayMetadataModel(OntModel newDisplayModelDisplayMetadataModel) {
        this.newDisplayModelDisplayMetadataModel = newDisplayModelDisplayMetadataModel;
    }
    
    public void setDisplayModel(OntModel displayModel) {
        this.displayModel = displayModel;
    }
    
    public OntModel getOldDisplayModelTboxModel() {
        return this.oldDisplayModelTboxModel;
    }
    
    public OntModel getNewDisplayModelTboxModel() {
        return this.newDisplayModelTboxModel;
    }

    public OntModel getOldDisplayModelDisplayMetadataModel() {
        return this.oldDisplayModelDisplayMetadataModel;
    }
    
    public OntModel getNewDisplayModelDisplayMetadataModel() {
        return this.newDisplayModelDisplayMetadataModel;
    }
    
    public OntModel getDisplayModel() {
        return this.displayModel;
    }
    
    public void setNewDisplayModelFromFile(OntModel newDisplayModel) {
        this.newDisplayModelFromFile = newDisplayModel;
    }
    
    public OntModel getNewDisplayModelFromFile() {
        return this.newDisplayModelFromFile;
    }
    
    public void setLoadedAtStartupDisplayModel(OntModel loadedModel) {
        this.loadedAtStartupDisplayModel = loadedModel;
    }
    
    public OntModel getLoadedAtStartupDisplayModel() {
        return this.loadedAtStartupDisplayModel;
    }
    
    public void setVivoListViewConfigDisplayModel(OntModel loadedModel) {
        this.oldDisplayModelVivoListViewConfig = loadedModel;
    }
    
    public OntModel getVivoListViewConfigDisplayModel() {
        return this.oldDisplayModelVivoListViewConfig;
    }
    
}
