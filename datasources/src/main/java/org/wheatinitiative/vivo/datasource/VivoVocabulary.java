package org.wheatinitiative.vivo.datasource;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

public class VivoVocabulary {

    public static final String FOAF = " http://xmlns.com/foaf/0.1/";
    public static final String OBO = "http://purl.obolibrary.org/obo/";
    public static final String VIVO = "http://vivoweb.org/ontology/core#";
    public static final Resource ORGANIZATION = ResourceFactory.createResource(
            FOAF + "Organization");
    public static final Resource PERSON = ResourceFactory.createResource(
            FOAF + "Person");
    public static final Property HAS_CONTACT = ResourceFactory.createProperty(
            OBO + "ARG_2000028");
    public static final Property RELATES = ResourceFactory.createProperty(
            VIVO + "relates");
    public static final Property RELATEDBY = ResourceFactory.createProperty(
            VIVO + "relatedBy");
    public static final Property PART_OF = ResourceFactory.createProperty(
            OBO + "BFO_0000050");
    public static final Property HAS_PART = ResourceFactory.createProperty(
            OBO + "BFO_0000051");
    public static final Property OLD_SUBORG_WITHIN = ResourceFactory.createProperty(
            VIVO + "subOrganizationWithin");
    public static final String CLASSGROUP_PEOPLE = 
            "http://vivoweb.org/ontology#vitroClassGrouppeople";
    public static final String CLASSGROUP_ACTIVITIES = 
            "http://vivoweb.org/ontology#vitroClassGroupactivities";
    public static final String CLASSGROUP_RESEARCH = 
            "http://vivoweb.org/ontology#vitroClassGrouppublications";
    
}
