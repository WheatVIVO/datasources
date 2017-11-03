package org.wheatinitiative.vivo.datasource;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

public class VivoVocabulary {

    public static final String DEFAULT_NAMESPACE = "http://vivo.wheatinitiative.org/individual/";
    
    public static final String BIBO = "http://purl.org/ontology/bibo/";
    public static final String FOAF = "http://xmlns.com/foaf/0.1/";
    public static final String OBO = "http://purl.obolibrary.org/obo/";
    public static final String SKOS = "http://www.w3.org/2004/02/skos/core#";
    public static final String VCARD = "http://www.w3.org/2006/vcard/ns#";
    public static final String VIVO = "http://vivoweb.org/ontology/core#";
    public static final Resource ORGANIZATION = ResourceFactory.createResource(
            FOAF + "Organization");
    public static final Resource PERSON = ResourceFactory.createResource(
            FOAF + "Person");
    public static final Resource DOCUMENT = ResourceFactory.createResource(
            BIBO + "Document");
    public static final Resource JOURNAL = ResourceFactory.createResource(
            BIBO + "Journal");
    public static final Resource AUTHORSHIP = ResourceFactory.createResource(
            VIVO + "Authorship");
    public static final Resource POSITION = ResourceFactory.createResource(
            VIVO + "Position");
    public static final Resource ROLE = ResourceFactory.createResource(
            OBO + "Role");
    public static final Resource OLD_ROLE = ResourceFactory.createResource(
            VIVO + "Role");
    public static final Resource GRANT = ResourceFactory.createResource(
            VIVO + "Grant");
    public static final Resource PROJECT = ResourceFactory.createResource(
            VIVO + "Project");
    public static final Resource CONCEPT = ResourceFactory.createResource(
            SKOS + "Concept");
    public static final Resource DATETIME_INTERVAL = ResourceFactory.createResource(
            VIVO + "DateTimeInterval");
    public static final Resource DATETIME_VALUE = ResourceFactory.createResource(
            VIVO + "DateTimeValue");
    public static final Resource VCARD_KIND = ResourceFactory.createResource(
            VCARD + "Kind");
    public static final Resource OLD_ADDRESS = ResourceFactory.createResource(
            VIVO + "Address");
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
