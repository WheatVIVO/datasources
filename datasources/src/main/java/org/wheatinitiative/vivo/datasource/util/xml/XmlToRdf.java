package org.wheatinitiative.vivo.datasource.util.xml;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import javax.xml.transform.stream.StreamSource;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.Serializer;
import net.sf.saxon.s9api.XsltCompiler;
import net.sf.saxon.s9api.XsltExecutable;
import net.sf.saxon.s9api.XsltTransformer;

public class XmlToRdf {

    Processor saxonProcessor = new Processor(!LICENSED_EDITION);
    
    private static final String XSLT_FILE = "/xsl/xml2rdf.xsl";
    private static boolean LICENSED_EDITION = true;
    
    /**
     * Reads XML from an InputStream and "lifts" it to RDF
     * @param xmlInputStream
     * @return model containing RDF reflecting the XML structure of the document 
     */
    public Model toRDF(InputStream xmlInputStream) {
        Processor processor = new Processor(false);
        XsltCompiler compiler  = processor.newXsltCompiler();
        XsltExecutable xsltExec = null;
        try {
            InputStream xsltIn = this.getClass().getResourceAsStream(XSLT_FILE);
            xsltExec = compiler.compile(new StreamSource(xsltIn));
        } catch (SaxonApiException e) {
            throw new RuntimeException("Unable to compile " + XSLT_FILE, e);
        }       
        try {
            XsltTransformer t = xsltExec.load();
            Serializer out = processor.newSerializer();
            out.setOutputProperty(Serializer.Property.METHOD, "xml");
            out.setOutputProperty(Serializer.Property.INDENT, "yes");
            ByteArrayOutputStream xmlOutputStream = new ByteArrayOutputStream();
            out.setOutputStream(xmlOutputStream);
            t.setSource(new StreamSource(xmlInputStream));
            t.setDestination(out);
            t.transform();
            ByteArrayInputStream rdfXmlInputStream = new ByteArrayInputStream(
                    xmlOutputStream.toByteArray());
            Model model = ModelFactory.createDefaultModel();
            model.read(rdfXmlInputStream, null, "RDF/XML");
            return model;
        } catch (SaxonApiException e) {
            throw new RuntimeException("could not convert to RDF/XML", e);        
        }
    }
    
    /**
     * Reads XML from a String and "lifts" it to RDF
     * @param xmlString
     * @return model containing RDF reflecting the XML structure of the document 
     */
    public Model toRDF(String xmlString) {
        try {
            InputStream xmlInputStream = new ByteArrayInputStream(
                    xmlString.getBytes("UTF-8"));
            return toRDF(xmlInputStream);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
    
}
