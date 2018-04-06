package org.wheatinitiative.vivo.datasource.util.xml;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.text.Normalizer;
import java.text.Normalizer.Form;

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
    
    private static final String XML2RDF_XSL = "/xsl/xml2rdf.xsl";
    private static final String DROP_EMPTY_NODES_XSL = "/xsl/dropEmptyNodes.xsl";
    private static boolean LICENSED_EDITION = true;
    
    public static final String GENERIC_NS = 
            "http://ingest.mannlib.cornell.edu/generalizedXMLtoRDF/0.1/";
    public static final String VITRO_NS = 
            "http://vitro.mannlib.cornell.edu/ns/vitro/0.7#";
    
    /**
     * Reads XML from an InputStream and "lifts" it to RDF
     * @param xmlInputStream
     * @return model containing RDF reflecting the XML structure of the document 
     */
    public Model toRDF(InputStream xmlInputStream) {
       xmlInputStream = applyXsl(xmlInputStream, DROP_EMPTY_NODES_XSL);
       xmlInputStream = applyXsl(xmlInputStream, XML2RDF_XSL);
       Model model = ModelFactory.createDefaultModel();
       model.read(xmlInputStream, null, "RDF/XML");
       return model;
    }
    
    /**
     * Reads XML from a String and "lifts" it to RDF
     * @param xmlString
     * @return model containing RDF reflecting the XML structure of the document 
     */
    public Model toRDF(String xmlString) {
        try {
            InputStream xmlInputStream = new ByteArrayInputStream(                    
                    Normalizer.normalize(xmlString, Form.NFC).getBytes("UTF-8"));
            return toRDF(xmlInputStream);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Reads an input stream containing an XML document, applies the
     * specified XSL transformation, and returns another InputStream
     * @param xmlInputStream 
     * @param xslResourcePath
     * @return
     */
    private InputStream applyXsl(InputStream xmlInputStream, 
            String xslResourcePath) {
        Processor processor = new Processor(false);
        XsltCompiler compiler  = processor.newXsltCompiler();
        XsltExecutable xsltExec = null;
        try {
            InputStream xsltIn = this.getClass().getResourceAsStream(
                    xslResourcePath);
            xsltExec = compiler.compile(new StreamSource(xsltIn));
        } catch (SaxonApiException e) {
            throw new RuntimeException("Unable to compile " 
                    + xslResourcePath, e);
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
            return new ByteArrayInputStream(xmlOutputStream.toByteArray());
        } catch (SaxonApiException e) {
            throw new RuntimeException("could not convert to RDF/XML", e);        
        }
    }
    
}
