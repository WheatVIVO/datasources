package org.wheatinitiative.vivo.datasource.util.csv;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.wheatinitiative.vivo.datasource.util.IteratorWithSize;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

public class CsvToRdf {

    private CSVFormat format;
    private List<Column> columnList = new ArrayList<Column>();
    private boolean skipFirstRow = true;
    private Set<String> nullValueStrings = new HashSet<String>();
    
    /**
     * Construct a CsvToRdf using the default CSV format
     */
    public CsvToRdf() {
        format = CSVFormat.DEFAULT;
    }

    /**
     * Construct a CsvToRdf using a specified file format
     * @param format
     */
    public CsvToRdf(CSVFormat format) {
        this.format = format;
    }

    /**
     * Construct a CsvToRdf using the specified delimiter char
     * @param delimiter
     */
    public CsvToRdf(char delimiter, char quote) {
        this.format = CSVFormat.newFormat(delimiter).withQuote(quote);
    }

    public void setSkipFirstRow(boolean skipFirstRow) {
        this.skipFirstRow = skipFirstRow;
    }
    
    public void addLiteralColumn(String propertyURI) {
        columnList.add(new LiteralColumn(propertyURI, null));
    }

    public void addLiteralColumn(String propertyURI, String datatypeURI) {
        columnList.add(new LiteralColumn(propertyURI, datatypeURI));
    }
    
    public void addLanguageLiteralColumn(String propertyURI, String language) {
        columnList.add(new LanguageLiteralColumn(propertyURI, language));
    }
    
    public void addResourceColumn(String propertyURI) {
        columnList.add(new ResourceColumn(propertyURI, null));
    }
    
    public void addResourceColumn(String propertyURI, String resourceURIPrefix) {
        columnList.add(new ResourceColumn(propertyURI, resourceURIPrefix));
    }
    
    public void removeColumn(Column column) {
        columnList.remove(column);
    }
    
    public List<Column> getColumnList() {
        return this.columnList;
    }
    
    public int getNumColumns() {
        return columnList.size();
    }
    
    public Column getLastColumn() {
        if(columnList.size() == 0) {
            return null;
        } else {
            return columnList.get(columnList.size() - 1);
        }
    }
    
    public void addNullValueString(String nullValueString) {
        this.nullValueStrings.add(nullValueString);
    }
    
    public void removeNullValueString(String nullValueString) {
        this.nullValueStrings.remove(nullValueString);
    }
    
    private class RowModelIterator implements IteratorWithSize<Model> {

        private Iterator<CSVRecord> recordIterator;
        
        public RowModelIterator(Iterator<CSVRecord> recordIterator) {
            this.recordIterator = recordIterator;
        }
        
        public boolean hasNext() {
            return recordIterator.hasNext();
        }

        public Model next() {
            return recordAsRDF(recordIterator.next());
        }

        public Integer size() {
            // return null as size is unknown
            return null;
        }
        
    }
    
    public IteratorWithSize<Model> getModelIterator(InputStream csvInputStream) {
        InputStreamReader in = new InputStreamReader(csvInputStream,
                StandardCharsets.UTF_8);
        try {
            Iterable<CSVRecord> records = this.format.parse(in);
            Iterator<CSVRecord> rows = records.iterator();
            if(this.skipFirstRow && rows.hasNext()) {
                rows.next(); // skip over the header 
            }
        return new RowModelIterator(rows);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public Model toRDF(InputStream csvInputStream) {
        Model model = ModelFactory.createDefaultModel();
        Iterator<Model> modelIt = getModelIterator(csvInputStream);
        while(modelIt.hasNext()) {
            model.add(modelIt.next());
        }
        return model;
    }
    
    private Model recordAsRDF(CSVRecord rec) {
        Model model = ModelFactory.createDefaultModel();
        if(rec.size() != columnList.size()) {
            throw new RuntimeException("Row has " +
                    rec.size() + " columns while expected width is " +
                    columnList.size() + " columns");
        }
        Resource recRes = model.createResource();
        Iterator<String> valueIt = rec.iterator();
        Iterator<Column> colIt = columnList.iterator();
        while(valueIt.hasNext()) {
            Column column = colIt.next();
            String value = valueIt.next();
            if(value == null || value.isEmpty()) {
                continue;
            }
            if(nullValueStrings.contains(value)) {
                continue;
            }
            List<String> values = null;
            if(column.getSplitValuesRegex() == null) {
                values = Arrays.asList(value); 
            } else {
                values = Arrays.asList(
                        value.split(column.getSplitValuesRegex()));
            }
            for (String singleValue : values) {
                singleValue = singleValue.trim();
                RDFNode objNode = getNode(singleValue, column, model);
                model.add(recRes, model.getProperty(
                        column.getPropertyURI()), objNode);
            }
        }
        return model;
    }
    
    private RDFNode getNode(String value, Column column, Model model) {
        if(column instanceof LiteralColumn) {
            LiteralColumn col = (LiteralColumn) column;
            if(col.getDatatypeURI() != null) {
                return model.createTypedLiteral(value, col.getDatatypeURI());
            } else {
                return model.createLiteral(value);
            }
        } else if (column instanceof LanguageLiteralColumn) {
            LanguageLiteralColumn col = (LanguageLiteralColumn) column;
            return ResourceFactory.createLangLiteral(value, col.getLanguage());
        } else if (column instanceof ResourceColumn) {
            ResourceColumn col = (ResourceColumn) column;
            String resourceURI = value;
            if(col.getPrefix() != null) {
                resourceURI = col.getPrefix() + resourceURI;
            }
            return model.getResource(resourceURI);
        } else {
            throw new RuntimeException("Unsupported column type " 
                    + column.getClass().getSimpleName());
        }
    }
    
    public IteratorWithSize<Model> getModelIterator(String csvString) {
        return getModelIterator(getInputStream(csvString));
    }

    public Model toRDF(String csvString) throws IOException {
        return toRDF(getInputStream(csvString));
    }
    
    private InputStream getInputStream(String string) {
        try {
            return new ByteArrayInputStream(
                    string.getBytes("UTF-8")); 
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public class Column {

        private String propertyURI;
        private String splitValuesRegex;

        public Column(String propertyURI) {
            this.propertyURI = propertyURI;
        }

        public String getPropertyURI() {
            return this.propertyURI;
        }
        
        public String getSplitValuesRegex() {
            return this.splitValuesRegex;
        }
        
        public void setSplitValuesRegex(String regex) {
            this.splitValuesRegex = regex;
        }

    }

    private class LiteralColumn extends Column {

        private String datatypeURI;

        public LiteralColumn(String propertyURI, String datatypeURI) {
            super(propertyURI);
            this.datatypeURI = datatypeURI;
        }

        public String getDatatypeURI() {
            return this.datatypeURI;
        }

    }

    private class LanguageLiteralColumn extends Column {

        private String language;

        public LanguageLiteralColumn(String propertyURI, String language) {
            super(propertyURI);
            this.language = language;
        }

        public String getLanguage() {
            return this.language;
        }

    }

    private class ResourceColumn extends Column {

        private String prefix;

        public ResourceColumn(String propertyURI, String resourceURIPrefix) {
            super(propertyURI);
            this.prefix = resourceURIPrefix;
        }

        public String getPrefix() {
            return this.prefix;
        }
    }

}
