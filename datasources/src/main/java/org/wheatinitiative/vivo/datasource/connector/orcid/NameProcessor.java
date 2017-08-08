package org.wheatinitiative.vivo.datasource.connector.orcid;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.vocabulary.RDFS;

public class NameProcessor {

    private static final Property GIVEN_NAME = ResourceFactory.createProperty("http://www.w3.org/2006/vcard/ns#givenName");
    private static final Property FAMILY_NAME = ResourceFactory.createProperty("http://www.w3.org/2006/vcard/ns#familyName");
    private static final List<Character> UPPERCASE = Arrays.asList('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z');
    private static final Log log = LogFactory.getLog(NameProcessor.class);
    
    public Name parseName(String value) {
        Name name = new Name();
        value = value.trim();
        if(value.contains(",")) {
            String[] tokens = value.split(",");
            if(tokens.length < 2) {
                log.warn("Strange name with comma " + value);
                name.setFamilyName(value);
                name.setGivenName(value);
                return fixCase(name);
            }
            name.setFamilyName(tokens[0].trim());
            name.setGivenName(tokens[1].trim());
            if(tokens.length > 2) {
                log.warn("Found more than two name parts in " + value);
            }
            return fixCase(name);
        } else {
            String[] tokens = value.split(" ");
            if(isLastNamePlusInitials(tokens)) {
                name.setFamilyName(tokens[0]);
                name.setGivenName(concatRemaining(tokens, 1));                
            } else {
                StringBuffer givenNameBuff = new StringBuffer();
                for(int i = 0; i < tokens.length; i++) {
                    if(i < (tokens.length - 1)) {
                        givenNameBuff.append(tokens[i]).append(" ");
                    } else {
                        name.setFamilyName(tokens[i]);
                    }
                }
                name.setGivenName(givenNameBuff.toString().trim());
            }
        }
        return fixCase(name);
    }
    
    private String concatRemaining(String[] tokens, int start) {
        StringBuffer buff = new StringBuffer();
        for(int i = start; i < tokens.length; i++) {
            buff.append(tokens[i]);
        }
        return buff.toString();
    }
    
    private Name fixCase(Name name) {
        name.setFamilyName(fixCase(name.getFamilyName()));
        name.setGivenName(fixCase(name.getGivenName()));
        return name;
    }
    
    private String fixCase(String string) {
        StringBuffer buff = new StringBuffer();
        String[] tokens = string.split(" ");
        for(int i = 0; i < tokens.length; i++) {
            if(isInitials(tokens[i])) {
                buff.append(tokens[i]);
            } else {
                buff.append(toLowercase(tokens[i]));
            }
            if(i < (tokens.length - 1)) {
                buff.append(" ");
            }
        }
        return buff.toString();
    }
    
    private String toLowercase(String string) {
        if(string.length() < 2) {
            return string;
        } else {
            StringBuffer buff = new StringBuffer();
            String[] tokens = string.split("-");
            for(int i = 0; i < tokens.length; i++) {
                buff.append(tokens[i].substring(0,1) + tokens[i].substring(1).toLowerCase());
                if(i < (tokens.length - 1)) {
                    buff.append("-");
                }
            }
            return buff.toString();
        }
    }
    
    private boolean isInitials(String token) {
        int letterCount = 0;
        for(int i = 0; i < token.length(); i++) {
            char c = token.charAt(i);
            if(!UPPERCASE.contains(c) && c != '.') {
                return false;                
            } else if (c != '.') {
                letterCount++;
            }
        }
        return letterCount <= 3;
    }
    
    private boolean isLastNamePlusInitials(String[] tokens) {
        if(!isFirstTokenLongest(tokens) || tokens.length < 2) {
            return false;
        }
        for(int i = 1; i < tokens.length; i++) {
            if(!isInitials(tokens[i])) {
                return false;
            }
        }
        return true;
    }
    
    private boolean isFirstTokenLongest(String[] tokens) {
        if(tokens.length < 2) {
            return true;
        }
        int len = tokens[0].length();
        for(int i = 1; i < tokens.length; i++) {
            if(tokens[i].length() >= len) {
                return false;
            }
        }
        return true;
    }
    
}
