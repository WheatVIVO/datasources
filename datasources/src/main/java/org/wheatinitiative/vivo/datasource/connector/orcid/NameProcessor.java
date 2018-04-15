package org.wheatinitiative.vivo.datasource.connector.orcid;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class NameProcessor {

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
                name = setLastNamePlusInitials(name, tokens);                
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
    
    private Name setLastNamePlusInitials(Name name, String[] tokens) {
        int pos = positionOfFirstInitial(tokens);
        if(pos == -1) {
            pos = tokens.length - 1; // shouldn't happen
        }
        StringBuffer familyNameBuff = new StringBuffer();
        StringBuffer givenNameBuff = new StringBuffer();
        for(int i = 0; i < tokens.length; i++) {
            if(i < pos) {
                if(i > 0) {
                    familyNameBuff.append(" ");
                }
                familyNameBuff.append(tokens[i]);
            } else {
                givenNameBuff.append(tokens[i]);
                if(i < (tokens.length - 1)) {
                    givenNameBuff.append(" ");
                }
            }
        }
        name.setFamilyName(familyNameBuff.toString());
        name.setGivenName(givenNameBuff.toString());
        return name;
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
            if(!Character.isUpperCase(c) && c != '.') {
                return false;                
            } else if (c != '.') {
                letterCount++;
            }
        }
        return letterCount <= 3;
    }
    
    private boolean isLastNamePlusInitials(String[] tokens) {
        if( /* !isFirstTokenLongest(tokens) || */ tokens.length < 2) {
            return false;
        }
        if(isInitials(tokens[0])) {
            return false;
        }
        boolean initialsAlreadyFound = false;
        for(int i = 1; i < tokens.length; i++) {
            boolean isInitials = isInitials(tokens[i]);
            if(!isInitials && initialsAlreadyFound) {
                return false;
            } else {
                initialsAlreadyFound = isInitials;
            }
        }
        return initialsAlreadyFound;
    }
    
    private int positionOfFirstInitial(String[] tokens) {
        for(int i = 0; i < tokens.length; i++) {
            if (isInitials(tokens[i])) {
                return i;
            }
        }
        return -1; // shouldn't happen
    }
    
//    private boolean isFirstTokenLongest(String[] tokens) {
//        if(tokens.length < 2) {
//            return true;
//        }
//        int len = tokens[0].length();
//        for(int i = 1; i < tokens.length; i++) {
//            if(tokens[i].length() >= len) {
//                return false;
//            }
//        }
//        return true;
//    }
//    
//    private String concatRemaining(String[] tokens, int start) {
//        StringBuffer buff = new StringBuffer();
//        for(int i = start; i < tokens.length; i++) {
//            buff.append(tokens[i]);
//            if(i < (tokens.length - 1)) {
//                buff.append(" ");
//            }
//        }
//        return buff.toString();
//    }
    
}
