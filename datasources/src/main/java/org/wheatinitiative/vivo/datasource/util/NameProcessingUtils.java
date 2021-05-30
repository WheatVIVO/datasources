package org.wheatinitiative.vivo.datasource.util;

public class NameProcessingUtils {

    public boolean isInitials(String token) {
        int letterCount = 0;
        for(int i = 0; i < token.length(); i++) {
            char c = token.charAt(i);
            if(!Character.isUpperCase(c) && c != '.' && c != '-') {
                return false;                
            } else if (c != '.' && c != '-') {
                letterCount++;
            }
        }
        boolean isInitials = (letterCount > 0) && (letterCount <= 3);
        return isInitials;
    }

    public boolean isAbbreviation(String token) {
        int letterCount = 0;
        for(int i = 0; i < token.length(); i++) {
            char c = token.charAt(i);
            if(!Character.isUpperCase(c) && c != '.' && c != '-') {
                return false;                
            } else if (c != '.' && c != '-') {
                letterCount++;
            }
        }
        boolean isInitials = (letterCount > 0) && (letterCount <= 9);
        return isInitials;
    }
    
}
