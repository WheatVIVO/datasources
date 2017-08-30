package org.wheatinitiative.vivo.datasource.connector.orcid;

public class Name {
    
    private String familyName;
    private String givenName;
    
    public String getFamilyName() {
        return this.familyName;
    }
    public void setFamilyName(String familyName) {
        this.familyName = familyName;
    }
    
    public String getGivenName() {
        return this.givenName;
    }
    public void setGivenName(String givenName) {
        this.givenName = givenName;
    }
}