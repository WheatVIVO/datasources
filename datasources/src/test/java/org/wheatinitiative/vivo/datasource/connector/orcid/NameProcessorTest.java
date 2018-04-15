package org.wheatinitiative.vivo.datasource.connector.orcid;

import junit.framework.TestCase;

public class NameProcessorTest extends TestCase {

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public NameProcessorTest( String testName ) {
        super( testName );
    }
    
    public void testNameParsing() {
        NameProcessor proc = new NameProcessor();
        Name name;
        
        name = proc.parseName("A. Harvey Millar");
        assertEquals("Millar", name.getFamilyName());
        assertEquals("A. Harvey", name.getGivenName());
        
        name = proc.parseName("A. R. Leitch");
        assertEquals("Leitch", name.getFamilyName());
        assertEquals("A. R.", name.getGivenName());
        
        name = proc.parseName("A.R. Leitch");
        assertEquals("Leitch", name.getFamilyName());
        assertEquals("A.R.", name.getGivenName());
        
        name = proc.parseName("A. Perez-Jones");
        assertEquals("Perez-Jones", name.getFamilyName());
        assertEquals("A.", name.getGivenName());
        
        name = proc.parseName("A. Suzuki");
        assertEquals("Suzuki", name.getFamilyName());
        assertEquals("A.", name.getGivenName());
        
        name = proc.parseName("Abdalla, O.");
        assertEquals("Abdalla", name.getFamilyName());
        assertEquals("O.", name.getGivenName());
        
        name = proc.parseName("Able, A.J.");
        assertEquals("Able", name.getFamilyName());
        assertEquals("A.J.", name.getGivenName());
        
        name = proc.parseName("Able A. J.");
        assertEquals("Able", name.getFamilyName());
        assertEquals("A. J.", name.getGivenName());
        
        name = proc.parseName("Able A J");
        assertEquals("Able", name.getFamilyName());
        assertEquals("A J", name.getGivenName());
        
        name = proc.parseName("Able A");
        assertEquals("Able", name.getFamilyName());
        assertEquals("A", name.getGivenName());
        
        name = proc.parseName("le Gouis, J.");
        assertEquals("le Gouis", name.getFamilyName());
        assertEquals("J.", name.getGivenName());
        
        name = proc.parseName("Le Gouis J");
        assertEquals("Le Gouis", name.getFamilyName());
        assertEquals("J", name.getGivenName());
                
        name = proc.parseName("de Santis, A.J.");
        assertEquals("de Santis", name.getFamilyName());
        assertEquals("A.J.", name.getGivenName());
        
        name = proc.parseName("de Santis A J");
        assertEquals("de Santis", name.getFamilyName());
        assertEquals("A J", name.getGivenName());
        
        name = proc.parseName("de Santis A.J.");
        assertEquals("de Santis", name.getFamilyName());
        assertEquals("A.J.", name.getGivenName());
        
        name = proc.parseName("de Santis A");
        assertEquals("de Santis", name.getFamilyName());
        assertEquals("A", name.getGivenName());
        
        name = proc.parseName("de Santis A.");
        assertEquals("de Santis", name.getFamilyName());
        assertEquals("A.", name.getGivenName());
        
        name = proc.parseName("van Cleef, Mynderse");
        assertEquals("van Cleef", name.getFamilyName());
        assertEquals("Mynderse", name.getGivenName());
        
        name = proc.parseName("Van Cleef, Mynderse");
        assertEquals("Van Cleef", name.getFamilyName());
        assertEquals("Mynderse", name.getGivenName());
        
        name = proc.parseName("van Cleef M");
        assertEquals("van Cleef", name.getFamilyName());
        assertEquals("M", name.getGivenName());
        
        name = proc.parseName("van Cleef MX");
        assertEquals("van Cleef", name.getFamilyName());
        assertEquals("MX", name.getGivenName());
        
        name = proc.parseName("Van Cleef M. X.");
        assertEquals("Van Cleef", name.getFamilyName());
        assertEquals("M. X.", name.getGivenName());
        
        name = proc.parseName("Van Cleef MX");
        assertEquals("Van Cleef", name.getFamilyName());
        assertEquals("MX", name.getGivenName());
    }
}
