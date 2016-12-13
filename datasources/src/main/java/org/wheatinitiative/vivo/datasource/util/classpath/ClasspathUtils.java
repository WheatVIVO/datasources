package org.wheatinitiative.vivo.datasource.util.classpath;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

public class ClasspathUtils {

    private final static Log log = LogFactory.getLog(ClasspathUtils.class);
    
    public List<String> listFilesInDirectory(String resourcePath) {
        List<String> files = new ArrayList<String>();
        PathMatchingResourcePatternResolver resolver = 
                new PathMatchingResourcePatternResolver();
        String pattern = resourcePath;
        if(!pattern.endsWith("/")) {
            pattern += "/";
        }
        try {
            Resource[] resources = resolver.getResources(pattern + "**");
            for (int i = 0; i < resources.length; i++) {
                // TODO revisit this; for now we have to filter paths that don't exist
                String filePath = pattern + resources[i].getFilename();
                InputStream test = this.getClass().getResourceAsStream(filePath);
                if(test != null && !filePath.equals(pattern)) {                        
                    files.add(filePath);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
//        InputStream in = this.getClass().getResourceAsStream(resourcePath);
//        BufferedReader buffr = new BufferedReader(new InputStreamReader(in));
//        try {
//            String ln;
//            try {
//                while ((ln = buffr.readLine()) != null) {
//                    files.add(resourcePath + "/" + ln);
//                }
//            } finally {
//                buffr.close();   
//            }
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
        
        return files;
    }
    
    public String loadQuery(String resourcePath) {
        InputStream inputStream = this.getClass().getResourceAsStream(
                resourcePath);
        StringBuffer fileContents = new StringBuffer();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(inputStream));
            String ln;
            while ( (ln = reader.readLine()) != null) {
                fileContents.append(ln).append('\n');
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to load " + resourcePath, e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return fileContents.toString();
    }
    
}
