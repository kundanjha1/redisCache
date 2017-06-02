package org.kundansonuj.datacache;



import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.Properties;



public class PropertiesManager {
	
	private static final String TXN_PROPERTIES_FILE = "config.properties";
	Properties prop;
	private static PropertiesManager instance = new PropertiesManager();
	
	
	private PropertiesManager(){
		try {
			loadProperties();
			}
		catch (Exception e) {
			e.printStackTrace();
		}		
	}
	
	private void loadProperties(){
		InputStream fin = getClass().getClassLoader().getResourceAsStream(TXN_PROPERTIES_FILE);
		prop = new Properties();
		try {
			prop.load(fin);
		}
		catch (Exception e) {
			e.printStackTrace();
		}		
	}
	
	public String getProperty(String key) {
		return prop.getProperty(key);
	}

	

	
	public static PropertiesManager getInstance() {
		return instance;
	}	
}
