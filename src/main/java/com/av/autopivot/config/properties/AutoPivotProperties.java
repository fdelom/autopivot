package com.av.autopivot.config.properties;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.jboss.logging.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.base.Strings;
import com.quartetfs.fwk.QuartetRuntimeException;

@Configuration
public class AutoPivotProperties {

	/** Logger **/
	protected static final Logger LOGGER = Logger.getLogger(AutoPivotProperties.class.getName());
	
	/** Charset key */
	public static final String CHARSET = "autopivot.charset";
	
	/** Charset with default value ISO-8859-1 */
	public static final String DEFAULT_CHARSET = "ISO-8859-1";
	private String charset = DEFAULT_CHARSET;
	
	public String getCharset() { return charset; }
	public void setCharset(String charset) { this.charset = charset; }
	
	public static abstract class APropertyInfo {
		public static final String DATA_INFO_FILENAME = "fileName";
		public static final String DATA_INFO_PIVOT_CACHE_SIZE = "pivot.cache.size";
		public static final String DATA_INFO_DIR_TO_WATCH = "dirToWatch";
		public static final String DATA_INFO_PATHMATCHER = "pathMatcher";
		public static final String DATA_INFO_DATASTORE_PARTITIONFIELD = "datastore.partitionField";
		public static final String DATA_INFO_AGGREGATE_PROVIDER_TYPE = "aggregateProviderType";
		
		public static final String DEFAULT_PATH_MATCHER = "glob:**.csv";
		public static final AGGREGATE_PROVIDER_TYPE DEFAULT_AGGREGATE_PROVIDER_TYPE = AGGREGATE_PROVIDER_TYPE.JUST_IN_TIME;
		
		public enum AGGREGATE_PROVIDER_TYPE {
			JUST_IN_TIME,
			BITMAP
		}
		
		public APropertyInfo() {
			properties = new HashMap<>();
		}
		
		private Map<String, String> properties;
		
		public abstract String getRootKey();
		
		public String getFileName() { return properties.get(DATA_INFO_FILENAME); }
		public String getDirToWatch() { return properties.get(DATA_INFO_DIR_TO_WATCH); }
		public String getDataStorePartitionField() { return properties.get(DATA_INFO_DATASTORE_PARTITIONFIELD); }
		
		public Integer getPivotCacheSize() {
			if (Strings.isNullOrEmpty(properties.get(DATA_INFO_PIVOT_CACHE_SIZE)) == false) {
				return Integer.parseInt(properties.get(DATA_INFO_PIVOT_CACHE_SIZE));
			}
			return null;
		}
		
		public AGGREGATE_PROVIDER_TYPE getAggregateProviderType() {
			if (Strings.isNullOrEmpty(properties.get(DATA_INFO_AGGREGATE_PROVIDER_TYPE)) == false) {
				return AGGREGATE_PROVIDER_TYPE.valueOf(properties.get(DATA_INFO_AGGREGATE_PROVIDER_TYPE));
			}
			return DEFAULT_AGGREGATE_PROVIDER_TYPE;
		}
		
		public String getPathMatcher() { 
			String pathMatcher = properties.get(DATA_INFO_PATHMATCHER);
			return Strings.isNullOrEmpty(pathMatcher) ? DEFAULT_PATH_MATCHER : pathMatcher; 
		}
		
		public void setProperty(String dataElmName, String key, String value) {
			String property = key.replace(getRootKey() + dataElmName + ".", "");
			properties.put(property, value);
		}
	}
	
	public static class DataInfo extends APropertyInfo {
		public static final String DATA_INFO_ROOT_KEY = "autopivot.discover.data.";

		public DataInfo() { super(); }
		
		@Override
		public String getRootKey() { return DATA_INFO_ROOT_KEY;	}
	}
	
	public static class RefDataInfo extends APropertyInfo {
		public static final String REF_DATA_INFO_ROOT_KEY = "autopivot.discover.refdata.";
		
		public RefDataInfo() { super(); }

		@Override
		public String getRootKey() { return REF_DATA_INFO_ROOT_KEY;	}
	}
	
	private Map<String, DataInfo> dataInfoMap;
	private Map<String, RefDataInfo> refDataInfoMap;
	
	public Map<String, DataInfo> getDataInfoMap() {
		return dataInfoMap;
	}
	
	public Map<String, RefDataInfo> getRefDataInfoMap() {
		return refDataInfoMap;
	}
	
	@Bean
	@Qualifier("autoPivotProperties")
	public Properties autoPivotProperties() {
		FileInputStream fileInputStream = null;
		
		try {
			String autopivotConfigPath = Thread.currentThread()
											   .getContextClassLoader()
											   .getResource("autopivot.properties")
											   .getPath();
			
			Properties autopivotProps = new Properties();
			fileInputStream = new FileInputStream(autopivotConfigPath);
			autopivotProps.load(fileInputStream);
			return autopivotProps;
		} catch (IOException ex) {
			throw new QuartetRuntimeException("Could not load properly the autopivot.properties.");
		}
		finally {
			if (fileInputStream != null) {
				try {
					fileInputStream.close();
				} catch (IOException ex) {
					throw new QuartetRuntimeException("Could not close properly the autopivot.properties.", ex);
				}
			}
		}
	}
	
	@Autowired
	public void loadConfiguration(@Qualifier("autoPivotProperties") Properties autoPivotProperties) throws ParseException {
		dataInfoMap = new HashMap<String, DataInfo>();
		refDataInfoMap = new HashMap<String, RefDataInfo>();

		Enumeration<Object> keyEnumeration = autoPivotProperties.keys();
		while (keyEnumeration.hasMoreElements()) {
			String key = (String)keyEnumeration.nextElement();
			if (key.equals(CHARSET)) {
				setCharset(autoPivotProperties.getProperty(key));
			}
			else if (key.startsWith(DataInfo.DATA_INFO_ROOT_KEY)) {
				addPropertyInfo(dataInfoMap,
								DataInfo.class,
								getElmName(DataInfo.DATA_INFO_ROOT_KEY, key),
								key,
								autoPivotProperties.getProperty(key));
			}
			else if (key.startsWith(RefDataInfo.REF_DATA_INFO_ROOT_KEY)) {
				addPropertyInfo(refDataInfoMap,
								RefDataInfo.class,
								getElmName(RefDataInfo.REF_DATA_INFO_ROOT_KEY, key),
								key,
								autoPivotProperties.getProperty(key));
			}
		}
		LOGGER.info(" autopivot.properties is loaded.");
	}
	
	private <T extends APropertyInfo> void addPropertyInfo(Map<String, T> propertyInfoMap, 
														   Class<T> elmClassType, 
														   String elmName, 
														   String key,
														   String property) throws ParseException {
		if (Strings.isNullOrEmpty(elmName)) {
			LOGGER.error("Could not find the identifier of the element associated to the property key: " + key);
			throw new ParseException("Could not find the identifier of the element associated to the property key: " + key, 
									 key.length());
		}
		
		T elm = null;
		if (propertyInfoMap.containsKey(elmName)) {
			elm = propertyInfoMap.get(elmName);
		}
		else {
			try {
				elm = elmClassType.newInstance();
			} catch (InstantiationException ex) {
				LOGGER.error("Could not instanciate object element of type: " + elmClassType.getCanonicalName());
				throw new QuartetRuntimeException("Could not instanciate object element of type: " + elmClassType.getCanonicalName());
			} catch (IllegalAccessException ex) {
				LOGGER.error("Object element of type: " + elmClassType.getCanonicalName() + " has not default constructor");
				throw new QuartetRuntimeException("Object element of type: " + elmClassType.getCanonicalName() + " has not default constructor");
			}
			propertyInfoMap.put(elmName, elm);
		}
		elm.setProperty(elmName, key, property);
	}
	
	private String getElmName(String rootKey, String fullKey) {
		if (Strings.isNullOrEmpty(fullKey)) {
			return null;
		}
		return fullKey.replace(rootKey, "").split("\\.")[0];
	}
}
