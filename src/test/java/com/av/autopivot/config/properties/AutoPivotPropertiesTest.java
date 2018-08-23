package com.av.autopivot.config.properties;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.util.Properties;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.av.autopivot.config.properties.AutoPivotProperties.APropertyInfo;
import com.av.autopivot.config.properties.AutoPivotProperties.APropertyInfo.AGGREGATE_PROVIDER_TYPE;
import com.av.autopivot.config.properties.AutoPivotProperties.DataInfo;
import com.av.autopivot.config.properties.AutoPivotProperties.RefDataInfo;
import com.google.common.base.Strings;
import com.quartetfs.fwk.QuartetRuntimeException;

@PrepareForTest({ AutoPivotProperties.class, FileInputStream.class })
@RunWith(PowerMockRunner.class)
public class AutoPivotPropertiesTest {
	@Test(expected = QuartetRuntimeException.class)
	public void shouldThrowQuartetRuntimeExceptionWhenPropertyFileIsNotFound() throws Exception {
		PowerMockito.whenNew(FileInputStream.class)
					.withArguments(Mockito.anyString())
					.thenThrow(new FileNotFoundException());
		AutoPivotProperties autoPivotProperties = new AutoPivotProperties();
		autoPivotProperties.autoPivotProperties();
	}
	
	@Test
	public void shouldReturnEmptyMapWhenPropertiesAreEmpty() throws ParseException {
		Properties props = new Properties();
		AutoPivotProperties autoPivotProperties = new AutoPivotProperties();
		autoPivotProperties.loadConfiguration(props);
		assertNotNull(autoPivotProperties.getDataInfoMap());
		assertNotNull(autoPivotProperties.getRefDataInfoMap());
		assertThat(autoPivotProperties.getDataInfoMap().entrySet(), hasSize(0));
		assertThat(autoPivotProperties.getRefDataInfoMap().entrySet(), hasSize(0));
	}
	
	@Test
	public void shouldReturnEmptyMapWhenPropertiesDontMatchAnyRootKey() throws ParseException {
		Properties props = new Properties();
		props.put("dummyKey", "dummyValues");
		props.put(APropertyInfo.DATA_INFO_AGGREGATE_PROVIDER_TYPE, "dummyValue");
		props.put(APropertyInfo.DATA_INFO_DATASTORE_PARTITIONFIELD, "dummyValue");
		props.put(APropertyInfo.DATA_INFO_DIR_TO_WATCH, "dummyValue");
		props.put(APropertyInfo.DATA_INFO_FILENAME, "dummyValue");
		props.put(APropertyInfo.DATA_INFO_PATHMATCHER, "dummyValue");
		props.put(APropertyInfo.DATA_INFO_PIVOT_CACHE_SIZE, "dummyValue");
		AutoPivotProperties autoPivotProperties = new AutoPivotProperties();
		autoPivotProperties.loadConfiguration(props);
		assertNotNull(autoPivotProperties.getDataInfoMap());
		assertNotNull(autoPivotProperties.getRefDataInfoMap());
		assertThat(autoPivotProperties.getDataInfoMap().entrySet(), hasSize(0));
		assertThat(autoPivotProperties.getRefDataInfoMap().entrySet(), hasSize(0));		
	}
	
	@Test
	public void shouldReturnDefaultCharsetWhenPropertyIsUndefined() throws ParseException {
		Properties props = new Properties();
		AutoPivotProperties autoPivotProperties = new AutoPivotProperties();
		autoPivotProperties.loadConfiguration(props);
		assertThat(autoPivotProperties.getCharset(), equalTo(AutoPivotProperties.DEFAULT_CHARSET));
	}
	
	@Test
	public void shouldReturnGivenCharsetWhenPropertyIsDefined() throws ParseException {
		Properties props = new Properties();
		props.put(AutoPivotProperties.CHARSET, "UTF-8");
		AutoPivotProperties autoPivotProperties = new AutoPivotProperties();
		autoPivotProperties.loadConfiguration(props);
		assertThat(autoPivotProperties.getCharset(), equalTo("UTF-8"));
	}
	
	@Test(expected = ParseException.class)
	public void shouldThrowParseExceptionWhenRootKeyIsNotFollowedByIdentifier() throws ParseException {
		Properties props = new Properties();
		props.put(DataInfo.DATA_INFO_ROOT_KEY, "dummyValue");
		AutoPivotProperties autoPivotProperties = new AutoPivotProperties();
		autoPivotProperties.loadConfiguration(props);
	}
	
	protected Properties createPropertyForTest(String root, String property, String value) {
		Properties props = new Properties();
		props.put(root + "test." + property, value);
		return props;
	}
	
	@Test
	public void shouldCreateDataInfoWhenPropertyIsDefinedWithValidRootKey() throws ParseException {
		AutoPivotProperties autoPivotProperties = new AutoPivotProperties();
		autoPivotProperties.loadConfiguration(createPropertyForTest(DataInfo.DATA_INFO_ROOT_KEY, "dummyKey", "dummyValue"));
		assertNotNull(autoPivotProperties.getDataInfoMap());
		assertThat(autoPivotProperties.getDataInfoMap().entrySet(), hasSize(1));
		assertThat(autoPivotProperties.getDataInfoMap(), hasEntry(equalTo("test"), instanceOf(DataInfo.class)));
	}
	
	@Test
	public void shouldCreateRefDataInfoWhenPropertyIsDefinedWithValidRootKey() throws ParseException {
		AutoPivotProperties autoPivotProperties = new AutoPivotProperties();
		autoPivotProperties.loadConfiguration(createPropertyForTest(RefDataInfo.REF_DATA_INFO_ROOT_KEY, "dummyKey", "dummyValue"));
		assertNotNull(autoPivotProperties.getRefDataInfoMap());
		assertThat(autoPivotProperties.getRefDataInfoMap().entrySet(), hasSize(1));
		assertThat(autoPivotProperties.getRefDataInfoMap(), hasEntry(equalTo("test"), instanceOf(RefDataInfo.class)));
	}
	
	@Test
	public void shouldReturnFileNameWhenPropertyIsDefined() throws ParseException {
		AutoPivotProperties autoPivotProperties = new AutoPivotProperties();
		autoPivotProperties.loadConfiguration(createPropertyForTest(DataInfo.DATA_INFO_ROOT_KEY, 
																	DataInfo.DATA_INFO_FILENAME,
																	"dummyValue"));
		DataInfo dataInfo = autoPivotProperties.getDataInfoMap().get("test");
		assertNotNull(dataInfo);
		assertFalse(Strings.isNullOrEmpty(dataInfo.getFileName()));
		assertThat(dataInfo.getFileName(), equalTo("dummyValue"));
	}
	
	@Test
	public void shouldReturnDirToWatchWhenPropertyIsDefined() throws ParseException {
		AutoPivotProperties autoPivotProperties = new AutoPivotProperties();
		autoPivotProperties.loadConfiguration(createPropertyForTest(DataInfo.DATA_INFO_ROOT_KEY, 
																	DataInfo.DATA_INFO_DIR_TO_WATCH,
																	"dummyValue"));
		DataInfo dataInfo = autoPivotProperties.getDataInfoMap().get("test");
		assertNotNull(dataInfo);
		assertFalse(Strings.isNullOrEmpty(dataInfo.getDirToWatch()));
		assertThat(dataInfo.getDirToWatch(), equalTo("dummyValue"));
	}
	
	@Test
	public void shouldReturnPathMatcherWhenPropertyIsDefined() throws ParseException {
		AutoPivotProperties autoPivotProperties = new AutoPivotProperties();
		autoPivotProperties.loadConfiguration(createPropertyForTest(DataInfo.DATA_INFO_ROOT_KEY, 
																	DataInfo.DATA_INFO_PATHMATCHER,
																	"dummyValue"));
		DataInfo dataInfo = autoPivotProperties.getDataInfoMap().get("test");
		assertNotNull(dataInfo);
		assertFalse(Strings.isNullOrEmpty(dataInfo.getPathMatcher()));
		assertThat(dataInfo.getPathMatcher(), equalTo("dummyValue"));
	}
	
	@Test
	public void shouldReturnDefaultPathMatcherWhenPropertyIsUndefined() throws ParseException {
		AutoPivotProperties autoPivotProperties = new AutoPivotProperties();
		autoPivotProperties.loadConfiguration(createPropertyForTest(DataInfo.DATA_INFO_ROOT_KEY, 
																	"dummyKey",
																	"dummyValue"));
		DataInfo dataInfo = autoPivotProperties.getDataInfoMap().get("test");
		assertNotNull(dataInfo);
		assertFalse(Strings.isNullOrEmpty(dataInfo.getPathMatcher()));
		assertThat(dataInfo.getPathMatcher(), equalTo(APropertyInfo.DEFAULT_PATH_MATCHER));
	}
	
	@Test
	public void shouldReturnDataStorePartitionFieldWhenPropertyIsDefined() throws ParseException {
		AutoPivotProperties autoPivotProperties = new AutoPivotProperties();
		autoPivotProperties.loadConfiguration(createPropertyForTest(DataInfo.DATA_INFO_ROOT_KEY, 
																	DataInfo.DATA_INFO_DATASTORE_PARTITIONFIELD,
																	"dummyValue"));
		DataInfo dataInfo = autoPivotProperties.getDataInfoMap().get("test");
		assertNotNull(dataInfo);
		assertFalse(Strings.isNullOrEmpty(dataInfo.getDataStorePartitionField()));
		assertThat(dataInfo.getDataStorePartitionField(), equalTo("dummyValue"));
	}
	
	@Test
	public void shouldReturnPivotCacheSizeWhenPropertyIsDefined() throws ParseException {
		AutoPivotProperties autoPivotProperties = new AutoPivotProperties();
		autoPivotProperties.loadConfiguration(createPropertyForTest(DataInfo.DATA_INFO_ROOT_KEY, 
																	DataInfo.DATA_INFO_PIVOT_CACHE_SIZE,
																	"100"));
		DataInfo dataInfo = autoPivotProperties.getDataInfoMap().get("test");
		assertNotNull(dataInfo);
		assertNotNull(dataInfo.getPivotCacheSize());
		assertThat(dataInfo.getPivotCacheSize(), equalTo(100));
	}
	
	@Test(expected = NumberFormatException.class)
	public void shouldThrowNumberFormatExceptionWhenPivotCacheSizeIsNotAValidNumber() throws ParseException {
		AutoPivotProperties autoPivotProperties = new AutoPivotProperties();
		autoPivotProperties.loadConfiguration(createPropertyForTest(DataInfo.DATA_INFO_ROOT_KEY, 
																	DataInfo.DATA_INFO_PIVOT_CACHE_SIZE,
																	"cent"));
		DataInfo dataInfo = autoPivotProperties.getDataInfoMap().get("test");
		assertNotNull(dataInfo);
		dataInfo.getPivotCacheSize();
	}
	
	@Test
	public void shouldReturnNullWhenPivotCacheSizeIsEmpty() throws ParseException {
		AutoPivotProperties autoPivotProperties = new AutoPivotProperties();
		autoPivotProperties.loadConfiguration(createPropertyForTest(DataInfo.DATA_INFO_ROOT_KEY, 
																	DataInfo.DATA_INFO_PIVOT_CACHE_SIZE,
																	""));
		DataInfo dataInfo = autoPivotProperties.getDataInfoMap().get("test");
		assertNotNull(dataInfo);
		assertNull(dataInfo.getPivotCacheSize());
	}
	
	@Test
	public void shouldReturnDefaultWhenAggregateProviderTypeIsUndefined() throws ParseException {
		AutoPivotProperties autoPivotProperties = new AutoPivotProperties();
		autoPivotProperties.loadConfiguration(createPropertyForTest(DataInfo.DATA_INFO_ROOT_KEY, 
																	"dummyKey",
																	"dummyValue"));
		DataInfo dataInfo = autoPivotProperties.getDataInfoMap().get("test");
		assertNotNull(dataInfo);
		assertNotNull(dataInfo.getAggregateProviderType());
		assertThat(dataInfo.getAggregateProviderType(), equalTo(APropertyInfo.DEFAULT_AGGREGATE_PROVIDER_TYPE));
	}
	
	@Test
	public void shouldReturnAggregateProviderTypeWhenPropertyIsDefined() throws ParseException {
		AutoPivotProperties autoPivotProperties = new AutoPivotProperties();
		autoPivotProperties.loadConfiguration(createPropertyForTest(DataInfo.DATA_INFO_ROOT_KEY, 
																	DataInfo.DATA_INFO_AGGREGATE_PROVIDER_TYPE,
																	AGGREGATE_PROVIDER_TYPE.BITMAP.name()));
		DataInfo dataInfo = autoPivotProperties.getDataInfoMap().get("test");
		assertNotNull(dataInfo);
		assertNotNull(dataInfo.getAggregateProviderType());
		assertThat(dataInfo.getAggregateProviderType(), equalTo(AGGREGATE_PROVIDER_TYPE.BITMAP));
	}
}
