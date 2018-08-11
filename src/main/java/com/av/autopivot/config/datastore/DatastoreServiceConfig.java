/*
 * (C) ActiveViam FS 2013-2018
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.av.autopivot.config.datastore;

import static com.av.autopivot.security.SecurityConstant.ROLE_ADMIN;
import static com.av.autopivot.security.SecurityConstant.ROLE_USER;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import com.av.autopivot.AutoPivotDiscoveryCreator;
import com.av.autopivot.config.properties.AutoPivotProperties;
import com.av.autopivot.config.properties.AutoPivotProperties.DataInfo;
import com.av.autopivot.config.properties.AutoPivotProperties.RefDataInfo;
import com.av.csv.CSVFormat;
import com.qfs.desc.IStoreSecurity;
import com.qfs.desc.IStoreSecurityBuilder;
import com.qfs.desc.impl.StoreSecurityBuilder;
import com.qfs.service.store.IDatastoreServiceConfiguration;
import com.quartetfs.fwk.format.IFormatter;
import com.quartetfs.fwk.format.IParser;
import com.quartetfs.fwk.impl.Pair;

/**
 * @author ActiveViam
 */
@Configuration
public class DatastoreServiceConfig implements IDatastoreServiceConfiguration {

	/** @see #getStoresSecurity() */
	protected Map<String, IStoreSecurity> storesSecurity;
	
	/** @see #getCustomParsers() */
	protected Map<String, Map<String, IParser<?>>> customParsers;

	/** @see #getCustomFormatters() */
	protected Map<String, Map<String, IFormatter>> customFormatters;

	/** Default query timeout for queries */
	protected static final long DEFAULT_QUERY_TIMEOUT = 30_000L;
	
	/** AutoPivotDiscoveryCreator */
	@Autowired
	protected AutoPivotDiscoveryCreator discoveryCreator;

	/**
	 * Constructor of {@link DatastoreServiceConfig}.
	 */
	public DatastoreServiceConfig(AutoPivotProperties autoPivotProperties, AutoPivotDiscoveryCreator discoveryCreator) {

		// SECURITY
		this.storesSecurity = new HashMap<>();
		IStoreSecurityBuilder builder = StoreSecurityBuilder.startBuildingStoreSecurity()
				.supportInsertion()
				.supportDeletion()
				.withStoreWriters(ROLE_ADMIN)
				.withStoreReaders(ROLE_USER);

		IStoreSecurity storeSecurity = builder.build();
		Map<String, DataInfo> dataInfoMap = autoPivotProperties.getDataInfoMap();
		for (String storeName : dataInfoMap.keySet()) {
			storesSecurity.put(storeName, storeSecurity);
		}
		
		List<Pair<RefDataInfo, CSVFormat>> discoveryList = discoveryCreator.createDiscoveryRefFormat();
		for (Pair<RefDataInfo, CSVFormat> pair : discoveryList) {
			CSVFormat discovery = pair.getRight();
			storesSecurity.put(discovery.getFileNameWithoutExtension(), storeSecurity);
		}
		
		// ADDITIONAL FORMATTERS
		this.customFormatters = new HashMap<>();

		// ADDITIONAL PARSERS
		this.customParsers = new HashMap<>();
	}

	@Override
	public Map<String, Map<String, IParser<?>>> getCustomParsers() {
		return this.customParsers;
	}

	@Override
	public Map<String, Map<String, IFormatter>> getCustomFormatters() {
		return this.customFormatters;
	}

	@Override
	public Map<String, IStoreSecurity> getStoresSecurity() {
		return storesSecurity;
	}

	@Override
	public long getDefaultQueryTimeout() { return DEFAULT_QUERY_TIMEOUT; }

}
