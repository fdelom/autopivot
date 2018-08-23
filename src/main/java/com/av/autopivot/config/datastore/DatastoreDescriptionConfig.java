/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.av.autopivot.config.datastore;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

import com.av.autopivot.AutoPivotDiscoveryCreator;
import com.av.autopivot.AutoPivotGenerator;
import com.av.autopivot.StoreInfo;
import com.av.autopivot.config.properties.AutoPivotProperties;
import com.av.autopivot.config.properties.AutoPivotProperties.DataInfo;
import com.av.autopivot.config.properties.AutoPivotProperties.RefDataInfo;
import com.av.csv.CSVFormat;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.desc.IReferenceDescription;
import com.qfs.desc.IStoreDescription;
import com.qfs.desc.impl.DatastoreSchemaDescription;
import com.qfs.server.cfg.IDatastoreDescriptionConfig;
import com.quartetfs.fwk.impl.Pair;

/**
 * 
 * Description of the datastore.
 * 
 * @author ActiveViam
 *
 */
public class DatastoreDescriptionConfig implements IDatastoreDescriptionConfig {

	/** AutoPivot Configuration */
	@Autowired
	protected AutoPivotProperties autoPivotProps;
	
	/** AutoPivotDiscoveryCreator */
	@Autowired
	protected AutoPivotDiscoveryCreator discoveryCreator;
	
	/**
	 * 
	 * Generator of store and cube descriptions.
	 * 
	 * @return ActivePivot generator
	 */
	@Bean
	public AutoPivotGenerator generator() {
		return new AutoPivotGenerator();
	}
	
	/** @return the references between stores */
	public Collection<IReferenceDescription> references() {
		final Collection<IReferenceDescription> references = new LinkedList<>();

		return references;
	}
	
	/**
	 *
	 * Provide the schema description of the datastore.
	 * <p>
	 * It is based on the descriptions of the stores in
	 * the datastore, the descriptions of the references
	 * between those stores, and the optimizations and
	 * constraints set on the schema.
	 *
	 * @return schema description
	 */
	@Bean
	public IDatastoreSchemaDescription schemaDescription() {	
		final Collection<IStoreDescription> stores = new LinkedList<>();
		stores.addAll(generateFromData());
		stores.addAll(generateFromRefDate());
		return new DatastoreSchemaDescription(stores, references());
	}
	
	private Collection<IStoreDescription> generateFromData() {
		final Collection<IStoreDescription> stores = new LinkedList<>();
		Map<String, DataInfo> dataInfoMap = autoPivotProps.getDataInfoMap();
		AutoPivotGenerator generator = generator();
		for (Entry<String, DataInfo> entry : dataInfoMap.entrySet()) {			
			CSVFormat discovery = discoveryCreator.createDiscoveryFormat(entry.getValue());
			
			StoreInfo storeDesc = StoreInfo.createStoreInfo(entry.getKey(), entry.getValue(), discovery);
			stores.add(generator.createStoreDescription(storeDesc));
		}
		return stores;
	}
	
	private Collection<IStoreDescription> generateFromRefDate() {
		final Collection<IStoreDescription> stores = new LinkedList<>();
		List<Pair<RefDataInfo, CSVFormat>> discoveryList = discoveryCreator.createDiscoveryRefFormat();
		AutoPivotGenerator generator = generator();
		
		for (Pair<RefDataInfo, CSVFormat> pair : discoveryList) {
			RefDataInfo refDataInfo = pair.getLeft();
			CSVFormat discovery = pair.getRight();
			StoreInfo storeDesc = StoreInfo.createStoreInfo(discovery.getFileNameWithoutExtension(), refDataInfo, discovery);
			stores.add(generator.createStoreDescription(storeDesc));
		}
		return stores;	
	}

}
