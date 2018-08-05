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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import com.av.autopivot.AutoPivotGenerator;
import com.av.autopivot.AutoPivotRefDataGenerator;
import com.av.autopivot.config.source.SourceConfig;
import com.av.csv.CSVFormat;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.desc.IReferenceDescription;
import com.qfs.desc.IStoreDescription;
import com.qfs.desc.impl.DatastoreSchemaDescription;
import com.qfs.server.cfg.IDatastoreDescriptionConfig;

/**
 * 
 * Description of the datastore.
 * 
 * @author ActiveViam
 *
 */
public class DatastoreDescriptionConfig implements IDatastoreDescriptionConfig {

	/** Spring environment, automatically wired */
	@Autowired
	protected Environment env;

	/** Source configuration */
	@Autowired
	protected SourceConfig sourceConfig;

	
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
	
	/**
	 * 
	 * Generator of store and cube descriptions.
	 * 
	 * @return ActivePivot Reference Data generator
	 */
	@Bean
	public AutoPivotRefDataGenerator generatorRefData() {
		return new AutoPivotRefDataGenerator();
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
		stores.add(generateFromData());
		stores.addAll(generateFromRefDate());
		return new DatastoreSchemaDescription(stores, references());
	}
	
	private IStoreDescription generateFromData() {
		CSVFormat discovery = sourceConfig.discoverCreator().createDiscoveryFormat();
		AutoPivotGenerator generator = generator();
		return generator.createStoreDescription(discovery, env);
	}
	
	private Collection<IStoreDescription> generateFromRefDate() {
		List<CSVFormat> discoveryList = sourceConfig.discoverCreator().createDiscoveryRefFormat();
		AutoPivotRefDataGenerator generator = generatorRefData();
		
		final Collection<IStoreDescription> stores = new LinkedList<>();	
		for (CSVFormat csvFormat : discoveryList) {
			stores.add(generator.createStoreDescription(csvFormat.getFileNameWithoutExtension(), csvFormat, env));
		}
		return stores;		
	}

}
