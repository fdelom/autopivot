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
package com.av.autopivot.config.source;


import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import com.av.autopivot.AutoPivotDiscoveryCreator;
import com.av.autopivot.config.properties.AutoPivotProperties;
import com.av.autopivot.config.properties.AutoPivotProperties.DataInfo;
import com.av.autopivot.config.properties.AutoPivotProperties.RefDataInfo;
import com.av.csv.CSVFormat;
import com.av.csv.calculator.DateDayCalculator;
import com.av.csv.calculator.DateMonthCalculator;
import com.av.csv.calculator.DateYearCalculator;
import com.qfs.msg.IColumnCalculator;
import com.qfs.msg.csv.ICSVSource;
import com.qfs.msg.csv.ICSVSourceConfiguration;
import com.qfs.msg.csv.ICSVTopic;
import com.qfs.msg.csv.IFileInfo;
import com.qfs.msg.csv.ILineReader;
import com.qfs.msg.csv.impl.CSVSource;
import com.qfs.platform.IPlatform;
import com.qfs.server.cfg.impl.DatastoreConfig;
import com.qfs.source.IStoreMessageChannel;
import com.qfs.source.ITuplePublisher;
import com.qfs.source.impl.AutoCommitTuplePublisher;
import com.qfs.source.impl.CSVMessageChannelFactory;
import com.qfs.source.impl.TuplePublisher;
import com.quartetfs.fwk.impl.Pair;

/**
 *
 * Spring configuration of the Sandbox ActivePivot server.<br>
 * The parameters of the Sandbox ActivePivot server can be quickly changed by modifying the
 * pojo.properties file.
 *
 * @author ActiveViam
 *
 */
@Configuration
public class SourceConfig {

	/** Logger **/
	protected static final Logger LOGGER = Logger.getLogger(SourceConfig.class.getName());
	
	/** AutoPivot Properties */
	@Autowired
	protected AutoPivotProperties autoPivotProps;

	/** Application datastore, automatically wired */
	@Autowired
	protected DatastoreConfig datastoreConfig;
	
	/** AutoPivotDiscoveryCreator */
	@Autowired
	protected AutoPivotDiscoveryCreator discoveryCreator;

	/** Create and configure the CSV engine */
	private ICSVSource<Path> createCSVSource(String sourceName) {
		
		// Allocate half the the machine cores to CSV parsing
		Integer parserThreads = Math.min(8, Math.max(1, IPlatform.CURRENT_PLATFORM.getProcessorCount() / 2));
		LOGGER.info("Allocating " + parserThreads + " parser threads.");
		
		CSVSource<Path> source = new CSVSource<Path>("CSVSource_" + sourceName);;
		Properties properties = new Properties();
		properties.put(ICSVSourceConfiguration.BUFFER_SIZE_PROPERTY, "256");
		properties.put(ICSVSourceConfiguration.PARSER_THREAD_PROPERTY, parserThreads.toString());
		source.configure(properties);
		
		return source;
	}
	
	/**
	 * Load the CSV file
	 */
	@Bean
	@DependsOn(value = "startManager")
	public Void loadAllData() throws Exception {
		
		loadData();
		loadRefData();
		
		LOGGER.info("AutoPivot initial loading complete.");
		
		return null; // Void
	}

	private void loadRefData() {
		AutoPivotTopicCreator topicCreator = new AutoPivotTopicCreator(discoveryCreator);
		List<Pair<RefDataInfo, CSVFormat>> discoveryList = discoveryCreator.createDiscoveryRefFormat();

		// Derive calculated columns
		for (Pair<RefDataInfo, CSVFormat> pair : discoveryList) {
			
			CSVFormat discovery = pair.getRight();
			ICSVSource<Path> source = createCSVSource(discovery.getFileNameWithoutExtension());
			CSVMessageChannelFactory<Path> channelFactory = new CSVMessageChannelFactory<>(source, datastoreConfig.datastore());
			ICSVTopic<Path> topic = topicCreator.createRefTopic(discovery);
			
			source.addTopic(topic);
			
			List<IColumnCalculator<ILineReader>> calculatedColumns = new ArrayList<IColumnCalculator<ILineReader>>();
			for(int c = 0; c < discovery.getColumnCount(); c++) {
				String columnName = discovery.getColumnName(c);
				String columnType = discovery.getColumnType(c);
				
				// When a date field is detected, we automatically
				// calculate the YEAR, MONTH and DAY fields.
				if(columnType.startsWith("DATE")) {
					calculatedColumns.add(new DateYearCalculator(columnName, columnName + ".YEAR"));
					calculatedColumns.add(new DateMonthCalculator(columnName, columnName + ".MONTH"));
					calculatedColumns.add(new DateDayCalculator(columnName, columnName + ".DAY"));
				}
				
			};
			channelFactory.setCalculatedColumns(discovery.getFileNameWithoutExtension(), calculatedColumns);
		
			// Create Listener to have an effective filewatching
			final ITuplePublisher<IFileInfo<Path>> publisher 
							= new AutoCommitTuplePublisher<>(new TuplePublisher<>(datastoreConfig.datastore(), 
															 discovery.getFileNameWithoutExtension()));
			IStoreMessageChannel<IFileInfo<Path>, ILineReader> channel
							= channelFactory.createChannel(discovery.getFileNameWithoutExtension(),
														   discovery.getFileNameWithoutExtension(),
														   publisher);
			source.listen(channel);
		}
	}

	private void loadData() {
		Map<String, DataInfo> dataInfoMap = autoPivotProps.getDataInfoMap();

		for (String dataToLoad : dataInfoMap.keySet()) {
			CSVFormat discovery = discoveryCreator.createDiscoveryFormat(dataInfoMap.get(dataToLoad));
			ICSVSource<Path> source = createCSVSource(dataToLoad);
			AutoPivotTopicCreator topicCreator = new AutoPivotTopicCreator(discoveryCreator);
			ICSVTopic<Path> topic = topicCreator.createTopic(discovery, dataToLoad, dataInfoMap.get(dataToLoad));
			
			source.addTopic(topic);
			
			CSVMessageChannelFactory<Path> channelFactory = new CSVMessageChannelFactory<>(source, datastoreConfig.datastore());
			
			// Derive calculated columns
			List<IColumnCalculator<ILineReader>> calculatedColumns = new ArrayList<IColumnCalculator<ILineReader>>();
			for(int c = 0; c < discovery.getColumnCount(); c++) {
				String columnName = discovery.getColumnName(c);
				String columnType = discovery.getColumnType(c);
				
				// When a date field is detected, we automatically
				// calculate the YEAR, MONTH and DAY fields.
				if(columnType.startsWith("DATE")) {
					calculatedColumns.add(new DateYearCalculator(columnName, columnName + ".YEAR"));
					calculatedColumns.add(new DateMonthCalculator(columnName, columnName + ".MONTH"));
					calculatedColumns.add(new DateDayCalculator(columnName, columnName + ".DAY"));
				}
				
			};
			channelFactory.setCalculatedColumns(dataToLoad, calculatedColumns);
			
			// Create Listener to have an effective filewatching
			final ITuplePublisher<IFileInfo<Path>> publisher 
							= new AutoCommitTuplePublisher<>(new TuplePublisher<>(datastoreConfig.datastore(), 
																				  dataToLoad));
			IStoreMessageChannel<IFileInfo<Path>, ILineReader> channel
							= channelFactory.createChannel(dataToLoad,
														   dataToLoad,
														   publisher);
			source.listen(channel);
		}
	}	
}
