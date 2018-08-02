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
package com.av.autopivot.spring.config.source;


import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;

import com.av.autopivot.AutoPivotDiscoveryCreator;
import com.av.autopivot.AutoPivotGenerator;
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
	protected static Logger LOGGER = Logger.getLogger(SourceConfig.class.getName());

	/** Property to identify the name of the file to load */
	public static final String FILENAME_PROPERTY = "fileName";


	/** Spring environment, automatically wired */
	@Autowired
	protected Environment env;

	/** Application datastore, automatically wired */
	@Autowired
	protected DatastoreConfig datastoreConfig;

	/** Create and configure the CSV engine */
	@Bean
	public ICSVSource<Path> CSVSource() throws IOException {
		
		// Allocate half the the machine cores to CSV parsing
		Integer parserThreads = Math.min(8, Math.max(1, IPlatform.CURRENT_PLATFORM.getProcessorCount() / 2));
		LOGGER.info("Allocating " + parserThreads + " parser threads.");
		
		CSVSource<Path> source = new CSVSource<Path>();
		Properties properties = new Properties();
		properties.put(ICSVSourceConfiguration.BUFFER_SIZE_PROPERTY, "256");
		properties.put(ICSVSourceConfiguration.PARSER_THREAD_PROPERTY, parserThreads.toString());
		source.configure(properties);
		
		return source;
	}
	
	/** Discover the input data file (CSV separator, column types) */
	@Bean
	public AutoPivotDiscoveryCreator discoverCreator() {
		return new AutoPivotDiscoveryCreator();
	}


	/**
	 * Load the CSV file
	 */
	@Bean
	@DependsOn(value = "startManager")
	public Void loadData(ICSVSource<Path> source) throws Exception {
		
		AutoPivotDiscoveryCreator discoveryCreator = discoverCreator();
		AutoPivotTopicCreator topicCreator = new AutoPivotTopicCreator(discoveryCreator);
		ICSVTopic<Path> topic = topicCreator.createTopic(AutoPivotGenerator.BASE_STORE);
		CSVFormat discovery = discoveryCreator.createDiscoveryFormat();
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
		channelFactory.setCalculatedColumns(AutoPivotGenerator.BASE_STORE, calculatedColumns);
		
		
		// Create Listener to have an effective filewatching
		final ITuplePublisher<IFileInfo<Path>> publisher 
						= new AutoCommitTuplePublisher<>(new TuplePublisher<>(datastoreConfig.datastore(), 
														 AutoPivotGenerator.BASE_STORE));
		IStoreMessageChannel<IFileInfo<Path>, ILineReader> channel
						= channelFactory.createChannel(AutoPivotGenerator.BASE_STORE,
													   AutoPivotGenerator.BASE_STORE,
													   publisher);
		source.listen(channel);
		
		LOGGER.info("AutoPivot initial loading complete.");
		
		return null; // Void
	}
	
}
