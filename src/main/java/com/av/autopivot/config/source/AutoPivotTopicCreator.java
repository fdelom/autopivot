package com.av.autopivot.config.source;

import java.nio.file.FileSystems;
import java.nio.file.Path;

import org.springframework.context.annotation.Bean;

import com.av.autopivot.AutoPivotDiscoveryCreator;
import com.av.autopivot.config.properties.AutoPivotProperties.DataInfo;
import com.av.csv.CSVFormat;
import com.google.common.base.Strings;
import com.qfs.msg.IWatcherService;
import com.qfs.msg.csv.ICSVTopic;
import com.qfs.msg.csv.filesystem.impl.DirectoryCSVTopic;
import com.qfs.msg.csv.filesystem.impl.SingleFileCSVTopic;
import com.qfs.msg.csv.impl.CSVParserConfiguration;
import com.qfs.msg.impl.WatcherService;
import com.quartetfs.fwk.QuartetRuntimeException;
import com.sun.istack.logging.Logger;

public class AutoPivotTopicCreator {
	/** Logger **/
	protected static final Logger LOGGER = Logger.getLogger(AutoPivotTopicCreator.class);
	
	private AutoPivotDiscoveryCreator autoPivotDiscoveryCreator = null;
	
	public AutoPivotTopicCreator(AutoPivotDiscoveryCreator autoPivotDiscoveryCreator) {
		this.autoPivotDiscoveryCreator = autoPivotDiscoveryCreator;
	}
	
	public ICSVTopic<Path> createTopic(CSVFormat discovery, String storeName, DataInfo dataInfo) {
		ICSVTopic<Path> topic = null;
		
		if (discovery == null) {
			throw new QuartetRuntimeException("Failed to initialize CSV Format");
		}
		
		if (Strings.isNullOrEmpty(dataInfo.getDirToWatch()) == false) {			
			// Create parser Configuration
			CSVParserConfiguration cfg = createParserConfiguration(discovery);
			
			// Load files with watcher activated
			IWatcherService watcherService = watcherService();
			topic = new DirectoryCSVTopic(storeName, cfg, autoPivotDiscoveryCreator.getDirectoryPathToWatch(dataInfo), 
										  FileSystems.getDefault().getPathMatcher(dataInfo.getPathMatcher()), watcherService);
		}
		else {
			String fileNameField = dataInfo.getFileName();
			
			// Create parser configuration
			CSVParserConfiguration cfg = createParserConfiguration(discovery);
			
			// Load only the target file
			IWatcherService watcherService = watcherService();
			topic = new SingleFileCSVTopic(storeName, cfg, fileNameField, watcherService);
		}
		return topic;
	}
	
	/**
	 * Create the CSV parser configuration
	 * 
	 * @param discovery CSV format information (CSV separator, column types)
	 * @return CSVParserConfiguration ready to be used
	 */
	public CSVParserConfiguration createParserConfiguration(CSVFormat discovery) {
		CSVParserConfiguration cfg = new CSVParserConfiguration(autoPivotDiscoveryCreator.getCharset(),
																discovery.getSeparator().charAt(0),
																discovery.getColumnCount(),
																true, 1,
																CSVParserConfiguration.toMap(discovery.getColumnNames()));
		cfg.setProcessQuotes(true);
		return cfg;
	}

	public ICSVTopic<Path> createRefTopic(CSVFormat discovery) {
		ICSVTopic<Path> topic = null;
		
		// Create parser configuration
		CSVParserConfiguration cfg = createParserConfiguration(discovery);
		
		// Load only the target file
		IWatcherService watcherService = watcherService();
		topic = new SingleFileCSVTopic(discovery.getFileNameWithoutExtension(), cfg, discovery.getFileName(), watcherService);
		
		return topic;
	}
	
	@Bean
	IWatcherService watcherService() {
		return new WatcherService();
	}
}
