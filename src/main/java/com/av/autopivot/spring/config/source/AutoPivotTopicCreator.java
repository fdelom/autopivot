package com.av.autopivot.spring.config.source;

import java.nio.file.FileSystems;
import java.nio.file.Path;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

import com.av.autopivot.AutoPivotDiscoveryCreator;
import com.av.csv.CSVFormat;
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
	protected static Logger LOGGER = Logger.getLogger(AutoPivotTopicCreator.class);
	
	/** Spring environment, automatically wired */
	@Autowired
	protected Environment env;
	
	private AutoPivotDiscoveryCreator autoPivotDiscoveryCreator = null;
	
	public AutoPivotTopicCreator(AutoPivotDiscoveryCreator autoPivotDiscoveryCreator) {
		this.autoPivotDiscoveryCreator = autoPivotDiscoveryCreator;
	}
	
	public ICSVTopic<Path> createTopic(String topicName) {
		ICSVTopic<Path> topic = null;
		
		CSVFormat discovery = autoPivotDiscoveryCreator.createDiscoveryFormat();
		if (discovery == null) {
			throw new QuartetRuntimeException("Failed to initialize CSV Format");
		}
		
		Boolean bFwActivated = env.getProperty("filewatcher.actived", Boolean.class, false);
		if (bFwActivated.equals(true)) {
			String pathMatcherConf = autoPivotDiscoveryCreator.getPathMatcher();
			
			// Create parser Configuration
			CSVParserConfiguration cfg = createParserConfiguration(discovery);
			
			// Load files with watcher activated
			topic = new DirectoryCSVTopic(topicName, cfg, autoPivotDiscoveryCreator.getDirectoryPathToWatch(), 
										  FileSystems.getDefault().getPathMatcher(pathMatcherConf), 500);
		}
		else {
			String fileNameField = env.getRequiredProperty("fileName");
			
			// Create parser configuration
			CSVParserConfiguration cfg = createParserConfiguration(discovery);
			
			// Load only the target file
			IWatcherService watcherService = new WatcherService();
			topic = new SingleFileCSVTopic(topicName, cfg, fileNameField, watcherService);
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
		cfg.setProcessQuotes(false);
		return cfg;
	}
}
