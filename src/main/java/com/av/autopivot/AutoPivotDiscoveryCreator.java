/**
 * 
 */
package com.av.autopivot;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

import com.av.csv.CSVFormat;
import com.av.csv.discover.CSVDiscovery;
import com.quartetfs.fwk.QuartetRuntimeException;
import com.sun.istack.logging.Logger;

public class AutoPivotDiscoveryCreator {
	/** Logger **/
	protected static Logger LOGGER = Logger.getLogger(AutoPivotDiscoveryCreator.class);
	protected static final String DEFAULT_MATCHER = "glob:**.csv";
	
	/** Spring environment, automatically wired */
	@Autowired
	protected Environment env;
	
	public String getPathMatcher() {
		return env.getProperty("filewatcher.pathMatcher", DEFAULT_MATCHER);
	}
	
	public Path getDirectoryPathToWatch() {
		String fwDirField = env.getRequiredProperty("filewatcher.dir");
		
		Path directory;
		try {
			directory = getDirPath(fwDirField);
		} catch (IOException ex) {
			throw new QuartetRuntimeException("Could not discover this directory: {}", fwDirField);
		}
		return directory;
	}

	private Path getDirPath(String dirName) throws IOException {
		Path path = Paths.get(dirName);
		// Standard path
		if (Files.exists(path) && Files.isDirectory(path)) {
			return path;
		}
		// Lookup path in classpath
		else {
			String cleanPath = getCleanPath(getClass().getClassLoader().getResource(dirName).getPath());
			return Paths.get(cleanPath);
		}
	}

	private String getCleanPath(String dirName) {
		// Attemps to get the path of the actual JAR file, because the working directory is frequently not where
		// the file is.
		// Example: file:/D:/Java/MyJar.jar!/MyClass.class
		// or /D:/Java/MyClass.class
		// Find the last ! and cut it off at that location. If this isn't being run from a jar,
		// there is no !, so it will cause an exception.
		try {
			try {
				dirName = dirName.substring(0, dirName.lastIndexOf('!'));
			}
			catch (Exception e) {
			}
			
			// Find the last / and cut it off at that location
			dirName = dirName.substring(0, dirName.lastIndexOf('/') + 1);
			// If it start with /, cut it off
			if (dirName.startsWith("/")) {
				dirName = dirName.substring(1, dirName.length());
			}
			// If it start with file:/, cut it off
			if (dirName.startsWith("file:/")) {
				dirName = dirName.substring(6, dirName.length());
			}
		}
		catch (Exception e) {
			dirName = "";
		}
		return dirName;
	}
	
	/**
	 * @return charset used by the CSV parsers.
	 */
	public Charset getCharset() {
		String charsetName = env.getProperty("charset");
		if(charsetName != null) {
			try {
				return Charset.forName(charsetName);
			} catch(Exception e) {
				LOGGER.warning("Unkown charset: " + charsetName);
			}
		}
		return Charset.defaultCharset();
	}
	
	/**
	 * Discover the input data file within directory (CSV separator, column type)
	 * 
	 * @return CSVFormat used to initialize CSVSource
	 */
	public CSVFormat createDiscoveryFormat() {
		CSVFormat discovery = null;
		Boolean bFwActivated = env.getProperty("filewatcher.activated", Boolean.class, false);
		if (bFwActivated.equals(true)) {
			discovery = discoverDir(getDirectoryPathToWatch());
		}
		else {
			discovery = discoverFile();
		}
		return discovery;
	}

	/**
	 * Discover the input data file (CSV separator, column types)
	 * 
	 * @return CSVFormat used to initialize CSVSource
	 */
	private CSVFormat discoverFile() {
		String fileName = env.getRequiredProperty("fileName");
		try {
			CSVFormat discovery = new CSVDiscovery().discoverFile(fileName, getCharset());
			return discovery;
		} catch(Exception e) {
			throw new QuartetRuntimeException("Could not discover csv file: " + fileName , e);
		}
	}

	/**
	 * Discover the first input data file within the directory and use it as template 
	 * (CSV separator, column types) 
	 * 
	 * @param directory to explore
	 * @return CSVFormat used to initialize CSVSource
	 */
	private CSVFormat discoverDir(Path directory) {
		final ArrayList<String> fileNameList = new ArrayList<>();
		CSVFormat discovery = null;
		
		final PathMatcher pattern = FileSystems.getDefault().getPathMatcher(getPathMatcher());
		FileVisitor<Path> matcherVisitor = new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attribs) {
				Path name = file.getFileName();
				
				if (pattern.matches(name)) {
					fileNameList.add(name.toString());
					return FileVisitResult.TERMINATE;
				}
				return FileVisitResult.CONTINUE;
			}
		};
		
		try {
			Files.walkFileTree(directory, matcherVisitor);
			discovery = new CSVDiscovery().discoverFile(directory.toString() + "\\" + fileNameList.get(0), getCharset());
			return discovery;
		}
		catch (Exception ex) {
			throw new QuartetRuntimeException("Could not discover a csv file with pattern: {} in directory: {}", 
											  getPathMatcher(), directory.toString(), ex);
		}
	}

	public List<CSVFormat> createDiscoveryRefFormat() {
		ArrayList<CSVFormat> discoveryList = null;
		discoveryList = discoverRefDir(getDirectoryRefPathToWatch());
		return discoveryList;
	}
	
	private ArrayList<CSVFormat> discoverRefDir(Path directory) {
		ArrayList<CSVFormat> discoveryList = new ArrayList<>();
		
		final PathMatcher pattern = FileSystems.getDefault().getPathMatcher(getRefPathMatcher());
		FileVisitor<Path> matcherVisitor = new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attribs) {
				Path name = file.getFileName();
				
				if (pattern.matches(name)) {
					CSVFormat discovery;
					try {
						discovery = new CSVDiscovery().discoverFile(directory.toString() + "\\" + name.toString(), getCharset());
					} catch (IOException ex) {
						throw new QuartetRuntimeException("Could not discover a csv file with pattern: {} in directory: {}", 
								  						  getRefPathMatcher(), directory.toString(), ex);
					}
					discoveryList.add(discovery);
				}
				return FileVisitResult.CONTINUE;
			}
		};
		
		try {
			Files.walkFileTree(directory, matcherVisitor);
			return discoveryList;
		}
		catch (Exception ex) {
			throw new QuartetRuntimeException("Could not discover a csv file with pattern: {} in directory: {}", 
											  getRefPathMatcher(), directory.toString(), ex);
		}
	}

	public String getRefPathMatcher() {
		return env.getProperty("filewatcher.refPathMatcher", DEFAULT_MATCHER);
	}

	public Path getDirectoryRefPathToWatch() {
		String fwDirField = env.getRequiredProperty("filewatcher.refdir");
		
		Path directory;
		try {
			directory = getDirPath(fwDirField);
		} catch (IOException ex) {
			throw new QuartetRuntimeException("Could not discover this directory: {}", fwDirField);
		}
		return directory;
	}
	
}