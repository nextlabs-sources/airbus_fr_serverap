package com.nextlabs.common;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class handle the properties file loading from PDP jservice folder
 * @author klee
 *
 */
public class PropertyLoader {

	private static final Log LOG = LogFactory.getLog(PropertyLoader.class);

	/**
	 * Load the properties from the file path given
	 * @param filePath relative file path of properties file
	 * @return Properties list of the given file path
	 */
	public static Properties loadPropertiesInPDP(String filePath) {

		String installLoc = Util.findInstallFolder();

		LOG.info(String.format("Install location is: %s", installLoc));

		if (filePath == null) {
			LOG.error("File path is undefined");
		}

		filePath = installLoc + filePath;

		LOG.info("Properties File Path:: " + filePath);

		Properties result = null;

		try {
			File file = new File(filePath);

			if (file != null) {
				FileInputStream fis = new FileInputStream(file);
				result = new Properties();
				result.load(fis); // Can throw IOException
			}
		} catch (Exception e) {
			LOG.error("Error parsing properties file ", e);
			result = null;
		}
		return result;
	}
	
	/**
	 * Retrieve absolute file path from the given relative fila path.
	 * @param filePath relative file path of properties file
	 * @return Absolute file path from the given relative 
	 */
	public static String getPropertiesFilePath(String filePath) {
		
		String installLoc = Util.findInstallFolder();

		LOG.info(String.format("Install location is: %s", installLoc));

		if (filePath == null) {
			LOG.error("File path is undefined");
		}

		return (installLoc + filePath);
		
	}

	/**
	 * Load the properties from the file path given
	 * @param filePath Absolute file path of the properties file
	 * @return Properties list of the given file path
	 */
	public static Properties loadPropertiesDirectly(String filePath) {
		Properties result = null;
		try {
			File file = new File(filePath);
			LOG.info("Properties File Path:: " + file.getAbsolutePath());
			if (file != null) {
				FileInputStream fis = new FileInputStream(file);
				result = new Properties();
				result.load(fis); // Can throw IOException
			}
		} catch (Exception e) {
			LOG.error("Error parsing properties file ", e);
			result = null;
		}
		return result;
	}
}
