package com.nextlabs.common;

import java.util.Properties;

/**
 * This abstract class provide the framework for all different provider.
 * 
 * @author klee
 *
 */
public interface Provider {
	
	/**
	 * Pass in the common properties
	 * @param props Properties list from a properties file 
	 */
	public void setCommonProperties(Properties props);

	/**
	 * @param id ID of the user
	 * @param attributeToSearch Attribute name to search for
	 * @return UserObject for the matched user
	 * @throws Exception java.lang.Exception
	 */
	public UserObject getUserObject(String id, String attributeToSearch) throws Exception;
	
	/**
	 * @param id ID of the resource
	 * @param attributeToSearch Attribute name to search for
	 * @return ResourceObject for the matched resource
	 * @throws Exception java.lang.Exception
	 */
	public ResourceObject getItemObject(String id, String attributeToSearch) throws Exception;
	
	/**
	 * @param id ID_HD of the program
	 * @param attributeToSearch  Attribute name to search for
	 * @return ResourceObject for the matched resource
	 * @throws Exception java.lang.Exception
	 */
	public ResourceObject getProgramObject(String id, String attributeToSearch) throws Exception;
	
	/**
	 * Reload the cache store
	 */
	public void refreshCache();

	/**
	 * Load the Profile for given Properties list
	 * @param props properties list for the 
	 * @param propsFilePath Properties absolute file path
	 */
	public void loadProfiles(Properties props, String propsFilePath);
	
	/**
	 * Load Single Profile 
	 * @param props properties list for the 
	 * @param propsFilePath Properties absolute file path
	 */
	public void loadSingleProfile(Properties props, String propsFilePath);
	
	/**
	 * Set as single profile
	 * @param isSingleProfile true if this is Single Profile
	 */
	public void setIsSingleProfile(Boolean isSingleProfile);	
	
	/**
	 * Status of the cache refreshing
	 * @return true if the cache store is refreshing
	 */
	public Boolean isRefreshing();
	
}
	