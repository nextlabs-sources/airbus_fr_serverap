package com.nextlabs.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.nextlabs.common.EncryptDecrypt;
import com.nextlabs.common.UserProfile;
import com.nextlabs.exception.InvalidProfileException;

/**
 * Setter and getter class for the user profile
 * 
 * @author klee
 *
 */
public class UserDBProfile extends UserProfile {
	
	private String userName;
	private String encryptedPassword;
	private String decryptedPassword;
	private String connectionUrl;
	private String schema;
	private String databaseDriverName;
	private List<String> keyAttributes;
	private Set<String> attributesToPull;
	private Map<String, Boolean> attributesCardinalityMap;
	private Map<String, String> attributesColumnNameMap;
	private Map<String, Boolean> attributesKeyCaseSensitiveMap;
	private Map<String, String> attributesKeyColumnNameMap;
	private Set<String> linkAttributesToPull;
	private Map<String, Boolean> linkAttributesCardinalityMap;
	private Map<String, String> linkAttributesColumnNameMap;
	private Boolean isValid;
	
	private static final Log LOG = LogFactory.getLog(UserDBProfile.class);
	
	/**
	 * Constructor for the class 
	 * @param name Profile Name
	 */
	public UserDBProfile(String name) {
		super(name);
		keyAttributes = new ArrayList<String>();
		attributesToPull = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
		attributesCardinalityMap = new HashMap<String, Boolean>();
		attributesColumnNameMap = new HashMap<String, String>();
		attributesKeyCaseSensitiveMap = new HashMap<String, Boolean>();
		attributesKeyColumnNameMap = new HashMap<String, String>();
		linkAttributesToPull =  new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
		linkAttributesCardinalityMap = new HashMap<String, Boolean>();
		linkAttributesColumnNameMap = new HashMap<String, String>();
	}
	
	/**
	 * Parsing the profile properties data
	 * @param props Properties list from properties file
	 * @param propFile Absolute file path for the properties file for e.g C:\PDP\nextlabs\jservice\config\UserReferentialPlugin.properties
	 * @throws InvalidProfileException Exception when the parameter is invalid
	 */
	public void parseProfile(Properties props, String propFile) throws InvalidProfileException {
		
		LOG.info(String.format("Started parsing profile for domain [%s]", this.name));

		if (props == null) {
			isValid = false;
			throw new InvalidProfileException("Properties is undefined");
		}
		
		databaseDriverName = getProperty("database_driver_name", props);
		
		if (databaseDriverName == null || databaseDriverName.length() == 0) {
			isValid = false;
			throw new InvalidProfileException("Database Driver Name undefined");
		}
				
		userName = getProperty("database_username", props);

		if (userName == null || userName.length() == 0) {
			isValid = false;
			throw new InvalidProfileException("Database Username is undefined");
		}
		
		EncryptDecrypt cryptor= null;
		
		try {
			cryptor = new EncryptDecrypt(propFile, this.name + "_database_password", this.name + "_database_password_encrypted");
		} catch (Exception e) {
			LOG.error(e);
		}
		encryptedPassword = getProperty("database_password", props);

		if (encryptedPassword == null || encryptedPassword.length() == 0) {
			isValid = false;
			throw new InvalidProfileException("Database Password is undefined");
		}
		
		decryptedPassword = cryptor.getDecryptValue();
		
		if (decryptedPassword == null || decryptedPassword.length() == 0) {
			isValid = false;
			throw new InvalidProfileException("Database Decrypted Password is undefined");
		}
		
		connectionUrl = getProperty("database_url", props);
		
		if (connectionUrl == null || connectionUrl.length() == 0) {
			isValid = false;
			throw new InvalidProfileException("Database connection URL is undefined");
		}
		
		schema = getProperty("database_schema_name", props);
		
		LOG.info(String.format("Database schema is [%s]", schema));

		String key_attributes = getProperty("key_attributes", props);

		if (key_attributes == null || key_attributes.length() == 0) {
			isValid = false;
			throw new InvalidProfileException("Key attribute is undefined");
		}

		for (String attr : key_attributes.split(",")) {
			
			LOG.info(String.format("key attributes for domain [%s] are [%s] ", name, attr));
			
			String[] pAttr = attr.trim().split(":");

			if (pAttr.length == 3) {
				keyAttributes.add(pAttr[1].trim());
				attributesKeyCaseSensitiveMap.put(pAttr[1], (pAttr[0].equals("cs") ? true : false));
				attributesKeyColumnNameMap.put(pAttr[1], pAttr[2]);
			} else {
				LOG.error(String.format("Key attribute [%s] is invalid", attr));
			}
		}

		LOG.info(String.format("key attributes for domain [%s] are [%s] ", name, keyAttributes));

		String attrs2pull = getProperty("attributes_to_pull", props);

		if (attrs2pull == null || attrs2pull.length() == 0) {
			isValid = false;
			throw new InvalidProfileException("No user attribute to pull");
		}

		for (String attr : attrs2pull.split(",")) {
			String[] pAttr = attr.trim().split(":");

			if (pAttr.length == 3) {
				attributesToPull.add(pAttr[1].trim());
				attributesCardinalityMap.put(pAttr[1], (pAttr[0].equals("multi") ? true : false));
				attributesColumnNameMap.put(pAttr[1], pAttr[2]);
			} else {
				LOG.error(String.format("Attribute [%s] is invalid", attr));
			}
		}

		LOG.info(String.format("Attributes to pull for domain [%s] are [%s] ", name, attrs2pull));
		
		String linkAttrs2pull = getProperty("link_attributes_to_pull", props);

		if (linkAttrs2pull == null || linkAttrs2pull.length() == 0) {
			isValid = false;
			throw new InvalidProfileException("No  link user attribute to pull");
		}

		for (String attr : linkAttrs2pull.split(",")) {
			String[] pAttr = attr.trim().split(":");

			if (pAttr.length == 3) {
				linkAttributesToPull.add(pAttr[1].trim());
				linkAttributesCardinalityMap.put(pAttr[1], (pAttr[0].equals("multi") ? true : false));
				linkAttributesColumnNameMap.put(pAttr[1], pAttr[2]);
			} else {
				LOG.error(String.format("Attribute [%s] is invalid", attr));
			}
		}

		LOG.info(String.format("Link Attributes to pull for domain [%s] are [%s] ", name, linkAttrs2pull));
		
		isValid = true;

		LOG.info(String.format("Finished parsing profile for domain [%s]", this.name));
		
	}
	
	/**
	 * Retrieve Database UserName
	 * @return Database UserName
	 */
	public String getUserName() {
		return userName;
	}

	/**
	 * Setter method for database username
	 * @param userName Database username
	 */
	public void setUserName(String userName) {
		this.userName = userName;
	}
	
	/**
	 * Getter method for encrypted password
	 * @return Database password in encrypted form
	 */
	public String getEncryptedPassword() {
		return encryptedPassword;
	}
	
	/**
	 * Setter method for database password
	 * @param encryptedPassword Database password in encrypted form
	 */
	public void setEncryptedPassword(String encryptedPassword) {
		this.encryptedPassword = encryptedPassword;
	}

	/**
	 * Getter method for database connection URL
	 * @return Database JDBC connection URL
	 */
	public String getConnectionUrl() {
		return connectionUrl;
	}

	/**
	 * Setter method for database connection URL
	 * @param connectionUrl Database JDBC connection URL
	 */
	public void setConnectionUrl(String connectionUrl) {
		this.connectionUrl = connectionUrl;
	}
	
	/**
	 * Getter method for getting list of key attributes
	 * @return List of key attributes
	 */
	public List<String> getKeyAttributes() {
		return keyAttributes;
	}

	/**
	 * Setter method for list of key attributes
	 * @param keyAttributes List of key attributes
	 */
	public void setKeyAttributes(List<String> keyAttributes) {
		this.keyAttributes = keyAttributes;
	}

	/**
	 * Getter method for list of attributes to retrieve from database
	 * @return List of attributes
	 */
	public Set<String> getAttributesToPull() {
		return attributesToPull;
	}

	/**
	 * Setter method for list of attributes
	 * @param attributesToPull List of attributes to retrieve from database
	 */
	public void setAttributesToPull(Set<String> attributesToPull) {
		this.attributesToPull = attributesToPull;
	}

	/**
	 * Getter method for profile valid status
	 * @return Status of the profile
	 */
	public Boolean getIsValid() {
		return isValid;
	}

	/**
	 * Setter method for profile valid status
	 * @param isValid Status of the profile
	 */
	public void setIsValid(Boolean isValid) {
		this.isValid = isValid;
	}

	/**
	 * Getting property value from given properties list and the profile name e.g DB_1_DATA1
	 * @param name Property name or key for an attribute e.g DATA1
	 * @param props Properties list from properties file
	 * @return Value of the property
	 */
	private String getProperty(String name, Properties props) {
		return props.getProperty(this.name + "_" + name);
	}

	/**
	 * Getter method for cardinality for list of attributes
	 * @return Cardinality for the list of attributes
	 */
	public Map<String, Boolean> getAttributesCardinalityMap() {
		return attributesCardinalityMap;
	}

	/**
	 * Setter method for cardinality for the list of attributes
	 * @param attributesCardinalityMap Cardinality for the list of attributes
	 */
	public void setAttributesCardinalityMap(Map<String, Boolean> attributesCardinalityMap) {
		this.attributesCardinalityMap = attributesCardinalityMap;
	}
	/**
	 * Getter method for case sensitive value of list of attributes
	 * @return Case sensitive value for list of attributes
	 */
	public Map<String, Boolean> getAttributesKeyCaseSensitiveMap() {
		return attributesKeyCaseSensitiveMap;
	}

	/**
	 * Setter method for case sensitive of list of attributes
	 * @param attributesKeyCaseSensitiveMap Case sensitive value for list of attributes, true/false
	 */
	public void setAttributesKeyCaseSensitiveMap(Map<String, Boolean> attributesKeyCaseSensitiveMap) {
		this.attributesKeyCaseSensitiveMap = attributesKeyCaseSensitiveMap;
	}
	
	/**
	 * Determine if the key attribute is case sensitive from a pre populate map 
	 * @param key Attribute name
	 * @return true/false, true is the attribute is case sensitive.
	 */
	public Boolean isKeyCaseSensitive(String key) {
		return (attributesKeyCaseSensitiveMap.get(key));
	}
	
	/**
	 * Determine if the key attribute is multi-value from a pre populate map 
	 * @param attributeName Attribute name
	 * @return true/false, true is the attribute is multi-value
	 */
	public Boolean isMultiAttribute(String attributeName) {
		return (attributesCardinalityMap.get(attributeName));
	}

	
	/**
	 * Getter method for column name of the list of attributes
	 * @return Database column name of list of attributes
	 */
	public Map<String, String> getAttributesColumnNameMap() {
		return attributesColumnNameMap;
	}

	
	/**
	 * Setter method for column name of the list of attributes
	 * @param attributesColumnNameMap Database column name of list of attributes
	 */
	public void setAttributesColumnNameMap(Map<String, String> attributesColumnNameMap) {
		this.attributesColumnNameMap = attributesColumnNameMap;
	}

	public Map<String, String> getAttributesKeyColumnNameMap() {
		return attributesKeyColumnNameMap;
	}

	public void setAttributesKeyColumnNameMap(Map<String, String> attributesKeyColumnNameMap) {
		this.attributesKeyColumnNameMap = attributesKeyColumnNameMap;
	}
	
	public String getAttributeDBColumnName(String key) {
		return attributesColumnNameMap.get(key);
	}
	
	public String getKeyAtttributeDBColumnName(String sKey) {
		return attributesKeyColumnNameMap.get(sKey);
		
	}

	public Set<String> getLinkAttributesToPull() {
		return linkAttributesToPull;
	}

	public void setLinkAttributesToPull(Set<String> linkAttributesToPull) {
		this.linkAttributesToPull = linkAttributesToPull;
	}

	public Map<String, Boolean> getLinkAttributesCardinalityMap() {
		return linkAttributesCardinalityMap;
	}

	public void setLinkAttributesCardinalityMap(Map<String, Boolean> linkAttributesCardinalityMap) {
		this.linkAttributesCardinalityMap = linkAttributesCardinalityMap;
	}

	public Map<String, String> getLinkAttributesColumnNameMap() {
		return linkAttributesColumnNameMap;
	}

	public void setLinkAttributesColumnNameMap(Map<String, String> linkAttributesColumnNameMap) {
		this.linkAttributesColumnNameMap = linkAttributesColumnNameMap;
	}
	
	public String getLinkAttributeDBColumnName(String key) {
		return linkAttributesColumnNameMap.get(key);
	}
	
	public Boolean isLinkMultiAttribute(String attributeName) {
		return (linkAttributesCardinalityMap.get(attributeName));
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getDatabaseDriverName() {
		return databaseDriverName;
	}

	public void setDatabaseDriverName(String databaseDriverName) {
		this.databaseDriverName = databaseDriverName;
	}

	public String getDecryptedPassword() {
		return decryptedPassword;
	}

	public void setDecryptedPassword(String decryptedPassword) {
		this.decryptedPassword = decryptedPassword;
	}
	
	
	
}
