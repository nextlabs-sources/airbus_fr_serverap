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
import com.nextlabs.common.ResourceProfile;
import com.nextlabs.exception.InvalidProfileException;

/**
 * Setter and getter class for the resource profile
 * 
 * @author klee
 *
 */
public class ResourceDBProfile extends ResourceProfile {
	
	private String userName;
	private String encryptedPassword;
	private String decryptedPassword;
	private String connectionUrl;
	private String schema;
	private String databaseDriverName;
	private List<String> keyAttributes;
	private List<String> progKeyAttributes;
	private Set<String> attributesToPull;
	
	private Map<String, Boolean> attributesKeyCaseSensitiveMap;
	private Map<String, String> attributesKeyColumnNameMap;
	private Map<String, Boolean> progAttributesKeyCaseSensitiveMap;
	private Map<String, String> progAttributesKeyColumnNameMap;
	private Map<String, Boolean> attributesCardinalityMap;
	private Map<String, String> attributesColumnNameMap;
	private Set<String> progAttributesToPull;
	private Map<String, Boolean> progAttributesCardinalityMap;
	private Map<String, String> progAttributesColumnNameMap;
	private Set<String> tableProgITAttributesToPull;
	private Map<String, Boolean> tableProgITAttributesCardinalityMap;
	private Map<String, String> tableProgITAttributesColumnNameMap;
	private Set<String> tableMAEXAttributesToPull;
	private Map<String, Boolean> tableMAEXAttributesCardinalityMap;
	private Map<String, String> tableMAEXAttributesColumnNameMap;
	private Set<String> tableEXCC2TAttributesToPull;
	private Map<String, Boolean> tableEXCC2TAttributesCardinalityMap;
	private Map<String, String> tableEXCC2TAttributesColumnNameMap;
	private Set<String> tableProgHDAttributesToPull;
	private Map<String, Boolean> tableProgHDAttributesCardinalityMap;
	private Map<String, String> tableProgHDAttributesColumnNameMap;
	private Boolean isValid;
	
	private static final Log LOG = LogFactory.getLog(ResourceDBProfile.class);
	
	/**
	 * Constructor for the class 
	 * @param name Profile Name
	 */
	public ResourceDBProfile(String name) {
		super(name);
		keyAttributes = new ArrayList<String>();
		attributesToPull = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
		attributesCardinalityMap = new HashMap<String, Boolean>();
		attributesColumnNameMap = new HashMap<String, String>();
		attributesKeyCaseSensitiveMap = new HashMap<String, Boolean>();
		attributesKeyColumnNameMap = new HashMap<String, String>();
		tableProgHDAttributesToPull =  new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
		tableProgHDAttributesCardinalityMap = new HashMap<String, Boolean>();
		tableProgHDAttributesColumnNameMap = new HashMap<String, String>();
		
		tableMAEXAttributesToPull =  new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
		tableMAEXAttributesCardinalityMap = new HashMap<String, Boolean>();
		tableMAEXAttributesColumnNameMap = new HashMap<String, String>();
		
		tableProgITAttributesToPull =  new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
		tableProgITAttributesCardinalityMap = new HashMap<String, Boolean>();
		tableProgITAttributesColumnNameMap = new HashMap<String, String>();
		
		tableEXCC2TAttributesToPull =  new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
		tableEXCC2TAttributesCardinalityMap = new HashMap<String, Boolean>();
		tableEXCC2TAttributesColumnNameMap = new HashMap<String, String>();
		
		progAttributesToPull =  new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
		progAttributesCardinalityMap = new HashMap<String, Boolean>();
		progAttributesColumnNameMap = new HashMap<String, String>();
		
		progKeyAttributes = new ArrayList<String>();
		progAttributesKeyCaseSensitiveMap = new HashMap<String, Boolean>();
		progAttributesKeyColumnNameMap = new HashMap<String, String>();

	}
	
	/**
	 * Parsing the profile properties data
	 * @param props Properties list from properties file
	 * @param propFile Absolute file path for the properties file for e.g C:\PDP\nextlabs\jservice\config\ResourceReferentialPlugin.properties
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
			throw new InvalidProfileException("No resource attribute to pull");
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
		
		String tblProgHDAttrs2pull = getProperty("proghd_attributes_to_pull", props);

		if (tblProgHDAttrs2pull == null || tblProgHDAttrs2pull.length() == 0) {
			isValid = false;
			throw new InvalidProfileException("No PROGHD attribute to pull");
		}

		for (String attr : tblProgHDAttrs2pull.split(",")) {
			String[] pAttr = attr.trim().split(":");

			if (pAttr.length == 3) {
				tableProgHDAttributesToPull.add(pAttr[1].trim());
				tableProgHDAttributesCardinalityMap.put(pAttr[1], (pAttr[0].equals("multi") ? true : false));
				tableProgHDAttributesColumnNameMap.put(pAttr[1], pAttr[2]);
			} else {
				LOG.error(String.format("Attribute [%s] is invalid", attr));
			}
		}

		LOG.info(String.format("PROGHD Attributes to pull for domain [%s] are [%s] ", name, tblProgHDAttrs2pull));
		
		String tblProgITAttrs2pull = getProperty("progit_attributes_to_pull", props);

		if (tblProgITAttrs2pull == null || tblProgITAttrs2pull.length() == 0) {
			isValid = false;
			throw new InvalidProfileException("No PROGIT attribute to pull");
		}

		for (String attr : tblProgITAttrs2pull.split(",")) {
			String[] pAttr = attr.trim().split(":");

			if (pAttr.length == 3) {
				tableProgITAttributesToPull.add(pAttr[1].trim());
				tableProgITAttributesCardinalityMap.put(pAttr[1], (pAttr[0].equals("multi") ? true : false));
				tableProgITAttributesColumnNameMap.put(pAttr[1], pAttr[2]);
			} else {
				LOG.error(String.format("Attribute [%s] is invalid", attr));
			}
		}

		LOG.info(String.format("PROGIT Attributes to pull for domain [%s] are [%s] ", name, tblProgITAttrs2pull));
		
		String tblMAEXAttrs2pull = getProperty("maex_attributes_to_pull", props);

		if (tblMAEXAttrs2pull == null || tblMAEXAttrs2pull.length() == 0) {
			isValid = false;
			throw new InvalidProfileException("No MAEX attribute to pull");
		}

		for (String attr : tblMAEXAttrs2pull.split(",")) {
			String[] pAttr = attr.trim().split(":");

			if (pAttr.length == 3) {
				tableMAEXAttributesToPull.add(pAttr[1].trim());
				tableMAEXAttributesCardinalityMap.put(pAttr[1], (pAttr[0].equals("multi") ? true : false));
				tableMAEXAttributesColumnNameMap.put(pAttr[1], pAttr[2]);
			} else {
				LOG.error(String.format("Attribute [%s] is invalid", attr));
			}
		}

		LOG.info(String.format("MAEX Attributes to pull for domain [%s] are [%s] ", name, tblProgHDAttrs2pull));
		
		
		String tblEXCC2TAttrs2pull = getProperty("excc2t_attributes_to_pull", props);

		if (tblEXCC2TAttrs2pull == null || tblEXCC2TAttrs2pull.length() == 0) {
			isValid = false;
			throw new InvalidProfileException("No EXCC2T attribute to pull");
		}

		for (String attr : tblEXCC2TAttrs2pull.split(",")) {
			String[] pAttr = attr.trim().split(":");

			if (pAttr.length == 3) {
				tableEXCC2TAttributesToPull.add(pAttr[1].trim());
				tableEXCC2TAttributesCardinalityMap.put(pAttr[1], (pAttr[0].equals("multi") ? true : false));
				tableEXCC2TAttributesColumnNameMap.put(pAttr[1], pAttr[2]);
			} else {
				LOG.error(String.format("Attribute [%s] is invalid", attr));
			}
		}

		LOG.info(String.format("EXCC2T Attributes to pull for domain [%s] are [%s] ", name, tblProgHDAttrs2pull));
		
		String tblProgAttrs2pull = getProperty("prog_attributes_to_pull", props);

		if (tblProgAttrs2pull == null || tblProgAttrs2pull.length() == 0) {
			isValid = false;
			throw new InvalidProfileException("No PROG attribute to pull");
		}

		for (String attr : tblProgAttrs2pull.split(",")) {
			String[] pAttr = attr.trim().split(":");

			if (pAttr.length == 3) {
				progAttributesToPull.add(pAttr[1].trim());
				progAttributesCardinalityMap.put(pAttr[1], (pAttr[0].equals("multi") ? true : false));
				progAttributesColumnNameMap.put(pAttr[1], pAttr[2]);
			} else {
				LOG.error(String.format("Attribute [%s] is invalid", attr));
			}
		}

		LOG.info(String.format("PROG Attributes to pull for domain [%s] are [%s] ", name, tblProgHDAttrs2pull));
		
		
		String prog_key_attributes = getProperty("prog_key_attributes", props);

		if (prog_key_attributes == null || prog_key_attributes.length() == 0) {
			isValid = false;
			throw new InvalidProfileException("Key attribute is undefined");
		}

		for (String attr : prog_key_attributes.split(",")) {
			
			LOG.info(String.format("PROG key attributes for domain [%s] are [%s] ", name, attr));
			
			String[] pAttr = attr.trim().split(":");

			if (pAttr.length == 3) {
				progKeyAttributes.add(pAttr[1].trim());
				progAttributesKeyCaseSensitiveMap.put(pAttr[1], (pAttr[0].equals("cs") ? true : false));
				progAttributesKeyColumnNameMap.put(pAttr[1], pAttr[2]);
			} else {
				LOG.error(String.format("Key attribute [%s] is invalid", attr));
			}
		}

		LOG.info(String.format("Prog attributes for domain [%s] are [%s] ", name, keyAttributes));
		
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
	
	public Boolean isKeyCaseSensitive(String key) {
		return (attributesKeyCaseSensitiveMap.get(key));
	}
	
	public Boolean isProgKeyCaseSensitive(String key) {
		return (progAttributesKeyCaseSensitiveMap.get(key));
	}
	
	public Boolean isMultiAttribute(String attributeName) {
		return (attributesCardinalityMap.get(attributeName));
	}

	public Map<String, String> getAttributesColumnNameMap() {
		return attributesColumnNameMap;
	}

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

	public Set<String> getTableProgHDAttributesToPull() {
		return tableProgHDAttributesToPull;
	}

	public void setTableProgHDAttributesToPull(Set<String> linkAttributesToPull) {
		this.tableProgHDAttributesToPull = linkAttributesToPull;
	}

	public Map<String, Boolean> getTableProgHDAttributesCardinalityMap() {
		return tableProgHDAttributesCardinalityMap;
	}

	public void setTableProgHDAttributesCardinalityMap(Map<String, Boolean> linkAttributesCardinalityMap) {
		this.tableProgHDAttributesCardinalityMap = linkAttributesCardinalityMap;
	}

	public Map<String, String> getTableProgHDAttributesColumnNameMap() {
		return tableProgHDAttributesColumnNameMap;
	}

	public void setTableProgHDAttributesColumnNameMap(Map<String, String> linkAttributesColumnNameMap) {
		this.tableProgHDAttributesColumnNameMap = linkAttributesColumnNameMap;
	}
	
	public String getTableProgHDAttributeDBColumnName(String key) {
		return tableProgHDAttributesColumnNameMap.get(key);
	}
	
	public Boolean isTableProgHDMultiAttribute(String attributeName) {
		return (tableProgHDAttributesCardinalityMap.get(attributeName));
	}
	
	public Boolean isTableProgITMultiAttribute(String attributeName) {
		return (tableProgITAttributesCardinalityMap.get(attributeName));
	}
	
	public Boolean isTableMAEXMultiAttribute(String attributeName) {
		return (tableMAEXAttributesCardinalityMap.get(attributeName));
	}
	
	public Boolean isTableEXCC2TMultiAttribute(String attributeName) {
		return (tableEXCC2TAttributesCardinalityMap.get(attributeName));
	}
	
	public Boolean isProgMultiAttribute(String attributeName) {
		return (progAttributesCardinalityMap.get(attributeName));
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

	public List<String> getProgKeyAttributes() {
		return progKeyAttributes;
	}

	public void setProgKeyAttributes(List<String> progKeyAttributes) {
		this.progKeyAttributes = progKeyAttributes;
	}

	public Set<String> getProgAttributesToPull() {
		return progAttributesToPull;
	}

	public void setProgAttributesToPull(Set<String> progAttributesToPull) {
		this.progAttributesToPull = progAttributesToPull;
	}

	public Map<String, Boolean> getProgAttributesKeyCaseSensitiveMap() {
		return progAttributesKeyCaseSensitiveMap;
	}

	public void setProgAttributesKeyCaseSensitiveMap(Map<String, Boolean> progAttributesKeyCaseSensitiveMap) {
		this.progAttributesKeyCaseSensitiveMap = progAttributesKeyCaseSensitiveMap;
	}

	public Map<String, String> getProgAttributesKeyColumnNameMap() {
		return progAttributesKeyColumnNameMap;
	}

	public void setProgAttributesKeyColumnNameMap(Map<String, String> progAttributesKeyColumnNameMap) {
		this.progAttributesKeyColumnNameMap = progAttributesKeyColumnNameMap;
	}
	
	public String getProgKeyAtttributeDBColumnName(String sKey) {
		return progAttributesKeyColumnNameMap.get(sKey);
		
	}

	public Map<String, Boolean> getProgAttributesCardinalityMap() {
		return progAttributesCardinalityMap;
	}

	public void setProgAttributesCardinalityMap(Map<String, Boolean> progAttributesCardinalityMap) {
		this.progAttributesCardinalityMap = progAttributesCardinalityMap;
	}

	public Map<String, String> getProgAttributesColumnNameMap() {
		return progAttributesColumnNameMap;
	}

	public void setProgAttributesColumnNameMap(Map<String, String> progAttributesColumnNameMap) {
		this.progAttributesColumnNameMap = progAttributesColumnNameMap;
	}
	
	public String getProgAttributeDBColumnName(String key) {
		return progAttributesColumnNameMap.get(key);
	}

	public Set<String> getTableProgITAttributesToPull() {
		return tableProgITAttributesToPull;
	}

	public void setTableProgITAttributesToPull(Set<String> tableProgITAttributesToPull) {
		this.tableProgITAttributesToPull = tableProgITAttributesToPull;
	}

	public Map<String, Boolean> getTableProgITAttributesCardinalityMap() {
		return tableProgITAttributesCardinalityMap;
	}

	public void setTableProgITAttributesCardinalityMap(Map<String, Boolean> tableProgITAttributesCardinalityMap) {
		this.tableProgITAttributesCardinalityMap = tableProgITAttributesCardinalityMap;
	}

	public Map<String, String> getTableProgITAttributesColumnNameMap() {
		return tableProgITAttributesColumnNameMap;
	}

	public void setTableProgITAttributesColumnNameMap(Map<String, String> tableProgITAttributesColumnNameMap) {
		this.tableProgITAttributesColumnNameMap = tableProgITAttributesColumnNameMap;
	}
	
	public String getTableProgITAttributeDBColumnName(String key) {
		return tableProgITAttributesColumnNameMap.get(key);
	}

	public Set<String> getTableMAEXAttributesToPull() {
		return tableMAEXAttributesToPull;
	}

	public void setTableMAEXAttributesToPull(Set<String> tableMAEXAttributesToPull) {
		this.tableMAEXAttributesToPull = tableMAEXAttributesToPull;
	}

	public Map<String, Boolean> getTableMAEXAttributesCardinalityMap() {
		return tableMAEXAttributesCardinalityMap;
	}

	public void setTableMAEXAttributesCardinalityMap(Map<String, Boolean> tableMAEXAttributesCardinalityMap) {
		this.tableMAEXAttributesCardinalityMap = tableMAEXAttributesCardinalityMap;
	}

	public Map<String, String> getTableMAEXAttributesColumnNameMap() {
		return tableMAEXAttributesColumnNameMap;
	}

	public void setTableMAEXAttributesColumnNameMap(Map<String, String> tableMAEXAttributesColumnNameMap) {
		this.tableMAEXAttributesColumnNameMap = tableMAEXAttributesColumnNameMap;
	}
	
	public String getTableMAEXAttributeDBColumnName(String key) {
		return tableMAEXAttributesColumnNameMap.get(key);
	}

	public Set<String> getTableEXCC2TAttributesToPull() {
		return tableEXCC2TAttributesToPull;
	}

	public void setTableEXCC2TAttributesToPull(Set<String> tableEXCC2TAttributesToPull) {
		this.tableEXCC2TAttributesToPull = tableEXCC2TAttributesToPull;
	}

	public Map<String, Boolean> getTableEXCC2TAttributesCardinalityMap() {
		return tableEXCC2TAttributesCardinalityMap;
	}

	public void setTableEXCC2TAttributesCardinalityMap(Map<String, Boolean> tableEXCC2TAttributesCardinalityMap) {
		this.tableEXCC2TAttributesCardinalityMap = tableEXCC2TAttributesCardinalityMap;
	}

	public Map<String, String> getTableEXCC2TAttributesColumnNameMap() {
		return tableEXCC2TAttributesColumnNameMap;
	}

	public void setTableEXCC2TAttributesColumnNameMap(Map<String, String> tableEXCC2TAttributesColumnNameMap) {
		this.tableEXCC2TAttributesColumnNameMap = tableEXCC2TAttributesColumnNameMap;
	}
	
	public String getTableEXCC2TAttributeDBColumnName(String key) {
		return tableEXCC2TAttributesColumnNameMap.get(key);
	}
	
	public String getDecryptedPassword() {
		return decryptedPassword;
	}

	public void setDecryptedPassword(String decryptedPassword) {
		this.decryptedPassword = decryptedPassword;
	}
	
}
