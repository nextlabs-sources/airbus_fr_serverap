package com.nextlabs.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.bluejungle.framework.expressions.EvalValue;
import com.bluejungle.framework.expressions.Multivalue;
import com.nextlabs.cache.ResourceCacheEngine;
import com.nextlabs.common.Provider;
import com.nextlabs.common.ResourceObject;
import com.nextlabs.common.UserObject;
import com.nextlabs.common.Util;
import com.nextlabs.exception.InvalidProfileException;
	
/**
 * Class the implement the detail for fetching data from resource database
 * 
 * @author klee
 *
 */
public class DBResouceProvider implements Provider {	
	private static final Log LOG = LogFactory.getLog(DBResouceProvider.class);
	private static DBResouceProvider provider;
	private Map<String, ResourceDBProfile> profiles;
	private ResourceDBProfile singleProfile;
	private Map<String, List<String>> resAttributeToProfileMap;
	private boolean isSingleProfile;
	private Map<String, String> idToObjectTypeMap;
	private final String RESOURCE_TYPE = "res";
	private int numberOfRetries;
	private int intervalBetweenRetries;
	private Boolean isRefreshing;
	private static Properties commonProp;
	private static HikariCPDataSource ds;
	private static String ITEM_SQL_QUERY = "SELECT [ID], [OBID], ## FROM DARIT";
	private static String ITEM_SQL_QUERY_WITH_CONDITION = "SELECT [ID], [OBID], ## FROM DARIT WHERE OBID = ?";
	private static final StringBuffer PROGRAM_SQL_QUERY = new StringBuffer("SELECT [ID_HD]").append(",[ID_INDCT]")
			.append(",[ID_MILCT]").append(",[ID_EXCC1T]").append(" FROM PROGIT");
	private static final StringBuffer PROGRAM_SQL_QUERY_WITH_CONDITION = new StringBuffer("SELECT [ID_HD]").append(",[ID_INDCT]")
			.append(",[ID_MILCT]").append(",[ID_EXCC1T]").append(" FROM PROGIT").append(" WHERE ID_HD = ?");
	private static final String PROGHD_SQL_QUERY = new StringBuffer()
			.append("SELECT A.OBID, C.PROGK, C.PROVS FROM DARIT A LEFT JOIN DAR_PROGT B")
			.append("	ON A.ID = B.ID_DARIT LEFT JOIN PROGHD C").append("	ON B.ID_HD = C.ID_HD WHERE A.OBID = ?")
			.toString();
	private static final StringBuffer PROGIT_SQL_QUERY = new StringBuffer()
			.append("SELECT A.OBID, C.ID_INDCT, C.ID_MILCT, C.ID_EXCC1T FROM DARIT A LEFT JOIN DAR_PROGT B")
			.append("	ON A.ID = B.ID_DARIT LEFT JOIN PROGIT C").append("	ON B.ID_HD = C.ID_HD WHERE A.OBID = ?");
	private static final String EXCC2T_SQL_QUERY = "SELECT ID_DARIT, EXCC2 FROM EXCC2T WHERE ID_DARIT = ?";
	private static final String MAEX_SQL_QUERY = "SELECT MATNR, EMBGR FROM MAEX WHERE MATNR = ?";

	/**
	 * Constructor for DBResourceProvider
	 */
	public DBResouceProvider() {
		resAttributeToProfileMap = new HashMap<String, List<String>>();
		idToObjectTypeMap = new ConcurrentHashMap<String, String>();
		isRefreshing = false;
	}

	
	/**
	 * Singleton control of the DBResourceProvider
	 * @return Object of DBResouceProvider
	 */
	public static DBResouceProvider getInstance() {
		if (provider == null) {
			provider = new DBResouceProvider();
		}

		return provider;
	}
	
	/**
	 * Construct the column for query
	 * @param profile ResourceDBProfile which contain all the profile information
	 * @return String contain the column user for query for e.g "[excc2], [excc1]"
	 */
	public String getQueryColumn(ResourceDBProfile profile) {
		
		Map<String, String> columnMap = profile.getAttributesColumnNameMap();
		StringBuffer sColumnBuffer = new StringBuffer();
		
		for (String value : columnMap.values()) {
		    
			sColumnBuffer.append("[").append(value).append("], ");
		}
		
		//To remove the last ","
		if (sColumnBuffer.length()>0) {
			sColumnBuffer.setLength(sColumnBuffer.length() - 2);
		}
		
		return sColumnBuffer.toString();
	}
	
	/**
	 * Retrieve profile from the provider
	 * @return ResourceDBProfile object
	 */
	public ResourceDBProfile getProfile() {	
		
		if(isSingleProfile) {
			return singleProfile;
		}
		else {
			//TODO return matching profile
			return null;
		}
		
	}
	
	
	/**
	 * Initialize of the Database connection pool
	 * @param profile ResourceDBProfile
	 */
	public void initDBConnetionPool(ResourceDBProfile profile) {
		
		ds = new HikariCPDataSource(profile);
		
		String sQueryColumn = getQueryColumn(profile);
		
		ITEM_SQL_QUERY = ITEM_SQL_QUERY.replaceAll("##", sQueryColumn);
		
		LOG.debug("Item Query is " + ITEM_SQL_QUERY);
		
		ITEM_SQL_QUERY_WITH_CONDITION = ITEM_SQL_QUERY_WITH_CONDITION.replaceAll("##", sQueryColumn);
				
	}
	
	/**
	 * Retrieve connection from DB connection pool
	 * @return A DB connection from the pool, null if the pool is not initialized.
	 * @throws SQLException Database SQL Exception
	 */
	public synchronized Connection getConnectionFromPool() throws SQLException {
		
		if (ds!=null)
			return ds.getConnection();
		
		LOG.error("Datasource is null and not initialized");
		
		return null;
	}
		
	/* (non-Javadoc)
	 * @see com.nextlabs.common.Provider#getItemObject(java.lang.String, java.lang.String)
	 */
	@Override
	public ResourceObject getItemObject(String id, String attributeToSearch) throws SQLException, NamingException {

		ResourceObject object = null;

		if (idToObjectTypeMap.get(id) == null
				|| (idToObjectTypeMap.get(id) != null && idToObjectTypeMap.get(id).equals(RESOURCE_TYPE))) {

			if (isSingleProfile) {
				
				object = queryForItem(singleProfile, id);

			} else {

				List<String> profilesToLook = resAttributeToProfileMap.get(attributeToSearch.toLowerCase());
				if (profilesToLook == null) {
					LOG.error(String.format("Attribute [%s] isn't provided by any domain", attributeToSearch));
					return null;
				}

				for (String profileName : profilesToLook) {
					ResourceDBProfile dbProfile = profiles.get(profileName);

					LOG.info(String.format("Attribute [%s] should be found in domain [%s]. Attemp to query...",
							attributeToSearch, dbProfile.getName()));

					object = queryForItem(dbProfile, id);

					if (object != null) {
						break;
					}
				}
			}
		} else {
			LOG.error(String.format("Type cannot be found for ID [%s]", id));
		}

		if (object == null) {
			LOG.error(String.format("Object [%s] cannot be queried from Resources DB", id));
		}

		return object;
	}

	/**
	 * Query for a resource with resource ID and store in into the cache store
	 * @param dbProfile DBProfile
	 * @param resId Resource ID
	 * @return ResourceObject which matched the given resource ID
	 * @throws NamingException
	 * @throws SQLException
	 */
	private ResourceObject queryForItem(ResourceDBProfile dbProfile, String resId)
			throws NamingException, SQLException {

		ResourceObject resObj = null;
		long startTime = System.currentTimeMillis();

		try (Connection con = getConnectionFromPool();
				PreparedStatement pst = con.prepareStatement(ITEM_SQL_QUERY_WITH_CONDITION,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);) {
			pst.setString(1, resId);
			try (ResultSet rs = pst.executeQuery()) {
				while (rs.next()) {

					resObj = produceItem(rs, dbProfile);

					// write user to cache
					ResourceCacheEngine.getInstance().writeObjectToItemCache(resObj);

					// update identifier map
					for (String key : dbProfile.getKeyAttributes()) {
						if (resObj.getAttribute(key.toLowerCase()) != null
								&& resObj.getAttribute(key.toLowerCase()).getValue() != null) {
							ResourceCacheEngine.getInstance().addItemIdentifier(
									(String) resObj.getAttribute(key.toLowerCase()).getValue(), resObj.getId());
							idToObjectTypeMap.put((String) resObj.getAttribute(key.toLowerCase()).getValue(),
									RESOURCE_TYPE);
						}
					}
				}
			}
			con.close();
		}

		long endTime = System.currentTimeMillis();

		LOG.info(String.format("Query for Item [%s] took %dms", resId, (endTime - startTime)));

		return resObj;
	}
	

	
	/* (non-Javadoc)
	 * @see com.nextlabs.common.Provider#refreshCache()
	 */
	@Override
	public synchronized void refreshCache() {
				
		if (!commonProp.getProperty("refresh","false").equalsIgnoreCase("false")) {

			isRefreshing = true;

			long startTime = System.currentTimeMillis();

			int count = 0;

			while (true) {
				try {
					if (isSingleProfile) {
						refreshProfile(singleProfile);
					} else {
						for (ResourceDBProfile dbProfile : profiles.values()) {
							refreshProfile(dbProfile);
						}
					}
					break;

				} catch (Exception e) {

					LOG.error("Cache refresh encountered an exception.", e);

					if (count++ == numberOfRetries) {
						LOG.error(String.format("Attempted [%d] retries without success.", numberOfRetries));
						break;
					} else {
						LOG.debug(String.format("Retrying refreshing cache in [%d] seconds..", intervalBetweenRetries));
						try {
							Thread.sleep(intervalBetweenRetries * 1000);
						} catch (InterruptedException ie) {
							// IGNORE
						}
					}
				}
			}

			long endTime = System.currentTimeMillis();

			isRefreshing = false;

			LOG.info("Cache refresh completed");
			LOG.info("Time Taken: " + Long.toString((endTime - startTime)) + "ms");

		}
		else{
			LOG.info("Skip reload cache since the refresh is false");
		}
	}

	/**
	 * Reload the cache store which contain Item and Program
	 * @param dbProfile ResourceDBProfile
	 * @throws NamingException
	 */
	private void refreshProfile(ResourceDBProfile dbProfile) throws NamingException {
		LOG.info(String.format("Started refreshing domain [%s]", dbProfile.getName()));

		if (!dbProfile.getIsValid()) {
			LOG.error(String.format("Profile [%s] is invalid. Skip refreshing.", dbProfile.getName()));
			return;
		}

		try (Connection conn = getConnectionFromPool()) {
			refreshItem(dbProfile, conn);
			refreshProgram(dbProfile, conn);
		} catch (SQLException e) {
			e.printStackTrace();
			LOG.error(e);
		}
	}

	/**
	 * Reload the Item object in cache store
	 * @param dbProfile ResourceDBProfile
	 * @param conn DB Connection
	 * @throws NamingException
	 * @throws SQLException
	 */
	private void refreshItem(ResourceDBProfile dbProfile, Connection conn) throws NamingException, SQLException {

		ResourceObject item = null;

		try (Connection con = getConnectionFromPool();
				PreparedStatement pst = con.prepareStatement(ITEM_SQL_QUERY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				ResultSet rs = pst.executeQuery();) {
			while (rs.next()) {
				item = produceItem(rs, dbProfile);
				
				// write item to cache
				ResourceCacheEngine.getInstance().writeObjectToItemCache(item);
				
				// update identifier map
				for (String key : dbProfile.getKeyAttributes()) {
					if (item.getAttribute(key.toLowerCase()) != null
							&& item.getAttribute(key.toLowerCase()).getValue() != null) {
						ResourceCacheEngine.getInstance().addItemIdentifier((String) item.getAttribute(key.toLowerCase()).getValue(), item.getId());
						idToObjectTypeMap.put((String) item.getAttribute(key.toLowerCase()).getValue(), RESOURCE_TYPE);
					}
				}
			}
		}
	}
	
	/**
	 * Reload the Program object in cache store
	 * @param dbProfile ResourceDBProfile
	 * @param conn Database Connection
	 * @throws NamingException
	 * @throws SQLException
	 */
	private void refreshProgram(ResourceDBProfile dbProfile, Connection conn) throws NamingException, SQLException {

		ResourceObject prog = null;
		
		try (Connection con = getConnectionFromPool();
				PreparedStatement pst = con.prepareStatement(PROGRAM_SQL_QUERY.toString(), ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				ResultSet rs = pst.executeQuery();) {
			while (rs.next()) {
				prog = produceProgram(rs, dbProfile);
				
				// write program to cache
				ResourceCacheEngine.getInstance().writeObjectToProgramCache(prog);
				
				// update identifier map
				for (String key : dbProfile.getProgKeyAttributes()) {
					if (prog.getAttribute(key.toLowerCase()) != null
							&& prog.getAttribute(key.toLowerCase()).getValue() != null) {
						ResourceCacheEngine.getInstance().addProgIdentifier((String) prog.getAttribute(key.toLowerCase()).getValue(), prog.getId());
						idToObjectTypeMap.put((String) prog.getAttribute(key.toLowerCase()).getValue(), RESOURCE_TYPE);
					}
				}
			}
		}
	}

	/**
	 * Produce a ResourceObject from multiple table
	 * @param rs Resultset contain the main entry
	 * @param profile  ResourceDBProfile for the profile
	 * @return A ResourceObject 
	 * @throws NamingException
	 * @throws SQLException
	 */
	private ResourceObject produceItem(ResultSet rs, ResourceDBProfile profile) throws NamingException, SQLException {

		ResourceObject resObj = null;

		String[] ids = new String[profile.getKeyAttributes().size()];

		for (int i = 0; i < profile.getKeyAttributes().size(); i++) {

			String keyAttributeName = profile.getKeyAttributes().get(i);

			String temp = rs.getString(profile.getKeyAtttributeDBColumnName(profile.getKeyAttributes().get(i)));
			if (temp == null) {
				ids[i] = "UNDEFINED";
			} else {
					ids[i] = temp;

					if (!profile.isKeyCaseSensitive(keyAttributeName)) {
						ids[i] = ids[i].toLowerCase();
					}
			}
		}

		String resId = Util.makeCombinedID(profile.getName(), ids);

		resObj = new ResourceObject(profile.getName(), resId, RESOURCE_TYPE);

		Set<String> sValues = new TreeSet<>();

		// process attributes to pull
		for (String attributeName: profile.getAttributesToPull()) {

			String temp = rs.getString(profile.getAttributeDBColumnName(attributeName));

			if (!profile.isMultiAttribute(attributeName)) {
				resObj.addAttribute(attributeName.toLowerCase(),(temp == null) ? EvalValue.NULL : EvalValue.build(temp));
			} else {

				if (temp == null && rs.isLast()) {
					resObj.addAttribute(attributeName.toLowerCase(), EvalValue.build(Multivalue.EMPTY));
				} else {
					
					int iCurrentRow = rs.getRow();

					sValues.clear();
					
					temp = rs.getString(profile.getAttributeDBColumnName(attributeName));
					
					if (temp != null && temp.length() > 0) {
						sValues.add(temp.trim());
					}

					while (rs.next()) {
						temp = rs.getString(profile.getAttributeDBColumnName(attributeName));
						if (temp != null && temp.length() > 0) {
							sValues.add(temp.trim());
						}
					}
					
					rs.absolute(iCurrentRow);
					
					if (sValues.size() > 0) {
						resObj.addAttribute(attributeName.toLowerCase(), EvalValue.build(Multivalue.create(sValues)));
					} else {
						resObj.addAttribute(attributeName.toLowerCase(), EvalValue.build(Multivalue.EMPTY));
					}
				}
			}

		}
		
		//Append attribute from another table
		produceProgHDAttribute(profile, rs.getString("OBID"), resObj);
		
		produceProgITAttribute(profile, rs.getString("OBID"), resObj);
		
		produceEXCC2TAttribute(profile, rs.getString("ID"), resObj);
		
		produceMAEXAttribute(profile, rs.getString("OBID"), resObj);

		// process key attributes
		for (int i = 0; i < profile.getKeyAttributes().size(); i++) {
			String attributeName = profile.getKeyAttributes().get(i);

			String temp = rs.getString(profile.getKeyAtttributeDBColumnName(attributeName));
			
			if (temp != null) {
				
				resObj.addAttribute(attributeName.toLowerCase(),
						EvalValue.build((profile.isKeyCaseSensitive(attributeName)) ? temp.toString()
								: temp.toString().toLowerCase()));
			}
		}
				
		return resObj;
	}
	
	/**
	 * Query the attribute value from ProgHD table
	 * @param profile ResourceDBProfile profile
	 * @param id Resource ID
	 * @param resObj ResourceObject for storing attribute value
	 * @throws SQLException
	 */
	private void produceProgHDAttribute(ResourceDBProfile profile, String id, ResourceObject resObj)
			throws SQLException {

		try (Connection con = getConnectionFromPool();
				PreparedStatement pst = con.prepareStatement(PROGHD_SQL_QUERY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);) {
			pst.setString(1, id);
			try (ResultSet rs = pst.executeQuery()) {
				if (rs.next()) {

					Set<String> sValues = new TreeSet<>();

					// process attributes to pull
					for (String attributeName : profile.getTableProgHDAttributesToPull()) {

						String temp = rs.getString(profile.getTableProgHDAttributeDBColumnName(attributeName));

						if (!profile.isTableProgHDMultiAttribute(attributeName)) {
							resObj.addAttribute(attributeName.toLowerCase(),
									(temp == null) ? EvalValue.NULL : EvalValue.build(temp));
						} else {

							if (temp == null && rs.isLast()) {
								resObj.addAttribute(attributeName.toLowerCase(), EvalValue.build(Multivalue.EMPTY));
							} else {

								int iCurrentRow = rs.getRow();

								sValues.clear();

								temp = rs.getString(profile.getTableProgHDAttributeDBColumnName(attributeName));

								if (temp != null && temp.length() > 0) {
									sValues.add(temp.trim());
								}

								while (rs.next()) {
									temp = rs.getString(profile.getTableProgHDAttributeDBColumnName(attributeName));
									if (temp != null && temp.length() > 0) {
										sValues.add(temp.trim());
									}
								}

								rs.absolute(iCurrentRow);
								
								if (sValues.size() > 0) {
									resObj.addAttribute(attributeName.toLowerCase(),
											EvalValue.build(Multivalue.create(sValues)));
								} else {
									resObj.addAttribute(attributeName.toLowerCase(), EvalValue.build(Multivalue.EMPTY));
								}
							}
						}

					}

				}
			}

		}

	}
	
	
	/**
	 * Query the attribute value from ProgIT table
	 * @param profile ResourceDBProfile profile
	 * @param id Resource ID
	 * @param resObj ResourceObject for storing attribute value
	 * @throws SQLException
	 */
	private void produceProgITAttribute(ResourceDBProfile profile, String id, ResourceObject resObj)
			throws SQLException {

		try (Connection con = getConnectionFromPool();
				PreparedStatement pst = con.prepareStatement(PROGIT_SQL_QUERY.toString(),ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);) {
			pst.setString(1, id);
			try (ResultSet rs = pst.executeQuery()) {
				if (rs.next()) {

					Set<String> sValues = new TreeSet<>();

					// process attributes to pull
					for (String attributeName : profile.getTableProgITAttributesToPull()) {

						String temp = rs.getString(profile.getTableProgITAttributeDBColumnName(attributeName));

						if (!profile.isTableProgITMultiAttribute(attributeName)) {
							resObj.addAttribute(attributeName.toLowerCase(),
									(temp == null) ? EvalValue.NULL : EvalValue.build(temp));
						} else {

							if (temp == null && rs.isLast()) {
								resObj.addAttribute(attributeName.toLowerCase(), EvalValue.build(Multivalue.EMPTY));
							} else {

								int iCurrentRow = rs.getRow();

								sValues.clear();

								temp = rs.getString(profile.getTableProgITAttributeDBColumnName(attributeName));

								if (temp != null && temp.length() > 0) {
									sValues.add(temp.trim());
								}

								while (rs.next()) {
									temp = rs.getString(profile.getTableProgITAttributeDBColumnName(attributeName));
									if (temp != null && temp.length() > 0) {
										sValues.add(temp.trim());
									}
								}

								rs.absolute(iCurrentRow);

								if (sValues.size() > 0) {
									resObj.addAttribute(attributeName.toLowerCase(),
											EvalValue.build(Multivalue.create(sValues)));
								} else {
									resObj.addAttribute(attributeName.toLowerCase(), EvalValue.build(Multivalue.EMPTY));
								}
							}
						}

					}

				}
			}

		}

	}
	
	
	/**
	 * Query the attribute value from MAEX table
	 * @param profile ResourceDBProfile profile
	 * @param id Resource ID
	 * @param resObj ResourceObject for storing attribute value
	 * @throws SQLException
	 */
	private void produceMAEXAttribute(ResourceDBProfile profile, String id, ResourceObject resObj) throws SQLException {

		try (Connection con = getConnectionFromPool();
				PreparedStatement pst = con.prepareStatement(MAEX_SQL_QUERY, ResultSet.TYPE_SCROLL_INSENSITIVE,
						ResultSet.CONCUR_READ_ONLY);) {
			pst.setString(1, id);
			try (ResultSet rs = pst.executeQuery()) {
				if (rs.next()) {

					Set<String> sValues = new TreeSet<>();

					// process attributes to pull
					for (String attributeName : profile.getTableMAEXAttributesToPull()) {

						String temp = rs.getString(profile.getTableMAEXAttributeDBColumnName(attributeName));

						if (!profile.isTableMAEXMultiAttribute(attributeName)) {
							resObj.addAttribute(attributeName.toLowerCase(),
									(temp == null) ? EvalValue.NULL : EvalValue.build(temp));
						} else {

							if (temp == null && rs.isLast()) {
								resObj.addAttribute(attributeName.toLowerCase(), EvalValue.build(Multivalue.EMPTY));
							} else {

								int iCurrentRow = rs.getRow();

								sValues.clear();

								temp = rs.getString(profile.getTableMAEXAttributeDBColumnName(attributeName));

								if (temp != null && temp.length() > 0) {
									sValues.add(temp.trim());
								}

								while (rs.next()) {
									temp = rs.getString(profile.getTableMAEXAttributeDBColumnName(attributeName));
									if (temp != null && temp.length() > 0) {
										sValues.add(temp.trim());
									}
								}

								rs.absolute(iCurrentRow);

								if (sValues.size() > 0) {
									resObj.addAttribute(attributeName.toLowerCase(),
											EvalValue.build(Multivalue.create(sValues)));
								} else {
									resObj.addAttribute(attributeName.toLowerCase(), EvalValue.build(Multivalue.EMPTY));
								}
							}
						}

					}

				}
			}

		}

	}
	
	
	/**
	 * Query the attribute value from EXCC2T table
	 * @param profile ResourceDBProfile profile
	 * @param id Resource ID
	 * @param resObj ResourceObject for storing attribute value
	 * @throws SQLException
	 */
	private void produceEXCC2TAttribute(ResourceDBProfile profile, String id, ResourceObject resObj)
			throws SQLException {

		try (Connection con = getConnectionFromPool();
				PreparedStatement pst = con.prepareStatement(EXCC2T_SQL_QUERY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);) {
			pst.setString(1, id);
			try (ResultSet rs = pst.executeQuery()) {
				if (rs.next()) {

					Set<String> sValues = new TreeSet<>();

					// process attributes to pull
					for (String attributeName : profile.getTableEXCC2TAttributesToPull()) {

						String temp = rs.getString(profile.getTableEXCC2TAttributeDBColumnName(attributeName));

						if (!profile.isTableEXCC2TMultiAttribute(attributeName)) {
							resObj.addAttribute(attributeName.toLowerCase(),
									(temp == null) ? EvalValue.NULL : EvalValue.build(temp));
						} else {

							if (temp == null && rs.isLast()) {
								resObj.addAttribute(attributeName.toLowerCase(), EvalValue.build(Multivalue.EMPTY));
							} else {

								int iCurrentRow = rs.getRow();

								sValues.clear();

								temp = rs.getString(profile.getTableEXCC2TAttributeDBColumnName(attributeName));

								if (temp != null && temp.length() > 0) {
									sValues.add(temp.trim());
								}

								while (rs.next()) {
									temp = rs.getString(profile.getTableEXCC2TAttributeDBColumnName(attributeName));
									if (temp != null && temp.length() > 0) {
										sValues.add(temp.trim());
									}
								}

								rs.absolute(iCurrentRow);
								
								if (sValues.size() > 0) {
									resObj.addAttribute(attributeName.toLowerCase(),
											EvalValue.build(Multivalue.create(sValues)));
								} else {
									resObj.addAttribute(attributeName.toLowerCase(), EvalValue.build(Multivalue.EMPTY));
								}
							}
						}

					}

				}
			}

		}

	}
	

	/* (non-Javadoc)
	 * @see com.nextlabs.common.Provider#loadProfiles(java.util.Properties, java.lang.String)
	 */
	@Override
	public void loadProfiles(Properties props, String propsFilePath) {

		profiles = new HashMap<String, ResourceDBProfile>();

		String sProfileNames = props.getProperty("profile_names");

		if (sProfileNames != null) {
			String[] profileNames = sProfileNames.split(",");

			for (String name : profileNames) {

				name = name.trim();

				LOG.info(String.format("Loading profile of domain [%s]", name));
				ResourceDBProfile profile = new ResourceDBProfile(name);
				try {
					profile.parseProfile(props, propsFilePath);
					profiles.put(profile.getName(), profile);

					for (String attr : profile.getAttributesToPull()) {
						if (resAttributeToProfileMap.containsKey(attr.toLowerCase())) {
							resAttributeToProfileMap.get(attr.toLowerCase()).add(profile.getName());
						} else {
							List<String> newIndex = new ArrayList<String>();
							newIndex.add(profile.getName());
							resAttributeToProfileMap.put(attr.toLowerCase(), newIndex);
						}
					}


				} catch (InvalidProfileException ipe) {
					LOG.error(String.format("Invalid profile for domain [%s]", name), ipe);
				}

			}
		} else {
			LOG.warn("Profile names are undefined");
		}

	}

	/* (non-Javadoc)
	 * @see com.nextlabs.common.Provider#loadSingleProfile(java.util.Properties, java.lang.String)
	 */
	@Override
	public void loadSingleProfile(Properties props, String propsFilePath) {
		String name = "DB_1";

		LOG.info(String.format("Loading profile of domain [%s]", name));
		ResourceDBProfile profile = new ResourceDBProfile(name);
		try {
			profile.parseProfile(props, propsFilePath);
			singleProfile = profile;
		} catch (InvalidProfileException ipe) {
			LOG.error(String.format("Invalid profile for domain [%s]", name), ipe);
		}

	}

	/* (non-Javadoc)
	 * @see com.nextlabs.common.Provider#setIsSingleProfile(java.lang.Boolean)
	 */
	@Override
	public void setIsSingleProfile(Boolean isSingleProfile) {
		this.isSingleProfile = isSingleProfile;
	}

	/* (non-Javadoc)
	 * @see com.nextlabs.common.Provider#isRefreshing()
	 */
	@Override
	public Boolean isRefreshing() {
		return isRefreshing;
	}

	/* (non-Javadoc)
	 * @see com.nextlabs.common.Provider#setCommonProperties(java.util.Properties)
	 */
	@Override
	public void setCommonProperties(Properties props) {
		commonProp = props;
		
	}

	/* (non-Javadoc)
	 * @see com.nextlabs.common.Provider#getUserObject(java.lang.String, java.lang.String)
	 */
	@Override
	public UserObject getUserObject(String id, String attributeToSearch) throws Exception {
		return null;
	}

	/* (non-Javadoc)
	 * @see com.nextlabs.common.Provider#getProgramObject(java.lang.String, java.lang.String)
	 */
	@Override
	public ResourceObject getProgramObject(String id, String attributeToSearch) throws Exception {
		
		ResourceObject object = null;

		if (idToObjectTypeMap.get(id) == null
				|| (idToObjectTypeMap.get(id) != null && idToObjectTypeMap.get(id).equals(RESOURCE_TYPE))) {

			if (isSingleProfile) {
				
				object = queryForProgram(singleProfile, id);

			} else {

				List<String> profilesToLook = resAttributeToProfileMap.get(attributeToSearch.toLowerCase());
				if (profilesToLook == null) {
					LOG.error(String.format("Attribute [%s] isn't provided by any domain", attributeToSearch));
					return null;
				}

				for (String profileName : profilesToLook) {
					ResourceDBProfile dbProfile = profiles.get(profileName);

					LOG.info(String.format("Attribute [%s] should be found in domain [%s]. Attemp to query...",attributeToSearch, dbProfile.getName()));

					object = queryForProgram(dbProfile, id);

					if (object != null) {
						break;
					}
				}
			}
		} else {
			LOG.error(String.format("Type cannot be found for ID [%s]", id));
		}

		if (object == null) {
			LOG.error(String.format("Object [%s] cannot be queried from resources DB", id));
		}

		return object;
	}
	
	/**
	 * Query for a program with HD_ID and store in into the cache store
	 * @param dbProfile DBProfile
	 * @param resId Program HD_ID
	 * @return ResourceObject which matched the given resource ID
	 * @throws NamingException
	 * @throws SQLException
	 */
	private ResourceObject queryForProgram(ResourceDBProfile dbProfile, String resId)
			throws NamingException, SQLException {

		ResourceObject resObj = null;
		
		long startTime = System.currentTimeMillis();

		try (Connection con = getConnectionFromPool();
				PreparedStatement pst = con.prepareStatement(PROGRAM_SQL_QUERY_WITH_CONDITION.toString(), ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);) {
			pst.setString(1, resId);
			try (ResultSet rs = pst.executeQuery()) {
				while (rs.next()) {

					resObj = produceProgram(rs, dbProfile);

					// write user to cache
					ResourceCacheEngine.getInstance().writeObjectToProgramCache(resObj);

					// update identifier map
					for (String key : dbProfile.getKeyAttributes()) {
						if (resObj.getAttribute(key.toLowerCase()) != null
								&& resObj.getAttribute(key.toLowerCase()).getValue() != null) {
							ResourceCacheEngine.getInstance().addProgIdentifier(
									(String) resObj.getAttribute(key.toLowerCase()).getValue(), resObj.getId());
							idToObjectTypeMap.put((String) resObj.getAttribute(key.toLowerCase()).getValue(),
									RESOURCE_TYPE);
						}
					}
				}
			}

		}

		long endTime = System.currentTimeMillis();

		LOG.info(String.format("Query for Item [%s] took %dms", resId, (endTime - startTime)));

		return resObj;
	}
	
	/**
	 * Produce a ResourceObject from PROGHD table
	 * @param rs Resultset contain the main entry
	 * @param profile  ResourceDBProfile for the profile
	 * @return A ResourceObject 
	 * @throws NamingException
	 * @throws SQLException
	 */
	private ResourceObject produceProgram(ResultSet rs, ResourceDBProfile profile) throws NamingException, SQLException {

		ResourceObject resObj = null;

		String[] ids = new String[profile.getProgKeyAttributes().size()];

		for (int i = 0; i < profile.getProgKeyAttributes().size(); i++) {

			String keyAttributeName = profile.getProgKeyAttributes().get(i);

			String temp = rs.getString(profile.getProgKeyAtttributeDBColumnName(profile.getProgKeyAttributes().get(i)));
			if (temp == null) {
				ids[i] = "UNDEFINED";
			} else {
					ids[i] = temp;

					if (!profile.isProgKeyCaseSensitive(keyAttributeName)) {
						ids[i] = ids[i].toLowerCase();
					}
			}
		}

		String resId = Util.makeCombinedID(profile.getName(), ids);

		resObj = new ResourceObject(profile.getName(), resId, RESOURCE_TYPE);

		Set<String> sValues = new TreeSet<>();

		// process attributes to pull
		for (String attributeName: profile.getProgAttributesToPull()) {

			String temp = rs.getString(profile.getProgAttributeDBColumnName(attributeName));

			if (!profile.isProgMultiAttribute(attributeName)) {
				resObj.addAttribute(attributeName.toLowerCase(),(temp == null) ? EvalValue.NULL : EvalValue.build(temp));
			} else {

				if (temp == null && rs.isLast()) {
					resObj.addAttribute(attributeName.toLowerCase(), EvalValue.build(Multivalue.EMPTY));
				} else {
					
					int iCurrentRow = rs.getRow();

					sValues.clear();
					
					temp = rs.getString(profile.getProgAttributeDBColumnName(attributeName));
					
					if (temp != null && temp.length() > 0) {
						sValues.add(temp.trim());
					}

					while (rs.next()) {
						temp = rs.getString(profile.getProgAttributeDBColumnName(attributeName));
						if (temp != null && temp.length() > 0) {
							sValues.add(temp.trim());
						}
					}
					
					rs.absolute(iCurrentRow);

					if (sValues.size() > 0) {
						resObj.addAttribute(attributeName.toLowerCase(), EvalValue.build(Multivalue.create(sValues)));
					} else {
						resObj.addAttribute(attributeName.toLowerCase(), EvalValue.build(Multivalue.EMPTY));
					}
				}
			}

		}

		// process key attributes
		for (int i = 0; i < profile.getProgKeyAttributes().size(); i++) {
			String attributeName = profile.getKeyAttributes().get(i);

			String temp = rs.getString(profile.getProgKeyAtttributeDBColumnName(attributeName));
			
			if (temp != null) {
				
				resObj.addAttribute(attributeName.toLowerCase(),
						EvalValue.build((profile.isKeyCaseSensitive(attributeName)) ? temp.toString()
								: temp.toString().toLowerCase()));
			}
		}
		
		return resObj;
	}

}
