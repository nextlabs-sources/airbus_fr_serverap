package com.nextlabs.common;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;

import com.nextlabs.db.ResourceDBProfile;
 
/**
 * This class handle all the encrypt and decrypt for the properties file
 * 
 * @author klee
 *
 */
public class EncryptDecrypt {
    private final String propertyFileName;
    private final String propertyKey;
    private final String isPropertyKeyEncrypted;
 
    final String decryptedUserPassword;
    private static final String sEncryptionKey = "s$%wVpeOb@pp";
    
    private static final Log LOG = LogFactory.getLog(ResourceDBProfile.class);
 
    /**
     * The constructor does most of the work.
     * It initializes all final variables and invoke two methods
     * for encryption and decryption job. After successful job
     * the constructor puts the decrypted password in variable
     * to be retrieved by calling class.
     * 
	 * 
	 * @param pPropertyFileName /Name of the properties file that contains the password
	 * @param pUserPasswordKey	/Left hand side of the password property as key. 
	 * @param pIsPasswordEncryptedKey 	/Key in the properties file that will tell us if the password is already encrypted or not
	 * 
	 * @throws Exception java.lang.Exception
	 */
    public EncryptDecrypt(String pPropertyFileName,String pUserPasswordKey, String pIsPasswordEncryptedKey) throws Exception {
        this.propertyFileName = pPropertyFileName;
        this.propertyKey = pUserPasswordKey;
        this.isPropertyKeyEncrypted = pIsPasswordEncryptedKey;
        try {
            encryptPropertyValue();
        } catch (Exception e) {
            throw new Exception("Problem encountered during encryption process",e);
        }
        decryptedUserPassword = decryptPropertyValue();
 
    }
    
    public String getDecryptValue() {
    	return decryptedUserPassword;
    }
 
    /**
     * The method that encrypt password in the properties file. 
     * This method will first check if the password is already encrypted or not. 
     * If not then only it will encrypt the password.
     * 
     * @throws ConfigurationException
     */
    private void encryptPropertyValue() throws ConfigurationException {
    	
        //Apache Commons Configuration 
        PropertiesConfiguration config = new PropertiesConfiguration(propertyFileName);
 
        //Retrieve boolean properties value to see if password is already encrypted or not
        String isEncrypted = config.getString(isPropertyKeyEncrypted);
 
        //Check if password is encrypted?
        if(isEncrypted.equals("false")){
            String tmpPwd = config.getString(propertyKey);
            //Encrypt
            StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
            // This is a required password for Jasypt. You will have to use the same password to
            // retrieve decrypted password later.
            // This password is not the password we are trying to encrypt taken from properties file.
            encryptor.setPassword(sEncryptionKey);
            String encryptedPassword = encryptor.encrypt(tmpPwd);
            LOG.info("Encryption done and encrypted password is : " + encryptedPassword ); 
 
            // Overwrite password with encrypted password in the properties file using Apache Commons Configuration library
            config.setProperty(propertyKey, encryptedPassword);
            // Set the boolean flag to true to indicate future encryption operation that password is already encrypted
            config.setProperty(isPropertyKeyEncrypted,"true");
            // Save the properties file
            config.save();
        }else{
        	LOG.info("User password is already encrypted.");
        }
    }
 
    /**
     * This method decrypt the encrypted value in properties file
     * @return
     * @throws ConfigurationException
     */
    private String decryptPropertyValue() throws ConfigurationException  {
    	LOG.info("Starting decryption");
        PropertiesConfiguration config = new PropertiesConfiguration(propertyFileName);
        String encryptedPropertyValue = config.getString(propertyKey);

        StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
        encryptor.setPassword(sEncryptionKey);
        String decryptedPropertyValue = encryptor.decrypt(encryptedPropertyValue);
 
        return decryptedPropertyValue;
    }
    
}
    
