package com.ekbana.db.dbencrypt;

import org.apache.log4j.Logger;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

public class Encrypt {

    private Logger log = Logger.getLogger(Encrypt.class);

    public String getEncryptedText(Object input)
    {
        try {
            // Static getInstance method is called with hashing MD5
            MessageDigest md = MessageDigest.getInstance("MD5");

            // digest() method is called to calculate message digest
            //  of an input digest() return array of byte
            byte[] messageDigest = md.digest(input.toString().trim().getBytes());

            // Convert byte array into signum representation
            BigInteger no = new BigInteger(1, messageDigest);

            // Convert message digest into hex value
            String hashtext = no.toString(16);

            while (hashtext.length() < 32) {
                hashtext = "0".concat(hashtext);
            }
            return hashtext;
        }

        // For specifying wrong message digest algorithms
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public String getSHA(Object obj){
        String generatedPassword;
        StringBuilder final_data = null;
        try{
            MessageDigest md=MessageDigest.getInstance("SHA-256");
//            md.update(salt.getBytes());
            byte[] bytes=md.digest(obj.toString().getBytes());

            StringBuilder sb=new StringBuilder();
            for (byte aByte : bytes) {
                sb.append(Integer.toString((aByte & 0xFF) + 0x100, 16));
            }
            generatedPassword=sb.toString();

            Random r=new Random();
            final_data = new StringBuilder();
            for(int i=0;i<32;i++){
                int randomNumber=r.nextInt(generatedPassword.length());
                final_data.append(generatedPassword.charAt(randomNumber));
            }
        }catch(NoSuchAlgorithmException ex){
            log.error(ex.getMessage());
        }

        return final_data.toString();
    }
}
