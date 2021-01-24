package com.flipkart.dsp.utils;

/**
 * +
 */

import com.flipkart.dsp.exceptions.EncryptionException;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;

public class Encryption {

    public static String encrypt(String input, String saltKey) throws EncryptionException {
        try {
            String algorithm = "AES";
            byte[] plainTextByte = input.getBytes();
            Base64.Encoder encoder = Base64.getEncoder();

            SecretKeySpec secretKey = new SecretKeySpec(saltKey.getBytes(), algorithm);
            Cipher cipher = Cipher.getInstance(algorithm);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            byte[] encryptedByte = cipher.doFinal(plainTextByte);
            return encoder.encodeToString(encryptedByte);
        } catch (Exception e) {
            throw new EncryptionException("Not able to encrypt input :" + input);
        }
    }

}
