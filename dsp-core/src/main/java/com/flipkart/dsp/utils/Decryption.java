package com.flipkart.dsp.utils;

import com.flipkart.dsp.exceptions.DecryptionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

/**
 * +
 */
@Slf4j
public class Decryption {

    public static String decrypt(String input, String saltKey) {
        try {
            Cipher cipher = Cipher.getInstance("AES");
            SecretKeySpec secretKey = new SecretKeySpec(saltKey.getBytes(), "AES");
            cipher.init(Cipher.DECRYPT_MODE, secretKey);
            byte[] decryptedByte = cipher.doFinal(Base64.decodeBase64(input));
            return new String(decryptedByte);
        } catch (Exception e) {
            String errorMessage = String.format("Not able to decrypt input: %s, ErrorMessage: %s", input, e.getMessage());
            throw new DecryptionException(errorMessage, e.getCause());
        }
    }

}
