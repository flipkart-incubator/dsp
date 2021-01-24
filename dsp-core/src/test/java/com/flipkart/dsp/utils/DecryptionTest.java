package com.flipkart.dsp.utils;

import com.flipkart.dsp.exceptions.DecryptionException;
import com.flipkart.dsp.utils.Decryption;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * +
 */
public class DecryptionTest {

    @Test
    public void testDecryptSuccess() {
        Decryption.decrypt("54lnGm57xZRhnCpttF5rMg==", "FKdofurh1me2uh0y");
    }

    @Test
    public void testDecryptFailure() {
        boolean isException = false;

        try {
            Decryption.decrypt("sdjfhsjgfvf=", "FKdofurh1me2uh0y");
        } catch (DecryptionException e) {
            isException = true;
            assertTrue(e.getMessage().contains("Not able to decrypt input:"));
        }
        assertTrue(isException);
    }
}
