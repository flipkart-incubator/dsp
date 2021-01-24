package com.flipkart.dsp.utils;

import com.flipkart.dsp.exceptions.EncryptionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({Encryption.class, Base64.class, SecretKeySpec.class, Cipher.class})
public class EncryptionTest {
    @Mock
    private Base64.Encoder encoder;
    @Mock private SecretKeySpec secretKeySpec;

    private Cipher cipher;
    private Encryption encryption;
    private byte[] bytes = new byte[100];

    @Before
    public void setUp() throws Exception {
        cipher = PowerMockito.mock(Cipher.class);
        PowerMockito.mockStatic(Base64.class);
        PowerMockito.mockStatic(Cipher.class);
        PowerMockito.mockStatic(SecretKeySpec.class);
        MockitoAnnotations.initMocks(this);
        this.encryption = spy(new Encryption());

        PowerMockito.when(Base64.getEncoder()).thenReturn(encoder);
        PowerMockito.whenNew(SecretKeySpec.class).withAnyArguments().thenReturn(secretKeySpec);
        PowerMockito.when(Cipher.getInstance("AES")).thenReturn(cipher);
    }

    @Test
    public void testEncryptSuccess() throws Exception {
        doNothing().when(cipher).init(1, secretKeySpec);
        when(cipher.doFinal(any())).thenReturn(bytes);
        when(encoder.encodeToString(bytes)).thenReturn("encryptedString");

        String actual = encryption.encrypt("input","saltKey");
        assertNotNull(actual);
        assertEquals(actual, "encryptedString");
        verifyStatic(Base64.class);
        Base64.getEncoder();
        verifyNew(SecretKeySpec.class).withArguments(any(), any());
        verifyStatic(Cipher.class);
        Cipher.getInstance("AES");
        verify(cipher, times(1)).init(1, secretKeySpec);
        verify(cipher, times(1)).doFinal(any());
        verify(encoder, times(1)).encodeToString(bytes);
    }

    @Test
    public void testEncryptFailure() throws Exception {
        PowerMockito.when(Cipher.getInstance("AES")).thenThrow(new NoSuchAlgorithmException());

        boolean isException = false;
        try {
            encryption.encrypt("input", "saltKey");
        } catch (EncryptionException e) {
            isException = true;
            assertEquals(e.getMessage(), "Not able to encrypt input :input");
        }

        assertTrue(isException);
        verifyStatic(Base64.class);
        Base64.getEncoder();
        verifyNew(SecretKeySpec.class).withArguments(any(), any());
        verifyStatic(Cipher.class);
        Cipher.getInstance("AES");
    }

}
