/*
Copyright 2021 JD Project Authors. Licensed under Apache-2.0.

Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.jd.jdbc.vindexes.cryto;

import com.jd.jdbc.key.Uint64key;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESedeKeySpec;

public final class TripleDES {

    public static final String DESEDE_ENCRYPTION_SCHEME = "DESede/ECB/NoPadding";

    public static final String ALGO_NAME = "DESede";

    private static final Log log = LogFactory.getLog(TripleDES.class);

    private static final String UNICODE_FORMAT = "UTF8";

    private static final TripleDES INSTANCE = new TripleDES();

    private static final Cipher DEC_CIPHER;

    private static final Cipher ENC_CIPHER;

    static {
        KeySpec myKeySpec;
        SecretKeyFactory mySecretKeyFactory;
        SecretKey key;
        byte[] keyAsBytes = new byte[24];
        String myEncryptionScheme;

        myEncryptionScheme = DESEDE_ENCRYPTION_SCHEME;
        try {
            myKeySpec = new DESedeKeySpec(keyAsBytes);
        } catch (InvalidKeyException e) {
            throw new RuntimeException("Build DESedeKeySpec failed", e);
        }
        try {
            mySecretKeyFactory = SecretKeyFactory.getInstance(ALGO_NAME);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Build SecretKeyFactory failed", e);
        }
        try {
            ENC_CIPHER = Cipher.getInstance(myEncryptionScheme);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
            throw new RuntimeException("Build ENC Cipher failed", e);
        }
        try {
            DEC_CIPHER = Cipher.getInstance(myEncryptionScheme);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
            throw new RuntimeException("Build DEC Cipher failed", e);
        }
        try {
            key = mySecretKeyFactory.generateSecret(myKeySpec);
        } catch (InvalidKeySpecException e) {
            throw new RuntimeException("SecretKeyFactory generateSecret failed", e);
        }

        try {
            ENC_CIPHER.init(Cipher.ENCRYPT_MODE, key);
            DEC_CIPHER.init(Cipher.DECRYPT_MODE, key);
        } catch (InvalidKeyException e) {
            throw new RuntimeException("ENC_CIPHER, DEC_CIPHER generateSecret failed", e);
        }
    }

    private TripleDES() {
    }

    public static TripleDES getInstance() {
        return INSTANCE;
    }

    /**
     * Method To Encrypt The String
     */
    public synchronized byte[] encrypt(BigInteger i) {
        byte[] encryptedByte;
        try {
            byte[] plainText = Uint64key.bytes(i);//unencryptedString.getBytes(UNICODE_FORMAT);
            byte[] encryptedText = ENC_CIPHER.doFinal(plainText);
            encryptedByte = Arrays.copyOf(encryptedText, 8);
            return encryptedByte;
        } catch (Exception e) {
            log.error("TripleDES.encrypt.exception", e);
            throw new RuntimeException("TripleDES encrypt failed", e);
        }
    }

    public synchronized long decrypt(final byte[] encryptedText) {
        try {
            byte[] plainText = DEC_CIPHER.doFinal(encryptedText);
            return Uint64key.uint64(plainText).longValue();
        } catch (Exception e) {
            throw new RuntimeException("TripleDES decrypt failed", e);
        }
    }
}
