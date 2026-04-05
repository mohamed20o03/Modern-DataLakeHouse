package com.abdelwahab.api_service.module.cdc.security;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * Encrypts and decrypts source database passwords using AES-256-GCM.
 *
 * <p>AES-256-GCM provides both confidentiality and integrity (authenticated
 * encryption). The encryption key is loaded from the {@code CDC_ENCRYPTION_KEY}
 * environment variable (Base64-encoded, 32 bytes).
 *
 * <p><b>Output format:</b> {@code Base64(IV || ciphertext || authTag)}
 * where IV is 12 bytes (GCM standard).
 *
 * <p><b>Security:</b> Each encryption uses a fresh random IV, so encrypting
 * the same plaintext twice produces different ciphertext.
 */
@Slf4j
@Component
public class CredentialEncryptor {

    private static final String ALGORITHM = "AES/GCM/NoPadding";
    private static final int GCM_IV_LENGTH = 12;   // 96 bits — GCM standard
    private static final int GCM_TAG_LENGTH = 128;  // 128Mike auth tag bits

    private final SecretKey secretKey;
    private final SecureRandom secureRandom = new SecureRandom();

    /**
     * @param base64Key Base64-encoded 32-byte AES key from {@code CDC_ENCRYPTION_KEY}
     */
    public CredentialEncryptor(
            @Value("${cdc.encryption.key:#{null}}") String base64Key) {

        if (base64Key == null || base64Key.isBlank()) {
            // Fallback for development — MUST be overridden in production
            log.warn("CDC_ENCRYPTION_KEY not set — using default dev key. "
                    + "This is insecure and MUST be changed in production!");
            base64Key = "dGhpcy1pcy1hLTMyLWJ5dGUtZGV2LWtleSEh";
        }

        byte[] keyBytes = Base64.getDecoder().decode(base64Key);
        if (keyBytes.length != 32) {
            throw new IllegalArgumentException(
                    "CDC_ENCRYPTION_KEY must be exactly 32 bytes (256 bits) when Base64-decoded. "
                    + "Got " + keyBytes.length + " bytes.");
        }

        this.secretKey = new SecretKeySpec(keyBytes, "AES");
        log.info("CredentialEncryptor initialised (AES-256-GCM)");
    }

    /**
     * Encrypts a plaintext string.
     *
     * @param plaintext the value to encrypt (e.g., a database password)
     * @return Base64-encoded ciphertext (IV + encrypted data + auth tag)
     */
    public String encrypt(String plaintext) {
        try {
            byte[] iv = new byte[GCM_IV_LENGTH];
            secureRandom.nextBytes(iv);

            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, new GCMParameterSpec(GCM_TAG_LENGTH, iv));

            byte[] ciphertext = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));

            // Concatenate IV + ciphertext for storage
            ByteBuffer output = ByteBuffer.allocate(iv.length + ciphertext.length);
            output.put(iv);
            output.put(ciphertext);

            return Base64.getEncoder().encodeToString(output.array());

        } catch (Exception e) {
            throw new RuntimeException("Failed to encrypt credential", e);
        }
    }

    /**
     * Decrypts a previously encrypted value.
     *
     * @param encryptedBase64 Base64-encoded ciphertext (IV + encrypted data + auth tag)
     * @return the original plaintext
     */
    public String decrypt(String encryptedBase64) {
        try {
            byte[] decoded = Base64.getDecoder().decode(encryptedBase64);

            ByteBuffer buffer = ByteBuffer.wrap(decoded);
            byte[] iv = new byte[GCM_IV_LENGTH];
            buffer.get(iv);
            byte[] ciphertext = new byte[buffer.remaining()];
            buffer.get(ciphertext);

            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, new GCMParameterSpec(GCM_TAG_LENGTH, iv));

            byte[] plaintext = cipher.doFinal(ciphertext);
            return new String(plaintext, StandardCharsets.UTF_8);

        } catch (Exception e) {
            throw new RuntimeException("Failed to decrypt credential", e);
        }
    }
}
