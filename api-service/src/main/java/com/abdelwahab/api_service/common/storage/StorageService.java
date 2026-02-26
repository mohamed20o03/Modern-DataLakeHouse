package com.abdelwahab.api_service.common.storage;

import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Business logic layer for file storage operations.
 * <p>
 * This service acts as an intermediary between controllers and storage repositories,
 * providing a location for business logic such as:
 * </p>
 * <ul>
 *   <li>File validation (size, type, name)</li>
 *   <li>Security checks (virus scanning, content validation)</li>
 *   <li>Metadata management</li>
 *   <li>Audit logging</li>
 *   <li>Error handling and retry logic</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StorageService {
    
    private final StorageRepository repository;
    
    /**
     * Stores a file for a specific job.
     * <p>
     * Currently delegates directly to the repository. Future enhancements could include
     * file validation, security scanning, or metadata enrichment.
     * </p>
     * 
     * @param file  multipart file upload payload
     * @param jobId job identifier used for object organization
     * @return object path in the backing store
     * @throws com.abdelwahab.api_service.common.exception.StorageException if storage operation fails
     */
    public String storeFile(MultipartFile file, String jobId) {
        log.info("Storing file for job: jobId={}, filename={}, size={}", jobId, file.getOriginalFilename(), file.getSize());
        
        // Future: Add validation, security checks, etc. here
        
        return repository.storeFile(file, jobId);
    }
}
