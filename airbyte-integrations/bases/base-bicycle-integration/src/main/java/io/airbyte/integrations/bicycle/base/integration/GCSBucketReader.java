package io.airbyte.integrations.bicycle.base.integration;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sumitmaheshwari
 * Created on 20/05/2024
 */
public class GCSBucketReader {

    private static final Logger logger = LoggerFactory.getLogger(GCSBucketReader.class.getName());
    public Map<String, String> getFileNameVsSignedUrlForFilesInGCPBucket(String serviceAccountKeyJson, String bucketUrl,
                                                                         String regexPattern) throws IOException {

        // Extract the bucket name from the URL
        Map<String, String> fileNameToSignedUrl = new HashMap<>();
        String bucketName = extractBucketName(bucketUrl);
        logger.info("Bucket name extracted {}", bucketName);

        // Extract the prefix (optional, if you want to filter within a specific folder)
        String prefix = extractPrefix(bucketUrl);

        Storage storage = null;
        try {
            // Authenticate using the service account key JSON string
            GoogleCredentials credentials =
                    GoogleCredentials.fromStream(new ByteArrayInputStream(serviceAccountKeyJson
                            .getBytes(Charset.defaultCharset())));
            storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
        } catch (IOException e) {
            throw e;
        }

        // Compile the pattern
        Pattern pattern = Pattern.compile(regexPattern);

        // Get the bucket
        Bucket bucket = storage.get(bucketName);

        // List all blobs in the bucket with the given prefix (if any)
        for (Blob blob : bucket.list(Storage.BlobListOption.prefix(prefix)).iterateAll()) {
            String fileName = blob.getName();
            fileName = extractFileName(fileName);
            // Match the file name with the regex pattern
            Matcher matcher = pattern.matcher(fileName);
            if (matcher.matches()) {
                URL signedUrl = storage.signUrl(blob, 4, TimeUnit.HOURS);
                //URL signedUrl = blob.signUrl(2, TimeUnit.HOURS, Storage.SignUrlOption.withV4Signature());
                fileNameToSignedUrl.put(fileName, signedUrl.toString());
            }
        }

        return fileNameToSignedUrl;
    }

    private String extractBucketName(String url) {
        String[] parts = url.split("/");
        return parts[2]; // "your-bucket-name"
    }

    // Method to extract the prefix (path within the bucket) from the URL
    private String extractPrefix(String url) {
        String[] parts = url.split("/", 4);
        return parts.length > 3 ? parts[3] : "";
    }

    private String extractFileName(String fullPath) {
        int lastSlashIndex = fullPath.lastIndexOf('/');
        return lastSlashIndex >= 0 ? fullPath.substring(lastSlashIndex + 1) : fullPath;
    }
}
