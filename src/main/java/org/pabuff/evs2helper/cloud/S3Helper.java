package org.pabuff.evs2helper.cloud;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.springframework.core.io.InputStreamResource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Logger;

public class S3Helper {

    Logger logger = Logger.getLogger(S3Helper.class.getName());

    public S3Client getS3Client(Map<String, Object> s3Config) {
        // Convert your existing credentials to the new format

        if(s3Config == null){
            logger.severe("S3 configuration is null");
            return null;
        }

        if(!s3Config.containsKey("access_key_id") || !s3Config.containsKey("secret_access_key") || !s3Config.containsKey("path") || !s3Config.containsKey("region")){
            logger.severe("S3 configuration is missing required fields");
            return null;
        }

        String accessKeyId = (String) s3Config.get("access_key_id");
        String secretAccessKey = (String) s3Config.get("secret_access_key");
        String path = (String) s3Config.get("path");
        String region = (String) s3Config.get("region");

        try{
            AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessKeyId, secretAccessKey);

            return S3Client.builder()
                    .endpointOverride(URI.create(path)) // Set your endpoint here
                    .region(Region.of(region)) // Replace with the appropriate region if necessary
                    .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                    .serviceConfiguration(S3Configuration.builder().build())
                    .build();
        }catch (Exception e){
            logger.severe("Failed to create S3 client: " + e.getMessage());
            return null;
        }
    }

    public Map<String, Object> getObjectUrl(Map<String, Object> req){
        logger.info("getObjectUrl() called");

        if(req == null){
            logger.severe("S3 configuration is null");
            return Collections.singletonMap("error", "null");
        }

        if(!req.containsKey("bucket_name") || !req.containsKey("object_name") || !req.containsKey("s3_client")){
            logger.severe("S3 configuration is missing required fields");
            return Collections.singletonMap("error", "missing fields");
        }

        String bucketName = (String) req.get("bucket_name");
        String objectName = (String) req.get("object_name");
        S3Client s3Client = (S3Client) req.get("s3_client");
        String url;

        try {
            // Generate a pre-signed URL for the object
            GetUrlRequest getUrlRequest = GetUrlRequest.builder()
                    .bucket(bucketName)
                    .key(objectName)
                    .build();

            url = String.valueOf(s3Client.utilities().getUrl(getUrlRequest));
        }catch (S3Exception e) {
            logger.severe("Object not found: " + objectName);
            return Collections.singletonMap("error", "Object does not exist in bucket.");
        } catch (Exception e) {
            logger.severe("Failed to get object URL: " + e.getMessage());
            return Collections.singletonMap("error", e.getMessage());
        }

            Map<String, Object> result = new HashMap<>();
            result.put("url", url);
            result.put("object_name", objectName);
            result.put("bucket_name", bucketName);

            return Map.of("result", result);
    }

    public Map<String, Object> checkObjectExists(S3Client s3Client, String bucketName, String objectName) {
        try {
            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectName)
                    .build();

            s3Client.headObject(headObjectRequest);
            logger.info("Object already exists in S3 bucket: " + bucketName + " with object name: " + objectName);
            return Collections.singletonMap("result", Map.of("exists", true));
        } catch (NoSuchKeyException e) {
            logger.info("Object does not exist in S3 bucket: " + objectName.split("/")[objectName.split("/").length - 1]);
            return Collections.singletonMap("result", Map.of("exists", false));
        } catch (AmazonServiceException ase) {
            logger.severe("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            logger.severe("Error Message:    " + ase.getMessage());
            logger.severe("HTTP Status Code: " + ase.getStatusCode());
            logger.severe("AWS Error Code:   " + ase.getErrorCode());
            logger.severe("Error Type:       " + ase.getErrorType());
            logger.severe("Request ID:       " + ase.getRequestId());
            return Collections.singletonMap("error", ase.getMessage());
        } catch (AmazonClientException ace) {
            logger.severe("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            logger.severe("Error Message: " + ace.getMessage());
            return Collections.singletonMap("error", ace.getMessage());
        }
    }

    public Map<String, Object> moveObject(Map<String, Object> req) {

        logger.info("moveObject() called");

        if (req == null) {
            logger.severe("S3 configuration is null");
            return Collections.singletonMap("error", "s3 configuration is null");
        }

        if (!req.containsKey("source_bucket") || !req.containsKey("source_object") || !req.containsKey("destination_bucket") || !req.containsKey("destination_object") || !req.containsKey("s3_client")) {
            logger.severe("S3 configuration is missing required fields");
            return Collections.singletonMap("error", "missing fields");
        }

        String sourceBucket = (String) req.get("source_bucket");
        String sourceKey = (String) req.get("source_object");
        String destinationBucket = (String) req.get("destination_bucket");
        String destinationKey = (String) req.get("destination_object");
        S3Client s3 = (S3Client) req.get("s3_client");
        ObjectCannedACL acl = AclUseCase.fromMap(req);

        // Check if the source object exists
        Map<String, Object> objectExist = checkObjectExists(s3, sourceBucket, sourceKey);

        if(objectExist.containsKey("error")){
            logger.severe("Failed to check if object exists: " + objectExist.get("error"));
            return Collections.singletonMap("error", objectExist.get("error"));
        }

        if(objectExist.get("result") == null){
            logger.severe("Failed to check if object exists: " + objectExist.get("error"));
            return Collections.singletonMap("error", objectExist.get("error"));
        }

        Map<String, Object> existResult = (Map<String, Object>) objectExist.get("result");

        if (existResult.containsKey("exists") && !(boolean) existResult.get("exists")) {
            logger.severe("Source object does not exist in S3 bucket: " + sourceBucket + " with object name: " + sourceKey);
            return Collections.singletonMap("error", "source object does not exist");
        }

        // Step 1: Copy the object
        try{
            CopyObjectRequest copyRequest = CopyObjectRequest.builder()
                    .sourceBucket(sourceBucket)
                    .sourceKey(sourceKey)
                    .destinationBucket(destinationBucket)
                    .destinationKey(destinationKey)
                    .acl(acl)
                    .build();

            s3.copyObject(copyRequest);
        }catch (Exception e){
            logger.severe("Failed to copy object: " + e.getMessage());
            return Collections.singletonMap("error", e.getMessage());
        }

        // Step 2: Delete the original object
        try{
            DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                    .bucket(sourceBucket)
                    .key(sourceKey)
                    .build();

            s3.deleteObject(deleteRequest);
        }catch (Exception e){
            logger.severe("Failed to delete object: " + e.getMessage());
            return Collections.singletonMap("error", e.getMessage());
        }

        logger.info("Object moved successfully from " + sourceBucket + "/" + sourceKey + " to " + destinationBucket + "/" + destinationKey);
        return Collections.singletonMap("success", "Object moved successfully from " + sourceBucket + "/" + sourceKey + " to " + destinationBucket + "/" + destinationKey);
    }

    public Map<String, Object> putObject(Map<String, Object> s3) {

        logger.info("putObject() called");

        if(s3 == null){
            logger.severe("S3 configuration is null");
            return Collections.singletonMap("error", "null");
        }

        if(!s3.containsKey("bucket_name") || !s3.containsKey("object_name") || !s3.containsKey("file") || !s3.containsKey("s3_client")){
            logger.severe("S3 configuration is missing required fields");
            return Collections.singletonMap("error", "missing fields");
        }

        String bucketName = (String) s3.get("bucket_name");
        String objectName = (String) s3.get("object_name");
        File theFile = (File) s3.get("file");
        S3Client s3Client = (S3Client) s3.get("s3_client");
        ObjectCannedACL acl = AclUseCase.fromMap(s3);

        logger.info("Uploading file to S3 bucket: " + bucketName + " with object name: " + objectName);

        //check if object exist
        Map<String, Object> objectExist = checkObjectExists(s3Client, bucketName, objectName);

        if(objectExist.containsKey("error")){
            logger.severe("Failed to check if object exists: " + objectExist.get("error"));
            return Collections.singletonMap("error", objectExist.get("error"));
        }

        if(objectExist.get("result") == null){
            logger.severe("Failed to check if object exists: " + objectExist.get("error"));
            return Collections.singletonMap("error", objectExist.get("error"));
        }

        Map<String, Object> existResult = (Map<String, Object>) objectExist.get("result");

        if(existResult.containsKey("exists") && (boolean) existResult.get("exists")){
            logger.severe("Object already exists in S3 bucket: " + bucketName + " with object name: " + objectName);
            return Collections.singletonMap("error", "already exist");
        }

        try{

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .acl(acl)
                    .key(objectName)
                    .build();

            // Upload the file
            PutObjectResponse response = s3Client.putObject(putObjectRequest, Paths.get(theFile.getAbsolutePath()));
            logger.info("File uploaded successfully to S3 bucket: " + bucketName + " with object name: " + objectName);
            return Collections.singletonMap("success", "File uploaded successfully to S3 bucket: " + objectName.split("/")[objectName.split("/").length - 1]);

        }catch (AmazonServiceException ase) {
            logger.severe("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            logger.severe("Error Message:    " + ase.getMessage());
            logger.severe("HTTP Status Code: " + ase.getStatusCode());
            logger.severe("AWS Error Code:   " + ase.getErrorCode());
            logger.severe("Error Type:       " + ase.getErrorType());
            logger.severe("Request ID:       " + ase.getRequestId());
            return Collections.singletonMap("error", ase.getMessage());
        } catch (AmazonClientException ace) {
            logger.severe("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            logger.severe("Error Message: " + ace.getMessage());
            return Collections.singletonMap("error", ace.getMessage());
        }
    }

    public Map<String, Object> deleteObject(Map<String, Object> s3) {

        logger.info("deleteObject() called");

        if(s3 == null){
            logger.severe("S3 configuration is null");
            return Collections.singletonMap("error", "null");
        }

        if(!s3.containsKey("bucket_name") || !s3.containsKey("object_name") || !s3.containsKey("s3_client")){
            logger.severe("S3 configuration is missing required fields");
            return Collections.singletonMap("error", "missing fields");
        }

        String bucketName = (String) s3.get("bucket_name");
        String objectName = (String) s3.get("object_name");
        S3Client s3Client = (S3Client) s3.get("s3_client");

        try{
            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectName)
                    .build();

            s3Client.deleteObject(deleteObjectRequest);
            logger.info("Object deleted successfully from S3 bucket: " + bucketName + " with object name: " + objectName);
            return Collections.singletonMap("success", "Object deleted successfully from S3 bucket: " + objectName.split("/")[objectName.split("/").length - 1]);

        }catch (NoSuchKeyException e){
            logger.info("Object does not exist in S3 bucket: " + bucketName + " with object name: " + objectName);
            return Collections.singletonMap("error", "not exist");
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            logger.severe("Error Message:    " + ase.getMessage());
            logger.severe("HTTP Status Code: " + ase.getStatusCode());
            logger.severe("AWS Error Code:   " + ase.getErrorCode());
            logger.severe("Error Type:       " + ase.getErrorType());
            logger.severe("Request ID:       " + ase.getRequestId());
            return Collections.singletonMap("error", ase.getMessage());
        } catch (AmazonClientException e){
            logger.severe("Failed to delete object: " + e.getMessage());
            return Collections.singletonMap("error", e.getMessage());
        }
    }

    public Map<String, Object> getObject(Map<String, Object> s3){

        logger.info("getObject() called");

        if (s3 == null) {
            logger.severe("S3 configuration is null");
            return Collections.singletonMap("error", "null");
        }

        if (!s3.containsKey("bucket_name") || !s3.containsKey("object_name") || !s3.containsKey("s3_client")) {
            logger.severe("S3 configuration is missing required fields");
            return Collections.singletonMap("error", "missing fields");
        }

        String bucketName = (String) s3.get("bucket_name");
        String objectName = (String) s3.get("object_name");
        S3Client s3Client = (S3Client) s3.get("s3_client");

        GetObjectRequest getObjectRequest = null;

        try {
            getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectName)
                    .build();

        } catch (NoSuchKeyException e){
            logger.severe("Object does not exist in S3 bucket: " + bucketName + " with object name: " + objectName);
            return Collections.singletonMap("error", "not exist");
        } catch (AmazonServiceException ase) {
            logger.severe("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            logger.severe("Error Message:    " + ase.getMessage());
            logger.severe("HTTP Status Code: " + ase.getStatusCode());
            logger.severe("AWS Error Code:   " + ase.getErrorCode());
            logger.severe("Error Type:       " + ase.getErrorType());
            logger.severe("Request ID:       " + ase.getRequestId());
            return Collections.singletonMap("error", ase.getMessage());
        } catch (AmazonClientException ace) {
            logger.severe("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            logger.severe("Error Message: " + ace.getMessage());
            return Collections.singletonMap("error", ace.getMessage());
        }

        if(getObjectRequest == null){
            logger.severe("Failed to create GetObjectRequest");
            return Collections.singletonMap("error", "Failed to create GetObjectRequest");
        }

        Map<String, Object> request = new HashMap<>();
        request.put("request", getObjectRequest);
        request.put("object_name", objectName);

        Map<String, Object> result = processObject(request, s3Client);

        if(result.containsKey("error")){
            logger.severe("Failed to process object: " + objectName);
            return Collections.singletonMap("error", result.get("error"));
        }

        return result;
    }

    //get files from s3
    public Map<String, Object> getObjectList(Map<String, Object> s3){

        logger.info("getObject() called");

        if(s3 == null){
            logger.severe("S3 configuration is null");
            return Collections.singletonMap("error", "null");
        }

        if(!s3.containsKey("bucket_name") || !s3.containsKey("s3_client")){
            logger.severe("S3 configuration is missing required fields");
            return Collections.singletonMap("error", "missing fields");
        }

        String bucketName = (String) s3.get("bucket_name");
        String objectPrefix = (String) s3.getOrDefault("object_prefix", null);
        S3Client s3Client = (S3Client) s3.get("s3_client");
        Boolean includeSubfolders = (Boolean) s3.getOrDefault("include_subfolders", true);

        List<Map<String, Object>> objectRequests = new ArrayList<>();

        try{
            ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                    .bucket(bucketName);

            if(objectPrefix != null){
                requestBuilder.prefix(objectPrefix);
            }

            if(!includeSubfolders){
                requestBuilder.delimiter("/");
            }

            ListObjectsV2Request listObjectsRequest = requestBuilder.build();
            ListObjectsV2Response response = s3Client.listObjectsV2(listObjectsRequest);

            for (S3Object object : response.contents()) {
                String key = object.key();

                if(key.endsWith("/")){
                    continue;
                }

                String objectName = key.substring(key.lastIndexOf("/") + 1);

                Map<String, Object> request = new HashMap<>();
                request.put("object_name", objectName);
                request.put("object_name_full", key);
                objectRequests.add(request);
            }

            Map<String, Object> result = new HashMap<>();
            result.put("result", objectRequests);
            return result;

        }catch (AmazonServiceException ase) {
            logger.severe("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            logger.severe("Error Message:    " + ase.getMessage());
            logger.severe("HTTP Status Code: " + ase.getStatusCode());
            logger.severe("AWS Error Code:   " + ase.getErrorCode());
            logger.severe("Error Type:       " + ase.getErrorType());
            logger.severe("Request ID:       " + ase.getRequestId());
            return Collections.singletonMap("error", ase.getMessage());
        } catch (AmazonClientException ace) {
            logger.severe("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            logger.severe("Error Message: " + ace.getMessage());
            return Collections.singletonMap("error", ace.getMessage());
        }
    }

    //test connection to S3
    public void testConnection(S3Client s3Client) {
        try {

            // Attempt to list buckets to test the connection
            List<Bucket> buckets = s3Client.listBuckets().buckets();

            // If we can list the buckets, the connection is successful
            logger.info("S3 connection successful. Buckets:");
            for (Bucket bucket : buckets) {
                logger.info("Bucket : " + bucket.name());
                logger.info("Creation Date : " + bucket.creationDate());

                ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                        .bucket(bucket.name())
                        .build();
                ListObjectsV2Response response = s3Client.listObjectsV2(listObjectsRequest);

                logger.info("Files in bucket: " + bucket.name());
                for (S3Object object : response.contents()) {
                    logger.info("File Name: " + object.key());
                    logger.info("Size: " + object.size());
                    logger.info("Last Modified: " + object.lastModified());
                }

            }

        } catch (AmazonS3Exception e) {
            logger.severe("Failed to connect to S3: " + e.getErrorMessage());
        } catch (Exception e) {
            logger.severe("Unexpected error: " + e.getMessage());
        }
    }

    public Map<String, Object> processObject (Map<String, Object> request, S3Client s3Client) {

        Map<String, Object> result = new HashMap<>();

        if(!request.containsKey("request") || !request.containsKey("object_name")){
            logger.severe("Request is missing");
            result.put("error", "Request is missing");
            return Collections.singletonMap("error", "Request is missing");
        }

        GetObjectRequest getObjectRequest = (GetObjectRequest) request.get("request");
        String objectName = (String) request.get("object_name");

        if(!objectName.equals(getObjectRequest.key())){
            logger.severe("Object name does not match request key");
            result.put("error", "Object name does not match request key");
            return Collections.singletonMap("error", "Object name does not match request key");
        }

        Map<String, Object> processedFile = new HashMap<>();
        String objectType = getObjectRequest.responseContentType();

        try {
            ResponseInputStream<GetObjectResponse> inputStream = s3Client.getObject(getObjectRequest);
            InputStreamResource resource = new InputStreamResource(inputStream);

            processedFile.put("file", resource);
            processedFile.put("object_name", getObjectRequest.key());
            processedFile.put("object_type", objectType);

        } catch (Exception e) {
            logger.severe("Failed to process file: " + getObjectRequest.key() + " - " + e.getMessage());
            result.put("error", "Failed to process file: " + getObjectRequest.key() + " - " + e.getMessage());
            return result;
        }

        result.put("result", processedFile);
        return result;
    }

    public Map<String, Object> getDirectoryList (Map<String, Object> s3){

        logger.info("getDirectoryList() called");

        if(s3 == null){
            logger.severe("S3 configuration is null");
            return Collections.singletonMap("error", "null");
        }

        if(!s3.containsKey("bucket_name") || !s3.containsKey("s3_client") || !s3.containsKey("object_prefix")){
            logger.severe("S3 configuration is missing required fields");
            return Collections.singletonMap("error", "missing fields");
        }

        String bucketName = (String) s3.get("bucket_name");
        String objectPrefix = (String) s3.getOrDefault("object_prefix", null);
        S3Client s3Client = (S3Client) s3.get("s3_client");

        List<Map<String, Object>> objectRequests = new ArrayList<>();

        try{
            ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(objectPrefix)
                    .delimiter("/")
                    .build();

            ListObjectsV2Response response = s3Client.listObjectsV2(listObjectsRequest);

            for (CommonPrefix commonPrefix : response.commonPrefixes()) {
                String key = commonPrefix.prefix();

                // Ensure it's a valid folder
                if (!key.equals(objectPrefix)) {
                    // Extract folder name (strip trailing '/')
                    String folderName = key.substring(objectPrefix.length(), key.length() - 1);

                    // Prepare request map with folder details
                    Map<String, Object> request = new HashMap<>();
                    request.put("folder_name", folderName); // The folder's name
                    request.put("folder_full_path", key);   // The full path of the folder

                    objectRequests.add(request);
                }
            }

            Map<String, Object> result = new HashMap<>();
            result.put("data", objectRequests);
            return result;

        }catch (AmazonServiceException ase) {
            logger.severe("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            logger.severe("Error Message:    " + ase.getMessage());
            logger.severe("HTTP Status Code: " + ase.getStatusCode());
            logger.severe("AWS Error Code:   " + ase.getErrorCode());
            logger.severe("Error Type:       " + ase.getErrorType());
            logger.severe("Request ID:       " + ase.getRequestId());
            return Collections.singletonMap("error", ase.getMessage());
        } catch (AmazonClientException ace) {
            logger.severe("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            logger.severe("Error Message: " + ace.getMessage());
            return Collections.singletonMap("error", ace.getMessage());
        }
    }
}
