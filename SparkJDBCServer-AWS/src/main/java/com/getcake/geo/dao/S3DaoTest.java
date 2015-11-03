package com.getcake.geo.dao;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetBucketLocationRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3DaoTest {

	private static final Logger logger = Logger.getLogger(S3DaoTest.class);
	private static final String SUFFIX = "/";
	
    private static AWSCredentials getAwsCredentials() throws Exception {
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (C:\\Users\\mnguyen\\.aws\\credentials).
         */
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\mnguyen\\.aws\\credentials), and is in valid format.",
                    e);
        }
        return credentials;
        // kinesis = new AmazonKinesisClient(credentials);
    }

	
    static public String getNewDataVersion(String bucketName, String prefix, Date latestImportDate) throws Exception {
    	Date nextFolderDate, latestFolderDate = null;
    	int year, month, date, hour, minute;
    	String folderName, dataVersion = null;
    	StringTokenizer strTokenizer;
    	Calendar calendar;
    	
    	if (latestImportDate == null) {
    		return null;
    	}
    	
		AWSCredentials credentials = getAwsCredentials (); 
		/*new BasicAWSCredentials(
			"YourAccessKeyID", 
			"YourSecretAccessKey"); */
	
		// create a client connection based on credentials
		AmazonS3 s3client = new AmazonS3Client(credentials);
	
		String delimiter = "/";
        if (!prefix.endsWith(delimiter)) {
            prefix += delimiter;
        }

        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(bucketName).withPrefix(prefix)
                .withDelimiter(delimiter);
        ObjectListing objects = s3client.listObjects(listObjectsRequest);
        List<String> folders = objects.getCommonPrefixes();
        calendar = Calendar.getInstance();
        for (String folder : folders) {
        	System.out.println (folder);
        	folderName = null;
            strTokenizer = new StringTokenizer (folder, "/");
            while (strTokenizer.hasMoreTokens()) {
            	folderName = strTokenizer.nextToken();
            }
        	year = Integer.parseInt(folderName.substring(0, 4));
        	month = Integer.parseInt(folderName.substring(4, 6));
        	date = Integer.parseInt(folderName.substring(6, 8));
        	hour = Integer.parseInt(folderName.substring(8, 10));
        	minute = Integer.parseInt(folderName.substring(10, 12));
        	
        	calendar.set(Calendar.YEAR, year);
        	calendar.set(Calendar.MONTH, month - 1);
        	calendar.set(Calendar.DAY_OF_MONTH, date);
        	calendar.set(Calendar.HOUR_OF_DAY, hour);
        	calendar.set(Calendar.MINUTE, minute);
        	nextFolderDate = calendar.getTime();
        	if (latestFolderDate == null) {
        		latestFolderDate = nextFolderDate;
        	} else if (nextFolderDate.after(latestFolderDate)) {
        		latestFolderDate = nextFolderDate;
        	}
        }
        
        if (latestFolderDate == null || latestImportDate.after(latestFolderDate)) {
        	calendar.setTime(latestImportDate);
        	dataVersion = "" + calendar.get(Calendar.YEAR) + calendar.get(Calendar.MONTH) + calendar.get(Calendar.DAY_OF_MONTH) +        		
                	calendar.get(Calendar.HOUR_OF_DAY) + calendar.get(Calendar.MINUTE); 
        }
        logger.debug("dataVersion:" + dataVersion);
        return dataVersion;
    }
    
    static public List<String> listKeysInDirectory(String bucketName, String prefix) throws Exception {

		AWSCredentials credentials = getAwsCredentials (); 
	// create a client connection based on credentials
		AmazonS3 s3client = new AmazonS3Client(credentials);
	
		String delimiter = "/";
        if (!prefix.endsWith(delimiter)) {
            prefix += delimiter;
        }

        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(bucketName).withPrefix(prefix)
                .withDelimiter(delimiter);
        ObjectListing objects = s3client.listObjects(listObjectsRequest);
        List<String> folders = objects.getCommonPrefixes();
        return folders;
    }
    
	public static void main(String[] args) {
		
		try {
			
			// credentials object identifying user for authentication
			// user must have AWSConnector and AmazonS3FullAccess for 
			// this example to work
			AWSCredentials credentials = getAwsCredentials (); 
				/*new BasicAWSCredentials(
					"YourAccessKeyID", 
					"YourSecretAccessKey"); */
			
			// create a client connection based on credentials
			AmazonS3 s3client = new AmazonS3Client(credentials);
			
			// create bucket - name must be unique for all S3 users
			String bucketName = "cake-qa-deployment-artifacts-us-west-2";
			// listKeysInDirectory (bucketName, "ipdata/");
			
			Date latestImportDate = new Date ();
			getNewDataVersion (bucketName, "ipdata/", latestImportDate);
			
			
            // Get location.
            String bucketLocation = s3client.getBucketLocation(new GetBucketLocationRequest(bucketName));
            System.out.println("bucket location = " + bucketLocation);
            
			// s3client.createBucket(bucketName);
			// s3client.
			
			// list buckets
            /*
			for (Bucket bucket : s3client.listBuckets()) {
				System.out.println(" - " + bucket.getName());
			}
			*/
            
			String folderName = "ipdata";
			/*
			// create folder into bucket
			createFolder(bucketName, folderName, s3client);
			
			// upload file to folder and set it to public
			String fileName = folderName + SUFFIX + "ipv6_1_2.csv";
			PutObjectResult putObjResult = s3client.putObject(new PutObjectRequest(bucketName, fileName, 
					new File("C:/Programs/IP-Ext-Import/data/ipv6_1_2.csv"))
					.withCannedAcl(CannedAccessControlList.BucketOwnerFullControl));
			// putObjResult.g
			*/
			
			ListObjectsRequest lor = new ListObjectsRequest().withBucketName(bucketName)
                    .withPrefix("ipdata/")
                    .withDelimiter("/");
			
			
			ObjectListing objectListing = s3client.listObjects(lor);
			for (S3ObjectSummary summary: objectListing.getObjectSummaries()) {
			    System.out.println(summary.getKey());
			}
			
			
			S3Object s3Obj = s3client.getObject(new GetObjectRequest(bucketName, folderName + SUFFIX + "201512010000/cities_201512010000.csv"));
			System.out.println("s3Obj.getKey(): " + s3Obj.getKey());
			
			 s3Obj = s3client.getObject(new GetObjectRequest(bucketName, folderName + SUFFIX + "201512010000"));
			System.out.println("s3Obj.getKey(): " + s3Obj.getKey());
				
			s3Obj = s3client.getObject(new GetObjectRequest(bucketName, folderName));
			System.out.println("s3Obj.getKey(): " + s3Obj.getKey());
			displayTextInputStream (s3Obj.getObjectContent());
			
			s3Obj = s3client.getObject(new GetObjectRequest(bucketName, folderName + SUFFIX + "ipv6_1_2.csv"));
			System.out.println("s3Obj.getKey(): " + s3Obj.getKey());
			displayTextInputStream (s3Obj.getObjectContent());
			// deleteFolder(bucketName, folderName, s3client);
			
			// deletes bucket
			// s3client.deleteBucket(bucketName);
		} catch (Throwable exc) {
			exc.printStackTrace();
		}
		
	}
	
    private static void displayTextInputStream(InputStream input)
    throws IOException {
    	// Read one text line at a time and display.
        BufferedReader reader = new BufferedReader(new 
        		InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            System.out.println("    " + line);
        }
        System.out.println();
    }	
	public static void createFolder(String bucketName, String folderName, AmazonS3 client) {
		// create meta-data for your folder and set content-length to 0
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(0);
		// create empty content
		InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
		// create a PutObjectRequest passing the folder name suffixed by /
		PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName,
				folderName + SUFFIX, emptyContent, metadata);
		// send request to S3 to create folder
		client.putObject(putObjectRequest);
	}
	
	/**
	 * This method first deletes all the files in given folder and than the
	 * folder itself
	 */
	public static void deleteFolder(String bucketName, String folderName, AmazonS3 client) {
		List<S3ObjectSummary> fileList = 
				client.listObjects(bucketName, folderName).getObjectSummaries();
		for (S3ObjectSummary file : fileList) {
			client.deleteObject(bucketName, file.getKey());
		}
		client.deleteObject(bucketName, folderName);
	}	
}
