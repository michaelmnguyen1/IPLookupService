/*
 * Michael M. Nguyen
 */
package com.getcake.geo.dao;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;

public class S3Dao {

	private static final Logger logger = Logger.getLogger(S3Dao.class);
	private static final long MILLISEC_PER_MINUTE = 1000 * 60;
	private static final int S3_WRITE_MAX_RETRY = 3;

	private static S3Dao instance;

	static {
		try {
			instance = new S3Dao();
		} catch (Throwable exc) {
			logger.error("", exc);
		}
	}

	public static S3Dao getInstance() {
		return instance;
	}


    private AWSCredentials getAwsCredentials(boolean instanceProfileFlag)  {
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (C:\\Users\\mnguyen\\.aws\\credentials).
         */
        AWSCredentials credentials = null;
        try {
        	if (instanceProfileFlag) {
                credentials = new InstanceProfileCredentialsProvider().getCredentials();

        	} else {
                credentials = new ProfileCredentialsProvider("default").getCredentials();
        	}
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\mnguyen\\.aws\\credentials), and is in valid format.",
                    e);
        }
        return credentials;
    }

	public String writeS3TextInputStream (String srcFileName, String bucketName, String bucketFolder, String fileName) throws IOException {
		AWSCredentials credentials;
		String respMsg = null;
    	long startTime, durationMillisec;
    	int retryCount;
		File srcFile;
	    AmazonS3 s3client;

	    retryCount = 1;
	    while (retryCount <= S3_WRITE_MAX_RETRY)
	    {
		    try {
				logger.debug("retryCount: " + retryCount + " Uploading to " + bucketName + " - bucketFolder:" + bucketFolder + " - fileName: " + fileName);
				credentials = getAwsCredentials (false);
				s3client = new AmazonS3Client(credentials);
				startTime = Calendar.getInstance().getTimeInMillis();

				srcFile = new File (srcFileName);
				s3client.putObject(bucketName, bucketFolder + "/" + fileName, srcFile);

				durationMillisec = Calendar.getInstance().getTimeInMillis() - startTime;
				respMsg = "Uploaded to " + bucketName + " - bucketFolder:" + bucketFolder + " - fileName: " + fileName + " - dur(ms):" + durationMillisec +
						" - dur(min):" + (float)((float)durationMillisec / (float)MILLISEC_PER_MINUTE);
				logger.debug(respMsg);
				break;
		    } catch (Throwable exc) {
				logger.error("Error writing to S3 retryCount: " + retryCount + " Uploading to " + bucketName + " - bucketFolder:" + bucketFolder + " - fileName: " + fileName);
		    	logger.error("", exc);
		    	retryCount++;
		    	if (retryCount > S3_WRITE_MAX_RETRY) {
					logger.error("Error writing to S3 retryCount: " + retryCount + " Uploading to " + bucketName + " - bucketFolder:" + bucketFolder + " - fileName: " + fileName);
					logger.error("Exceeded max retry attempts of " +  S3_WRITE_MAX_RETRY, exc);
					throw exc;
		    	}
		    }

	    }
		return respMsg;
    }

    public String [] getNewDataVersion(String bucketName, String prefix, Date latestMssqlImportDate) throws Exception {
		AWSCredentials credentials;
		AmazonS3 s3client;
		long currS3ExportedVersion, latestS3ExportedVersion = -1, latestMssqlImportVersion;
    	int month, date, hour, minute;
    	String folderName;
    	String ouputs[];
    	StringTokenizer strTokenizer;
    	Calendar calendar;
    	StringBuilder newIpVerStrB;

    	ouputs = new String [2];
		credentials = getAwsCredentials (false);
		s3client = new AmazonS3Client(credentials);

		String delimiter = "/";
        if (!prefix.endsWith(delimiter)) {
            prefix += delimiter;
        }

        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(bucketName).withPrefix(prefix)
                .withDelimiter(delimiter);
        ObjectListing objects = s3client.listObjects(listObjectsRequest);
        List<String> exporedS3DateVersions = objects.getCommonPrefixes();
        calendar = Calendar.getInstance();
        for (String exporedS3DateVersion : exporedS3DateVersions) {
        	logger.debug("exporedS3DateVersion: " + exporedS3DateVersion);
        	folderName = null;
            strTokenizer = new StringTokenizer (exporedS3DateVersion, "/");
            while (strTokenizer.hasMoreTokens()) {
            	folderName = strTokenizer.nextToken();
            }
            /*
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
        	if (latestFolderDate == null || nextFolderDate.after(latestFolderDate)) {
        		latestFolderDate = nextFolderDate;
        		ouputs[0] = folderName;
        	}
        	*/
            try {
                currS3ExportedVersion = Long.parseLong(folderName);
            	if (currS3ExportedVersion > latestS3ExportedVersion) {
            		latestS3ExportedVersion = currS3ExportedVersion;
            		ouputs[0] = folderName;
            	}
            } catch (NumberFormatException exc) {
            	logger.error("folderName " + folderName + " does not match expected format.  Skipped it", exc);
            }
        }

    	if (latestMssqlImportDate == null) {
    		return ouputs;
    	}

    	newIpVerStrB = new StringBuilder ();
    	calendar.setTime(latestMssqlImportDate);
    	newIpVerStrB.append(calendar.get(Calendar.YEAR));
    	month = calendar.get(Calendar.MONTH) + 1;
    	if (month >= 10) {
        	newIpVerStrB.append(month);
    	} else {
        	newIpVerStrB.append('0');
        	newIpVerStrB.append(month);
    	}

    	date = calendar.get(Calendar.DAY_OF_MONTH);
    	if (date >= 10) {
        	newIpVerStrB.append(date);
    	} else {
        	newIpVerStrB.append('0');
        	newIpVerStrB.append(date);
    	}

    	hour = calendar.get(Calendar.HOUR_OF_DAY);
    	if (hour >= 10) {
        	newIpVerStrB.append(hour);
    	} else {
        	newIpVerStrB.append('0');
        	newIpVerStrB.append(hour);
    	}

    	minute = calendar.get(Calendar.MINUTE);
    	if (hour >= 10) {
        	newIpVerStrB.append(minute);
    	} else {
        	newIpVerStrB.append('0');
        	newIpVerStrB.append(minute);
    	}
    	latestMssqlImportVersion = Long.parseLong(newIpVerStrB.toString());

    	if (latestMssqlImportVersion > latestS3ExportedVersion) {
        	ouputs[1] = newIpVerStrB.toString();
        }
        logger.debug("Latest S3 Export Data Version:" + ouputs[0]);
        logger.debug("New Data Version for export:" + ouputs[1]);
        return ouputs;
    }


}
