

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class TrapTapS3Client {

	final static String BUCKET_NAME = "traptap";
	
	private static boolean upload(PutObjectRequest request) {
    	
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (~/.aws/credentials), and is in valid format.",
                    e);
        }

        AmazonS3 s3 = new AmazonS3Client(credentials);
        Region region = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(region);
        
        try {
        	
            /*
             * Upload an object to your bucket - You can easily upload a file to
             * S3, or upload directly an InputStream if you know the length of
             * the data in the stream. You can also specify your own metadata
             * when uploading to S3, which allows you set a variety of options
             * like content-type and content-encoding, plus additional metadata
             * specific to your applications.
             */
//            System.out.println("Uploading a new object to S3 from a file");
            s3.putObject(request);
            
//            String resourceUrl = "https://s3.amazonaws.com/" + request.getBucketName() + "/" + request.getKey();
//            System.out.println("Uploaded file available at URL: " + resourceUrl);
            return true;
            

        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
            System.out.println("Tile Index:       " + request.getMetadata().getUserMetaDataOf("tile-index"));
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
            System.out.println("Tile Index:       " + request.getMetadata().getUserMetaDataOf("tile-index"));
        } catch (Exception e) {
        	e.printStackTrace();
        } catch (Throwable e) {
        	e.printStackTrace();
        }
        
        return false;
    }
	
	public static boolean uploadString(String contents, String key, int tileIndex) {
		
		assert contents != null;
		assert key != null;
				
		BufferedWriter writer = null;
		File file = null;
        try {
            //create a temporary file
            file = new File("TrapTapPU_" + tileIndex + ".xml");
            GZIPOutputStream zip = new GZIPOutputStream(new FileOutputStream(file));
            writer = new BufferedWriter(new OutputStreamWriter(zip, "UTF-8"));
            writer.write(contents);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                // Close the writer regardless of what happens...
                writer.close();
            } catch (Exception e) {
            	e.printStackTrace();
            }
        }		
		
		ObjectMetadata metaData = new ObjectMetadata();
		metaData.addUserMetadata("tile-index", String.valueOf(tileIndex));
		metaData.setContentEncoding("gzip");
		
		PutObjectRequest request = new PutObjectRequest(BUCKET_NAME, key + ".gz", file);
		request.setMetadata(metaData);
		
		boolean success = upload(request);
		
		file.delete();
		
		return success;
		
		/*
		byte[] bytes = contents.getBytes();
		ByteArrayInputStream stream = new ByteArrayInputStream(contents.getBytes());
		
		ObjectMetadata metaData = new ObjectMetadata();
		metaData.addUserMetadata("tile-index", String.valueOf(tileIndex));
		metaData.setContentLength(bytes.length);
				
		return upload(new PutObjectRequest(BUCKET_NAME, key, stream, metaData));
		*/
	}
	
//    public static String uploadFile(String filePath, String key) {
//    	try {
//    		File file = new File(filePath);
//    		return upload(new PutObjectRequest(BUCKET_NAME, key, file));
//    	}
//    	catch (NullPointerException e) {
//    		System.out.println("No file to upload");
//    		return null;
//    	}
//    }
}
