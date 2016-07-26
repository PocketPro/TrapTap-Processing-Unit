import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HttpsURLConnection;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;


public class TrapTapPU {

	private static final double dLat = 0.1;
	private static final double dLong = 0.1;
	private static long start = 0;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		// Print system out to file.
//		PrintStream out;
//		try {
//			out = new PrintStream(new FileOutputStream("traptap_test.txt", true));
//			System.setOut(out);
//		} catch (FileNotFoundException e1) {
//			System.out.print(stringFromException(e1));
//		}
		
		// Choose whether you want to used a fixed thread pool or a cached thread pool.
		ExecutorService executor = Executors.newFixedThreadPool(30);
		
		start = System.currentTimeMillis();
		long numFiles = 0;
		
		// Vancouver
		numFiles += processVancouver(executor);
		
		// Winnipeg
//		numFiles += processWinnipeg(executor);
		
		// Cupertino
//		numFiles += processCupertino(executor);
		
		// Process World
//		numFiles += processWorld(executor, -90, -180);
		
		// Resume from tile
//		numFiles += processWorld(executor, 4633070);
		
		// Process from a list
//		numFiles += processList(executor, new ArrayList<Integer>(Arrays.asList(new Integer[]{2000000})));
		
		// Process missing files
//		numFiles += processMissingFilesInDirectory(executor, "/Users/eytanmoudahi/Downloads/MissingFiles");
		
		// Check each record in dynamodb
//		numFiles += processForbiddenURLsInDirectory(executor, "/Users/eytanmoudahi/Downloads/MissingFiles");
		
//		executor.execute(new HeadRunnableProcess("https://s3.amazonaws.com/traptap/1466022572_mappingFile(37.4,-122.3),(37.5,-122.2).xml.gz", 4586977));
		
		
		executor.shutdown();
		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {

		}		
		
		long stop = System.currentTimeMillis();
		long elapsed = stop - start;
		System.out.print("All done! " + numFiles + " files in " + elapsed + "ms.");
		
	}
	
	public static int processWinnipeg(ExecutorService executor) {
		int numFiles = 0;
		for (double i = 49.5; i < 50.3; i += dLat) {
			for (double j = -97.7; j < -96.4; j += dLong) {
				int tileIndex = tileIndex(i + dLat/2.0,j + dLong/2.0); // use the midpoint for getting tileIndex
				executor.execute(new RunnableProcess(i, j, i+dLat, j+dLong, tileIndex)); // async
				++numFiles;
			}
		}
		return numFiles;
	}
	
	public static int processVancouver(ExecutorService executor) {
		int numFiles = 0;
		for (double i = 48.0; i < 51.0; i += dLat) {
			for (double j = -124.0; j < -121.0; j += dLong) {
				int tileIndex = tileIndex(i + dLat/2.0,j + dLong/2.0); // use the midpoint for getting tileIndex
				executor.execute(new RunnableProcess(i, j, i+dLat, j+dLong, tileIndex)); // async
				++numFiles;
			}
		}		
		return numFiles;
	}
	
	public static int processCupertino(ExecutorService executor) {
		int numFiles = 0;
		for (double i = 37.0; i < 38.0; i += dLat) {
			for (double j = -123; j < -122; j += dLong) {
				int tileIndex = tileIndex(i + dLat/2.0,j + dLong/2.0); // use the midpoint for getting tileIndex
				executor.execute(new RunnableProcess(i, j, i+dLat, j+dLong, tileIndex)); // async
				++numFiles;
			}
		}
		return numFiles;
	}
	
	public static int processWorld(ExecutorService executor, int startTileIndex) {
		double startLat = -90.0 + Math.floor(startTileIndex / 3600) * 0.1;
		double startLong = -180.0 + (startTileIndex - Math.floor(startTileIndex / 3600)*3600)*0.1; 
		return processWorld(executor, startLat, startLong);
	}
	
	public static int processList(ExecutorService executor, ArrayList<Integer> array) {
		int numFiles = 0;
		for (Integer index : array) {
			double latitude = -90.0 + Math.floor(index / 3600) * 0.1;
			double longitude = -180.0 + (index - Math.floor(index / 3600)*3600)*0.1;
			executor.execute(new RunnableProcess(latitude, longitude, latitude+dLat, longitude+dLong, index)); // async
			++numFiles;
		}
		return numFiles;
	}
	
	public static int processWorld(ExecutorService executor, double startLat, double startLong) {
		int numFiles = 0;
		for (double i = startLat; i < 90.0; i += dLat) {
			for (double j = startLong; j < 180.0; j += dLong) {
				int tileIndex = tileIndex(i + dLat/2.0,j + dLong/2.0); // use the midpoint for getting tileIndex
				executor.execute(new RunnableProcess(i, j, i+dLat, j+dLong, tileIndex)); // async
				++numFiles;
			}
		}
		return numFiles;
	}
	
	public static void missingFiles2() {
		
		AmazonDynamoDBClient client = new AmazonDynamoDBClient(
			    new ProfileCredentialsProvider());
		
		List<Map<String, AttributeValue>> keys = new ArrayList<Map<String, AttributeValue>>();
		keys.add(batchItemKey("0"));
		keys.add(batchItemKey("1"));
		
		Collection<String> attributesToGet = new ArrayList<String>();
		attributesToGet.add("tileIndex");
		
		KeysAndAttributes keysAndAttributes = new KeysAndAttributes()
				.withKeys(keys)
				.withAttributesToGet(attributesToGet);
		
		Map<String, KeysAndAttributes> requestItems = new HashMap<String,KeysAndAttributes>();
		requestItems.put("mapping_files", keysAndAttributes);
		
		BatchGetItemRequest batch = new BatchGetItemRequest().withRequestItems(requestItems);
		BatchGetItemResult bResult = client.batchGetItem(batch);
		System.out.println(bResult.getResponses());
		
	}
	
	/**
	 * This function reads the contents of directory and uploads the files 
	 * missing. It's assumed the contents of the directory are the exported data
	 * from dynamoDB. 
	 * @param executor
	 * @param directory
	 * @return
	 */
	public static int processMissingFilesInDirectory(ExecutorService executor, String directory) {
		
		HashMap<Integer, Boolean> availableFiles = new HashMap<Integer, Boolean>(1801*3601);
		
		final File baseDirectory = new File(directory);
		
	    for (final File file : baseDirectory.listFiles()) {
	        if (file.isFile()) {      	
	        	System.out.println("reading file " + file.getName());
	        	try {
	        		BufferedReader br = new BufferedReader(new FileReader(file));
	        	    for(String line; (line = br.readLine()) != null; ) {
	        	    	JSONObject obj = new JSONObject(line);
	        	    	String tileIndexString = obj.getJSONObject("tileIndex").getString("n");
	        	    	Integer tileIndex = Integer.parseInt(tileIndexString);
	        	    	availableFiles.put(tileIndex, true);
	        	    }
	        	    br.close();
	        	} catch(IOException e) {
	        		System.out.print(stringFromException(e));
	        	} catch (JSONException e) {
	        		System.out.print(stringFromException(e));
				}
	        }
	    }
	    
	    assert availableFiles.get(209963) == true;
	    
	    int numFiles = 0;
	    int startingIndex = 0;
	    for (int index = startingIndex; index < 3600*1800; ++index) {
	    	if (availableFiles.get(index) == null) {
	    		double latitude = -90.0 + Math.floor(index / 3600) * 0.1;
				double longitude = -180.0 + (index - Math.floor(index / 3600)*3600)*0.1;
				executor.execute(new RunnableProcess(latitude, longitude, latitude+dLat, longitude+dLong, index)); // async
				++numFiles;
	    	}
	    }
	    return numFiles;
	}
	
	/**
	 * This function reads the contents of directory and for each listing checks
	 * to HEAD of the URL to make sure its contents are accessible
	 * @param executor
	 * @param directory
	 * @return
	 */
	public static int processForbiddenURLsInDirectory(ExecutorService executor, String directory) {
		
		HashMap<Integer, String> urls = new HashMap<Integer, String>(1801*3601);
		
		final File baseDirectory = new File(directory);
		
	    for (final File file : baseDirectory.listFiles()) {
	        if (file.isFile()) {      	
	        	System.out.println("reading file " + file.getName());
	        	try {
	        		BufferedReader br = new BufferedReader(new FileReader(file));
	        	    for(String line; (line = br.readLine()) != null; ) {
	        	    	JSONObject obj = new JSONObject(line);
	        	    	String tileIndexString = obj.getJSONObject("tileIndex").getString("n");
	        	    	Integer tileIndex = Integer.parseInt(tileIndexString);
	        	    	String url = obj.getJSONObject("url").getString("s");
	        	    	urls.put(tileIndex, url);
	        	    }
	        	    br.close();
	        	} catch(IOException e) {
	        		System.out.print(stringFromException(e));
	        	} catch (JSONException e) {
	        		System.out.print(stringFromException(e));
				}
	        }
	    }
	    
	    int numFiles = 0;
	    int startingIndex = 96896;
	    for (int index = startingIndex; index < 3600*1800; ++index) {
	    	String url = urls.get(index);
	    	if (url != null) {
	    		executor.execute(new HeadRunnableProcess(url,  index));
	    		numFiles++;
	    	}
	    }
	    return numFiles;
	}	
	
	public static class HeadRunnableProcess implements Runnable {
		
		private String url;
		private int tileIndex;
		
		public HeadRunnableProcess(String url, int tileIndex) {
			this.tileIndex = tileIndex;
			this.url = url;
		}
		
		public void run() {
			boolean shouldBreak = false;
			for (int i = 0; i<3 && shouldBreak == false; ++i) {
				int responseCode = getResponseCode(url);
				
				switch (responseCode) {
				case HttpsURLConnection.HTTP_OK:
					System.out.println(tileIndex + ": OK");
					shouldBreak = true;
					break;
				case HttpsURLConnection.HTTP_FORBIDDEN:
					System.out.println(tileIndex + ": FORBIDDEN. Processing...");
					double latitude = -90.0 + Math.floor(tileIndex / 3600) * 0.1;
					double longitude = -180.0 + (tileIndex - Math.floor(tileIndex / 3600)*3600)*0.1;
					process(latitude, longitude, latitude+dLat, longitude+dLong, tileIndex);
					shouldBreak = true;
					break;
				case HttpsURLConnection.HTTP_INTERNAL_ERROR:
					break;
				default:
					System.out.println(tileIndex + ": Unexpected response code " + responseCode);
					shouldBreak = true;
					break;
				}

			}
		}

		private int getResponseCode(String url) {
			try {
				HttpsURLConnection.setFollowRedirects(false);
				HttpsURLConnection connection;
				connection = (HttpsURLConnection) new URL(url).openConnection();
				connection.setRequestMethod("HEAD");
				int responseCode = connection.getResponseCode();
				return responseCode;
			} catch (IOException e) {
				System.out.print(stringFromException(e));
				return -1;
			}
			
		}
	}
	
	public static Map<String, AttributeValue> batchItemKey(String tileIndex) {
		Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		key.put("tileIndex", new AttributeValue().withN(tileIndex));
		return key;
	}
	
	public static int tileIndex(double latitude, double longitude){
		
		final double MIN_LAT = -90.0;
		final double MAX_LAT = 90.0;
		final double MIN_LONG = -180;
		final double MAX_LONG = 180;
		final double d = 0.1;
				
		assert (MIN_LAT <= latitude && latitude <= MAX_LAT);
		assert (MIN_LONG <= longitude && longitude <= MAX_LONG);
				
		int columnIndex = BigDecimal.valueOf(longitude).subtract(BigDecimal.valueOf(MIN_LONG)).divide(BigDecimal.valueOf(d)).intValue();
		int rowIndex = BigDecimal.valueOf(latitude).subtract(BigDecimal.valueOf(MIN_LAT)).divide(BigDecimal.valueOf(d)).intValue();
		
		int tileIndex = 3600 * rowIndex + columnIndex;
		
		return tileIndex;
	}
	
	public static class RunnableProcess implements Runnable {
		private double minLat;
		private double minLong;
		private double maxLat; 
		private double maxLong;
		private int tileIndex;

		public RunnableProcess(double minLat, double minLong, double maxLat, double maxLong, int tileIndex) {
			this.minLat = minLat;
			this.minLong = minLong;
			this.maxLat = maxLat;
			this.maxLong = maxLong;
			this.tileIndex = tileIndex;
		}

		public void run() {
			long start = System.currentTimeMillis();
			process(minLat, minLong, maxLat, maxLong, tileIndex);
			long stop = System.currentTimeMillis();
			long elapsed = stop - start;
			System.out.println(tileIndex + ": " + elapsed + "ms");
		}
	}
	
	public static void process(double minLat, double minLong, double maxLat, double maxLong, int tileIndex)
	{
		String query = TrapTapOSMClient.comboQuery(minLat, minLong, maxLat, maxLong);
		String xml = TrapTapOSMClient.executeQuery(query);
		
		int maxRetry = 5;
		for (int i = 0; i < maxRetry; ++i) {
			boolean success = TrapTapS3Client.uploadString(xml, key(minLat, minLong, maxLat, maxLong), tileIndex);
			if (success) {
				break;
			} else if (i < maxRetry - 2) {
//				System.out.println(tileIndex + ": attempt " + i + " failed. Retrying...");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					System.out.print(stringFromException(e));
				}
			} else {
				System.out.println(tileIndex + ": Failed!");
			}
		}
	}
	
	private static String key(double minLat, double minLong, double maxLat, double maxLong)
	{
		DecimalFormat df = new DecimalFormat("#.0");
		String time = Long.toString(start/1000);
		return time + "_mappingFile(" + df.format(minLat) + "," + df.format(minLong) + "),(" + df.format(maxLat) + "," + df.format(maxLong) + ").xml";
	}
	
	private static String stringFromException(Exception e) {
		StringWriter errors = new StringWriter();
		e.printStackTrace(new PrintWriter(errors));
		return errors.toString();
	}
}
