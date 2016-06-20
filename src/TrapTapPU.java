import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class TrapTapPU {

	private static final double dLat = 0.1;
	private static final double dLong = 0.1;
	private static long start = 0;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		// Print system out to file.
		PrintStream out;
		try {
			out = new PrintStream(new FileOutputStream("traptap_test.txt", true));
			System.setOut(out);
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		// Choose whether you want to used a fixed thread pool or a cached thread pool.
//		ExecutorService executor = Executors.newCachedThreadPool();
		ExecutorService executor = Executors.newFixedThreadPool(5);
		
		start = System.currentTimeMillis();
		long numFiles = 0;
		
		// Vancouver
//		numFiles += processVancouver(executor);
		
		// Winnipeg
//		numFiles += processWinnipeg(executor);
		
		// Cupertino
//		numFiles += processCupertino(executor);
		
		// Process World
		numFiles += processWorld(executor, -90, -180);
		
		// Resume from tile
//		numFiles += processWorld(executor, 1106715)
		
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
					// TODO Auto-generated catch block
					e.printStackTrace();
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
	
}
