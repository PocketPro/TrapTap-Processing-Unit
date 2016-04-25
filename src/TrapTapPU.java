import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class TrapTapPU {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		final double MIN_LAT = 49.0;
		final double MIN_LONG = -123.0;
		final double MAX_LAT = 50.0;
		final double MAX_LONG = -122.0;
		final double dLat = 0.1;
		final double dLong = 0.1;
		
//		ExecutorService executor = Executors.newCachedThreadPool();
		ExecutorService executor = Executors.newFixedThreadPool(5);
		
		long start = System.currentTimeMillis();
		long numFiles = 0;
		
		// Vancouver
		for (double i = MIN_LAT; i < MAX_LAT; i += dLat) {
			for (double j = MIN_LONG; j < MAX_LONG; j += dLong) {
				int tileIndex = tileIndex(i + dLat/2.0,j + dLong/2.0); // use the midpoint for getting tileIndex
				executor.execute(new RunnableProcess(i, j, i+dLat, j+dLong, tileIndex)); // async
				++numFiles;
			}
		}
		
		// Winnipeg
		for (double i = 49.5; i < 50.3; i += dLat) {
			for (double j = -97.7; j < -96.4; j += dLong) {
				int tileIndex = tileIndex(i + dLat/2.0,j + dLong/2.0); // use the midpoint for getting tileIndex
				executor.execute(new RunnableProcess(i, j, i+dLat, j+dLong, tileIndex)); // async
				++numFiles;
			}
		}
		
		// I tend to have trouble with this tile.
//		executor.execute(new RunnableProcess(49.10, -122.80, 49.20, -122.70, 5008171)); // async
		
		
		executor.shutdown();
		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {

		}		
		
		long stop = System.currentTimeMillis();
		long elapsed = stop - start;
		System.out.print("All done! " + numFiles + " files in " + elapsed + "ms.");
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
			System.out.println(elapsed);
		}
	}
	
	public static void process(double minLat, double minLong, double maxLat, double maxLong, int tileIndex)
	{
		String query = TrapTapOSMClient.query(minLat, minLong, maxLat, maxLong);
		String xml = TrapTapOSMClient.executeQuery(query);
		
		int maxRetry = 5;
		for (int i = 0; i < maxRetry; ++i) {
			boolean success = TrapTapS3Client.uploadString(xml, key(minLat, minLong, maxLat, maxLong), tileIndex);
			if (success) {
				break;
			} else if (i < maxRetry - 2) {
				System.out.println("retrying...");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				System.out.println("Failed!");
			}
		}
	}
	
	private static String key(double minLat, double minLong, double maxLat, double maxLong)
	{
		DecimalFormat df = new DecimalFormat("#.00");
		return "mappingFile(" + df.format(minLat) + "," + df.format(minLong) + "),(" + df.format(maxLat) + "," + df.format(maxLong) + ")_" + System.currentTimeMillis() + ".xml";
	}
	
}
