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
				
		if (tileIndex(49.1044, -122.8011) != 5008171) {
			System.out.println("Failed tileIndex test");
		}
		
		ExecutorService executor = Executors.newCachedThreadPool();
//		executor.execute(new RunnableProcess(49.1, -122.9, 49.2, -122.8, 5008171));
		
		for (double i = MIN_LAT; i < MAX_LAT; i += dLat) {
			for (double j = MIN_LONG; j < MAX_LONG; j += dLong) {
				int tileIndex = tileIndex(i,j);
				executor.execute(new RunnableProcess(i, j, i+dLat, j+dLong, tileIndex)); // async
			}
		}
		
		executor.shutdown();
		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {

		}		

		System.out.print("All done!");
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
		TrapTapS3Client.uploadString(xml, key(minLat, minLong, maxLat, maxLong), tileIndex);
	}
	
	private static String key(double minLat, double minLong, double maxLat, double maxLong)
	{
		DecimalFormat df = new DecimalFormat("#.00");
		return "mappingFile(" + df.format(minLat) + "," + df.format(minLong) + "),(" + df.format(maxLat) + "," + df.format(maxLong) + ")_" + System.currentTimeMillis() + ".xml";
	}
	
}
