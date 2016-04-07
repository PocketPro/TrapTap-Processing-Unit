import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;


public class TrapTapLocalExporter {

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
		
		for (double i = MIN_LAT; i < MAX_LAT; i += dLat) {
			for (double j = MIN_LONG; j < MAX_LONG; j += dLong) {
				
				int tileIndex = TrapTapPU.tileIndex(i,j);
				process(i, j, i+dLat, j+dLong, tileIndex);
				
//				int tileIndex = TrapTapPU.tileIndex(i,j);
//				
//				String query = TrapTapOSMClient.query(i, j, i+dLat, j+dLong);
//				String xml = TrapTapOSMClient.executeQuery(query);
//				
//				BufferedWriter writer = null;
//				File file = null;
//		        try {
//		            //create a temporary file
//		            file = new File("TrapTapPU_" + tileIndex + ".xml");
//		            GZIPOutputStream zip = new GZIPOutputStream(new FileOutputStream(file));
//		            writer = new BufferedWriter(new OutputStreamWriter(zip, "UTF-8"));
//		            writer.write(xml);
//		        } catch (Exception e) {
//		            e.printStackTrace();
//		        } finally {
//		            try {
//		                // Close the writer regardless of what happens...
//		                writer.close();
//		            } catch (Exception e) {
//		            	e.printStackTrace();
//		            }
//		        }		
				
//				BufferedWriter writer = null;
//				File file = null;
//		        try {
//		            //create a temporary file
//		            file = new File("/Users/eytanmoudahi/TrapTapPU_" + tileIndex + ".xml");
//		
//		            writer = new BufferedWriter(new FileWriter(file));
//		            writer.write(xml);
//		        } catch (Exception e) {
//		            e.printStackTrace();
//		        } finally {
//		            try {
//		                // Close the writer regardless of what happens...
//		                writer.close();
//		            } catch (Exception e) {
//		            }
//		        }		
				
			}
		}
	}
	
	public static void process(double minLat, double minLong, double maxLat, double maxLong, int tileIndex)
	{
		String query = TrapTapOSMClient.query(minLat, minLong, maxLat, maxLong);
		String xml = TrapTapOSMClient.executeQuery(query);
		
		BufferedWriter writer = null;
		File file = null;
        try {
            //create a temporary file
            file = new File("/Users/eytanmoudahi/TrapTapPU_" + tileIndex + ".xml.gz");
            GZIPOutputStream zip = new GZIPOutputStream(new FileOutputStream(file));
            writer = new BufferedWriter(new OutputStreamWriter(zip, "UTF-8"));
            writer.write(xml);
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
	}

}
