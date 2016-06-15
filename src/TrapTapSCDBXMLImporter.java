
/**
 * This class isn't really required anymore because the full SCDB database is 
 * only a couple megabytes. It may be needed in the future... 
 * @author eytanmoudahi
 *
 */
public class TrapTapSCDBXMLImporter {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String query = TrapTapOSMClient.comboQuery(49.1, -122.7, 49.2, -122.6); // Walnut Grove SS
		System.out.println("Executing Query: " + query);
		String result = TrapTapOSMClient.executeQuery(query);
		System.out.println("====================");
		System.out.println(result);
		
	
		TrapTapLocalExporter.writeToFile(result, "comboQuery");	
	}
	
	

}
