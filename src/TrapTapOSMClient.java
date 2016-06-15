import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

/**
 * Use this class to run queries on an OSM databse on the local machine.
 * @author eytanmoudahi
 *
 */
public class TrapTapOSMClient {

	public static String executeQuery(String query) {
		
		String retValue = null;
		
		final String dbDir = "/Users/eytanmoudahi/Downloads/Overpass-API-test754_osx-2/src/build/db";
		
		try {
			ProcessBuilder builder = new ProcessBuilder("/Users/eytanmoudahi/overpass/bin/osm3s_query", "--db-dir=" + dbDir);
			builder.redirectErrorStream(true);
			Process process = builder.start();
						
			InputStream is = process.getInputStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(is));

			// Execute Query
			process.getOutputStream().write(query.getBytes(Charset.forName("UTF-8")));
			process.getOutputStream().close();			
			
			// Read Data
			StringBuilder xmlBuilder = new StringBuilder(1024*1024*7);
			String line = null;
			while ((line = reader.readLine()) != null) {
				xmlBuilder.append(line);
				xmlBuilder.append("\n");
			}
			
			// remove the final \n
			xmlBuilder.deleteCharAt(xmlBuilder.length()-1); 
			
			// remove the preamble
			int endIndex = xmlBuilder.indexOf("<");
			if (endIndex == -1) {
				// TODO: Figure out why there was no preamble...
			} else {
				xmlBuilder.replace(0, endIndex, "");
			}
			
			String xml = xmlBuilder.toString();
			retValue = xml;
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return retValue;
	}
	
	public static String query(double minLat, double minLong, double maxLat, double maxLong) {
		String query = "(way[\"highway\"][\"highway\" != \"pedestrian\"][\"highway\" != \"path\"][\"highway\" != \"footway\"][\"highway\" != \"cycleway\"][\"highway\" != \"bus_guideway\"][\"highway\" != \"bridleway\"][\"highway\" != \"steps\"][\"highway\" != \"escape\"][\"highway\" != \"User Defined\"](" + minLat + "," + minLong + "," + maxLat + "," + maxLong + ");>;);out body;";
		return query;
	}
	
	public static String comboQuery(double minLat, double minLong, double maxLat, double maxLong) {
		
		String boundingBox = "(" + minLat + "," + minLong + "," + maxLat + "," + maxLong + ")";
		
		String highwayQuery = "way[\"highway\"][\"highway\" != \"pedestrian\"][\"highway\" != \"path\"][\"highway\" != \"footway\"][\"highway\" != \"cycleway\"][\"highway\" != \"bus_guideway\"][\"highway\" != \"bridleway\"][\"highway\" != \"steps\"][\"highway\" != \"escape\"][\"highway\" != \"User Defined\"]" + boundingBox + ";>;";
		String wayQuery = "way[\"amenity\"=\"school\"]" + boundingBox + ";>;";
		String relQuery = "rel[\"amenity\"=\"school\"]" + boundingBox + ";>;";
		String nodeQuery = "node[\"amenity\"=\"school\"]" + boundingBox + ";";
		
		String query = "(" + highwayQuery + wayQuery + relQuery + nodeQuery + ");out body;";
		return query;
	}
	
}
