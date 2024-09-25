package utility;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class CheckPricesTables {

	public static final String[] PAIRS = { "AUDUSD", "EURUSD", "GBPUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY",
			"AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD", "CADCHF", "CADJPY", "CHFJPY", "EURAUD", "EURCAD", "EURCHF",
			"EURGBP", "EURJPY", "EURNZD", "GBPAUD", "GBPCAD", "GBPCHF", "GBPJPY", "GBPNZD", "NZDCAD", "NZDCHF",
			"NZDJPY" };
	public static final String[] TFRAMES_STR = { "D1", "H1", "M30", "M15", "M5", "M1" };
	public static int[] YEARS = { 2023 };

	public static void main(String[] args) throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:MySQL://localhost:3306/newsfx_db", "root", "555555");
		for (int year : YEARS) {
			for (String tf : TFRAMES_STR) {
				for (String pair : PAIRS) {
					String tbl = pair.toLowerCase() + "_" + tf.toLowerCase() + "_" + year;
					String sql = "SELECT COUNT(*) AS 'n' FROM " + tbl + ";";
					Statement stmt = conn.createStatement();
					ResultSet rs = stmt.executeQuery(sql);
					if (rs.next()) {
						int n = rs.getInt("n");
						if (n > 0) {
							System.out.println(tbl + " >> " + n);
						}
					}
				}
				System.out.println("-------------------------------------");
			}
			System.out.println("-------------------------------------");
		}
	}

}
