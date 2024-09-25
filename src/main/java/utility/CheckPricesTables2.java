package utility;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class CheckPricesTables2 {

	public static final String[] PAIRS = { "AUDUSD", "EURUSD", "GBPUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY",
			"AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD", "CADCHF", "CADJPY", "CHFJPY", "EURAUD", "EURCAD", "EURCHF",
			"EURGBP", "EURJPY", "EURNZD", "GBPAUD", "GBPCAD", "GBPCHF", "GBPJPY", "GBPNZD", "NZDCAD", "NZDCHF",
			"NZDJPY" };
	public static final String[] TFRAMES_STR = { "M1" };
//	public static final String[] TFRAMES_STR = { "D1", "H1", "M30", "M15", "M5", "M1" };
	public static int[] YEARS = { 2023, 2022, 2021, 2020 };

	public static void main(String[] args) throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:MySQL://localhost:3306/newsfx_db", "root", "555555");
		for (String tf : TFRAMES_STR) {
			for (String pair : PAIRS) {
				for (int year : YEARS) {
					String tbl = pair.toLowerCase() + "_" + tf.toLowerCase() + "_" + year;
					String countSql = "SELECT COUNT(*) AS 'n' FROM " + tbl + ";";
					Statement countStm = conn.createStatement();
					ResultSet countRs = countStm.executeQuery(countSql);
					countRs.next();
					int total = countRs.getInt("n");

					String countSwingSql = "SELECT COUNT(*) AS 'n' FROM " + tbl
							+ " AS o WHERE o.swing_up = 0 AND o.swing_down = 0;";
					Statement countSwingStm = conn.createStatement();
					ResultSet countSwingRs = countSwingStm.executeQuery(countSwingSql);
					countSwingRs.next();
					int n = countSwingRs.getInt("n");
					if (n > 0) {
						System.out.println(tbl + " EMPTY SWING: " + n + "/" + total);
					}
				}
				System.out.println("-------------------------------------");
			}
			System.out.println("-------------------------------------");
		}
	}

}
