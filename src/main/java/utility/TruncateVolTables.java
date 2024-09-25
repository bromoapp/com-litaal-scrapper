package utility;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class TruncateVolTables {

	public static final String[] PAIRS = { "AUDUSD", "EURUSD", "GBPUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY",
			"AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD", "CADCHF", "CADJPY", "CHFJPY", "EURAUD", "EURCAD", "EURCHF",
			"EURGBP", "EURJPY", "EURNZD", "GBPAUD", "GBPCAD", "GBPCHF", "GBPJPY", "GBPNZD", "NZDCAD", "NZDCHF",
			"NZDJPY" };
	public static final int[] YEARS = { 2020, 2021, 2022, 2023 };

	public static void main(String[] args) throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:MySQL://localhost:3306/newsfx_db", "root", "555555");
		for (String pair : PAIRS) {
			for (int year : YEARS) {
				String tbl = pair + "_vol_" + year;
				String truncSql = "TRUNCATE " + tbl;
				Statement stm = conn.createStatement();
				stm.execute(truncSql);
			}
		}
	}

}
