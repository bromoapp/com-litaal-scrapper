package utility;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Perbaikan kolom low to not null
 */
public class AlterPairPriceTables {

	public static int[] YEARS = { 2023 };
	public static final String[] PAIRS = { "AUDUSD", "EURUSD", "GBPUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY",
			"AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD", "CADCHF", "CADJPY", "CHFJPY", "EURAUD", "EURCAD", "EURCHF",
			"EURGBP", "EURJPY", "EURNZD", "GBPAUD", "GBPCAD", "GBPCHF", "GBPJPY", "GBPNZD", "NZDCAD", "NZDCHF",
			"NZDJPY" };
	public static final String[] TFRAMES_STR = { "D1", "H1", "M30", "M15", "M5", "M1" };

	public static void main(String[] args) throws Exception {
		String alter = "ALTER TABLE `%s`\r\n" + "	CHANGE COLUMN `low` `low` DOUBLE NOT NULL AFTER `high`;";
		Connection conn = DriverManager.getConnection("jdbc:MySQL://localhost:3306/newsfx_db", "root", "555555");
		for (String tf : TFRAMES_STR) {
			for (String pair : PAIRS) {
				Statement stm = conn.createStatement();
				for (int yr : YEARS) {
					String tbl = pair.toLowerCase() + "_" + tf.toLowerCase() + "_" + yr;
					String sql = String.format(alter, tbl);
					System.out.println(sql);
					stm.addBatch(sql);
				}
				stm.executeBatch();
			}
		}
		conn.close();
	}

}
