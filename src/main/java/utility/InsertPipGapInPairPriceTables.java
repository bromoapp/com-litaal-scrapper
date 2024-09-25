package utility;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class InsertPipGapInPairPriceTables {

	public static int[] YEARS = { 2022, 2021, 2020 };
	public static final String[] MAJORS = { "AUDUSD", "EURUSD", "GBPUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY" };
	public static final String[] CROSSES1 = { "AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD", "CADCHF", "CADJPY", "CHFJPY" };
	public static final String[] CROSSES2 = { "EURAUD", "EURCAD", "EURCHF", "EURGBP", "EURJPY", "EURNZD", "GBPAUD" };
	public static final String[] CROSSES3 = { "GBPCAD", "GBPCHF", "GBPJPY", "GBPNZD", "NZDCAD", "NZDCHF", "NZDJPY" };
	public static final String[] TFRAMES_STR = { "M15", "M5", "M1" };
//	public static final String[] TFRAMES_STR = { "D1", "H1", "M30", "M15", "M5", "M1" };

	public static void main(String[] args) throws Exception {
		String select = "SELECT o.id, o.high, o.low FROM %s AS o WHERE o.pip_gap = 0;";
		String update = "UPDATE %s set pip_gap = %s WHERE id = %s;";
		Connection conn = DriverManager.getConnection("jdbc:MySQL://localhost:3306/newsfx_db", "root", "555555");
		Statement stmSelect = conn.createStatement();
		Statement stmUpdate = conn.createStatement();
		for (String tf : TFRAMES_STR) {
			for (String pair : CROSSES3) {
				for (int yr : YEARS) {
					String tbl = pair.toLowerCase() + "_" + tf.toLowerCase() + "_" + yr;
					String selectSql = String.format(select, tbl);
					ResultSet rsSelect = stmSelect.executeQuery(selectSql);
					while (rsSelect.next()) {
						Long id = rsSelect.getLong("id");
						BigDecimal high = rsSelect.getBigDecimal("high");
						BigDecimal low = rsSelect.getBigDecimal("low");
						double divider = 0.0001;
						if (pair.contains("JPY")) {
							divider = 0.01;
						}
						BigDecimal diff = high.subtract(low).divide(BigDecimal.valueOf(divider), 1, RoundingMode.HALF_DOWN);
						String updateSql = String.format(update, tbl, diff, id);
						System.out.println(updateSql);
						stmUpdate.addBatch(updateSql);
					}
					stmUpdate.executeLargeBatch();
				}
			}
		}
		conn.close();
	}

}
