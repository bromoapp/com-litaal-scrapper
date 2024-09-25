package utility;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import runner.common.Paging;

/**
 * Updating pair price with swing values
 */
public class AddSwingValToPairPriceTables {

	private static int[] YEARS = { 2023, 2022, 2021, 2020 };
	private static final String[] PAIRS = { "AUDUSD", "EURUSD", "GBPUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY",
			"AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD", "CADCHF", "CADJPY", "CHFJPY", "EURAUD", "EURCAD", "EURCHF",
			"EURGBP", "EURJPY", "EURNZD", "GBPAUD", "GBPCAD", "GBPCHF", "GBPJPY", "GBPNZD", "NZDCAD", "NZDCHF",
			"NZDJPY" };
	private static final String[] TFRAMES_STR = { "M1" };
	private static final int max_perpage = 1000;

	private static String count = "SELECT COUNT(*) AS 'n' FROM %s;";
	private static String selectAll = "SELECT * FROM %s;";
	private static String selectLimit = "SELECT * FROM %s LIMIT %s, %s;";
	private static String update = "UPDATE %s SET swing_up = ?, swing_down = ? WHERE id = ?;";

	public static void main(String[] args) throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:MySQL://localhost:3306/newsfx_db", "root", "555555");
		Statement countStm = conn.createStatement();
		Statement selectStm = conn.createStatement();
		for (String tf : TFRAMES_STR) {
			for (String pair : PAIRS) {
				for (int yr : YEARS) {
					String tbl = pair.toLowerCase() + "_" + tf.toLowerCase() + "_" + yr;
					String countSql = String.format(count, tbl);
					ResultSet countRs = countStm.executeQuery(countSql);
					if (countRs.next()) {
						int total = countRs.getInt("n");
						if (total > max_perpage) {
							System.out.println("---------- BIGGER THEN MAX -------------------------------------");
							int pages = total / max_perpage;
							int remains = total % max_perpage;
							if (remains > 0) {
								pages += 1;
							}
							for (int pgno = 1; pgno <= pages; pgno++) {
								Paging pging = getPagingInfo(total, max_perpage, pgno);
								String selectLimitSql = String.format(selectLimit, tbl, pging.getOffset(), max_perpage);
								System.out.println(selectLimitSql);
								String updateSql = String.format(update, tbl);
								PreparedStatement ps = conn.prepareStatement(updateSql);
								ResultSet selectLimitRs = selectStm.executeQuery(selectLimitSql);
								while (selectLimitRs.next()) {
									updateDb(selectLimitRs, ps, pair);
								}
								ps.executeBatch();
							}
						} else {
							System.out.println("---------- LESSER THEN MAX -------------------------------------");
							String selectAllSql = String.format(selectAll, tbl);
							System.out.println(selectAllSql);
							ResultSet selectAllRs = selectStm.executeQuery(selectAllSql);
							String updateSql = String.format(update, tbl);
							PreparedStatement ps = conn.prepareStatement(updateSql);
							while (selectAllRs.next()) {
								updateDb(selectAllRs, ps, pair);
							}
							ps.executeBatch();
						}
					}
				}
			}
		}
		conn.close();
	}

	private static void updateDb(ResultSet selectRs, PreparedStatement ps, String pair) throws Exception {
		long id = selectRs.getLong("id");
		BigDecimal swingUpGap, swingDownGap = null;
		BigDecimal open = BigDecimal.valueOf(selectRs.getDouble("open"));
		BigDecimal close = BigDecimal.valueOf(selectRs.getDouble("close"));
		BigDecimal high = BigDecimal.valueOf(selectRs.getDouble("high"));
		BigDecimal low = BigDecimal.valueOf(selectRs.getDouble("low"));
		if (close.compareTo(open) == 1) {
			// BULLISH
			swingUpGap = high.subtract(close);
			swingDownGap = open.subtract(low);
		} else {
			// BEARISH
			swingUpGap = high.subtract(open);
			swingDownGap = close.subtract(low);
		}
		BigDecimal divider = new BigDecimal(0.0001);
		if (pair.contains("JPY")) {
			divider = new BigDecimal(0.01);
		}
		double swingUp = swingUpGap.divide(divider, 1, RoundingMode.CEILING).doubleValue();
		double swingDown = swingDownGap.divide(divider, 1, RoundingMode.CEILING).doubleValue();
		ps.setDouble(1, swingUp);
		ps.setDouble(2, swingDown);
		ps.setLong(3, id);
		ps.addBatch();
	}

	private static Paging getPagingInfo(int total, int max, int pgno) {
		int pages = total / max;
		int remains = total % max;
		if (remains > 0) {
			pages += 1;
		}
		return new Paging(pages, ((pgno - 1) * max));
	}
}
