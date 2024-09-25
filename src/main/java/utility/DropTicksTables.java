package utility;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import runner.common.Config;

public class DropTicksTables {
	public static final String[] PAIRS = { "EURAUD", "EURCAD", "EURCHF", "EURGBP", "EURJPY", "EURNZD", "GBPAUD" };
	public static final String[] TFRAMES_STR = { "D1", "H1", "M30", "M15", "M5", "M1" };

	public static void main(String[] args) throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:MySQL://localhost:3306/newsfx_db", "root", "555555");

		for (int year : Config.YEARS) {
			for (String pair : PAIRS) {
				for (String tf : Config.TFRAMES_STR) {
					String sql = "DROP TABLE IF EXISTS " + pair.toLowerCase() + "_" + tf.toLowerCase() + "t_" + year
							+ ";";
					System.out.println(sql);
					Statement stmt = conn.createStatement();
					stmt.execute(sql);
				}
			}
		}
	}

}
