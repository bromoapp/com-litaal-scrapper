package runner.common;

import java.util.LinkedList;
import java.util.List;

import data.scrapping.common.vo.ScrapTime;

public class Config {
	/****************************************************************
	 * List of arguments for tables generator
	 ****************************************************************/
	public static int[] YEARS = { 2023, 2022, 2021, 2020 };
	public static String[] NEWS_PROVIDERS = { "dailyfx", "dukascopy", "fxstreet", "mql5" };

	/****************************************************************
	 * List of times to scrap from setup
	 ****************************************************************/
	public static List<ScrapTime> SCRAP_TIMES = getScrapTimes();
	private static List<ScrapTime> times;

	public static List<ScrapTime> getScrapTimes() {
		if (times == null) {
			times = new LinkedList<>();
			times.add(new ScrapTime(2023, 3, 3));
		}
		return times;
	}

	/****************************************************************
	 * DB setup for storing scrapped data to DB
	 ****************************************************************/
	public static final String CONN_STRING = "jdbc:MySQL://localhost:3306/newsfx_db";
	public static final String DB_USERNAME = "root";
	public static final String DB_PASSWORD = "555555";

	/****************************************************************
	 * Pairs and Pair Types to scrap both pair price and tick setup
	 ****************************************************************/
	public static final String[] PAIRS = {
			"AUDUSD", "EURUSD", "GBPUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY",
			"AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD", "CADCHF", "CADJPY", "CHFJPY", 
			"EURAUD", "EURCAD", "EURCHF", "EURGBP", "EURJPY", "EURNZD", "GBPAUD",
			"GBPCAD", "GBPCHF", "GBPJPY", "GBPNZD", "NZDCAD", "NZDCHF", "NZDJPY" 
	};

	/****************************************************************
	 * Time frames to scrap setup
	 ****************************************************************/
//	public static final String[] TFRAMES_STR = { "D1", "H1", "M30" };
	public static final String[] TFRAMES_STR = { "D1", "H1", "M30", "M15", "M5", "M1" };
	public static final int[] TFRAMES_INT = { 1440, 60, 30, 15, 5, 1 };

}
