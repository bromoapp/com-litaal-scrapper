package data.scrapping.prices.worker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.PreparedStatement;

import org.jeasy.flows.engine.WorkFlowEngine;
import org.jeasy.flows.engine.WorkFlowEngineBuilder;
import org.jeasy.flows.work.DefaultWorkReport;
import org.jeasy.flows.work.WorkContext;
import org.jeasy.flows.work.WorkReport;
import org.jeasy.flows.work.WorkStatus;
import org.jeasy.flows.workflow.SequentialFlow;
import org.jeasy.flows.workflow.WorkFlow;

import data.scrapping.common.worker.DbRelatedWorkerBase;
import runner.common.Config;

public class PairPriceFiles2Db extends DbRelatedWorkerBase {

	private final String header = "timestamp,open,high,low,close,volume";
	private String[] pairs;
	private String[] tframes;

	public PairPriceFiles2Db(String dbConnString, String dbUsername, String dbPassword, String folder, int year,
			String[] pairs, String[] tframes) {
		this.setDbConnString(dbConnString);
		this.setDbUsername(dbUsername);
		this.setDbPassword(dbPassword);
		this.setPath(folder);
		this.setYear(year);
		this.pairs = pairs;
		this.tframes = tframes;
	}

	@Override
	public WorkReport execute(WorkContext ctx) {
		System.out.println("START PERSISTING TO DB...");
		String insertSql = "INSERT INTO %s (time, open, high, low, close, volume, pip_gap, swing_up, swing_down) "
				+ "VALUES (?,?,?,?,?,?,?,?,?)";
		try {
			for (String pair : this.pairs) {
				for (String tf : this.tframes) {
					String path = this.getPath() + "\\" + pair + "\\" + this.getYear() + "\\" + tf + "\\download\\";
					File dir = new File(path);
					if (dir.isDirectory()) {
						int n = 1;
						int o = 1;
						for (File csv : dir.listFiles()) {
							if (csv.isFile() && csv.getName()
									.contains((pair + "-" + tf + "-bid-" + this.getYear()).toLowerCase())) {
								System.out.println(">>> " + csv.getName());

								String tblName = (pair + "_" + tf + "_" + this.getYear()).toLowerCase();
								String sql = String.format(insertSql, tblName);
								PreparedStatement ps = this.getConn().prepareStatement(sql);

								FileReader fr = new FileReader(csv);
								BufferedReader br = new BufferedReader(fr);
								String line = "";
								while ((line = br.readLine()) != null) {
									if (!line.equalsIgnoreCase(header)) {
										String[] values = line.split(",");
										Long time = Long.parseLong(values[0]);
										ps.setLong(1, time / 1000L);// ---> time

										Double open = Double.valueOf(values[1]);
										ps.setDouble(2, open);// ---> open

										Double high = Double.valueOf(values[2]);
										ps.setDouble(3, high);// ---> high

										Double low = Double.valueOf(values[3]);
										ps.setDouble(4, low);// ---> low

										Double close = Double.valueOf(values[4]);
										ps.setDouble(5, close);// ---> close

										Double volume = Double.valueOf(values[5]);
										ps.setDouble(6, volume);// ---> volume

										BigDecimal gap = BigDecimal.valueOf(high).subtract(BigDecimal.valueOf(low));
										BigDecimal divider = new BigDecimal(0.0001);
										if (pair.contains("JPY")) {
											divider = new BigDecimal(0.01);
										}
										double pipGap = gap.divide(divider, 1, RoundingMode.HALF_DOWN).doubleValue();
										ps.setDouble(7, pipGap);// ---> pip_gap

										BigDecimal swingUpGap, swingDownGap = null;
										if (close.compareTo(open) == 1) {
											// Bullish
											swingUpGap = BigDecimal.valueOf(high).subtract(BigDecimal.valueOf(close));
											swingDownGap = BigDecimal.valueOf(open).subtract(BigDecimal.valueOf(low));
										} else {
											// Bearish
											swingUpGap = BigDecimal.valueOf(high).subtract(BigDecimal.valueOf(open));
											swingDownGap = BigDecimal.valueOf(close).subtract(BigDecimal.valueOf(low));
										}
										double swingUp = swingUpGap.divide(divider, 1, RoundingMode.CEILING)
												.doubleValue();
										ps.setDouble(8, swingUp);

										double swingDown = swingDownGap.divide(divider, 1, RoundingMode.CEILING)
												.doubleValue();
										ps.setDouble(9, swingDown);

										ps.addBatch();
										n += 1;
									}
								}
								ps.executeBatch();
								br.close();
								fr.close();
								System.out.println(this.getYear() + " [" + pair + "][" + tf + "] " + o + " -> " + n);
								o += 1;
							}
						}
					}
				}
			}

			this.getConn().close();
			return new DefaultWorkReport(WorkStatus.COMPLETED, ctx);
		} catch (Exception e) {
			e.printStackTrace();
			return new DefaultWorkReport(WorkStatus.FAILED, ctx);
		}
	}

	private static String rootPath = ".\\resources\\";
	private static int yr = 2023;
	public static final String[] PAIRS = { "AUDUSD", "EURUSD", "GBPUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY",
			"AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD", "CADCHF", "CADJPY", "CHFJPY", "EURAUD", "EURCAD", "EURCHF",
			"EURGBP", "EURJPY", "EURNZD", "GBPAUD", "GBPCAD", "GBPCHF", "GBPJPY", "GBPNZD", "NZDCAD", "NZDCHF",
			"NZDJPY" };
	public static final String[] TFRAMES_STR = { "D1", "H1", "M30", "M15", "M5", "M1" };

	public static void main(String[] args) {
		PairPriceFiles2Db persister = new PairPriceFiles2Db(Config.CONN_STRING, Config.DB_USERNAME, Config.DB_PASSWORD,
				rootPath, yr, PAIRS, TFRAMES_STR);
		WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
		WorkFlow flow = SequentialFlow.Builder.aNewSequentialFlow().named("Scrapping DailyFx News").execute(persister)
				.build();
		WorkContext ctx = new WorkContext();
		WorkReport report = engine.run(flow, ctx);
		System.out.println(report.getStatus());
	}

}
