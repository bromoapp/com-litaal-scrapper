package data.scrapping.prices.worker;

import java.io.File;
import java.io.FileReader;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.DateFormatSymbols;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.jeasy.flows.engine.WorkFlowEngine;
import org.jeasy.flows.engine.WorkFlowEngineBuilder;
import org.jeasy.flows.work.DefaultWorkReport;
import org.jeasy.flows.work.WorkContext;
import org.jeasy.flows.work.WorkReport;
import org.jeasy.flows.work.WorkStatus;
import org.jeasy.flows.workflow.SequentialFlow;
import org.jeasy.flows.workflow.WorkFlow;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import data.scrapping.common.helper.Util;
import data.scrapping.common.vo.ScrapTime;
import data.scrapping.common.worker.DbRelatedWorkerBase;
import data.scrapping.prices.vo.ETimeFrame;
import data.scrapping.prices.vo.TickData;
import runner.common.Config;

public class PairTickFiles2Db extends DbRelatedWorkerBase {

	private String[] timeframes;
	private String[] pairs;

	public PairTickFiles2Db(String dbConnString, String dbUsername, String dbPassword, String folder, int year,
			int monthStart, int monthEnd, String[] tframes, String[] pairs) {
		this.setDbConnString(dbConnString);
		this.setDbUsername(dbUsername);
		this.setDbPassword(dbPassword);
		this.setPath(folder);
		this.setYear(year);
		this.setMonthStart(monthStart);
		this.setMonthEnd(monthEnd);
		this.timeframes = tframes;
		this.pairs = pairs;
	}

	@Override
	public WorkReport execute(WorkContext ctx) {
		System.out.println("START PERSISTING TO DB...");
		try {
			String[] months = new DateFormatSymbols().getShortMonths();
			if (this.pairs.length > 0) {
				for (String pair : this.pairs) {
					for (String tframe : this.timeframes) {
						ETimeFrame tf = ETimeFrame.valueOf(tframe);
						String tblName = pair.toLowerCase() + "_" + tframe.toLowerCase() + "t_" + this.getYear();
						String sql = "INSERT INTO " + tblName + " (time, total_ticks, highest_ask, lowest_bid,"
								+ " spread, ask_vol, bid_vol, total_vol) VALUES (?,?,?,?,?,?,?,?)";
						for (int month = this.getMonthStart(); month <= this.getMonthEnd(); month++) {
							String path = this.getPath() + "\\" + pair + "\\" + this.getYear() + "\\" + "\\TICK\\"
									+ months[month - 1] + "\\download\\";
							File dir = new File(path);
							if (dir.isDirectory()) {
								Map<Long, List<String[]>> groups = new LinkedHashMap<>();
								for (File csv : dir.listFiles()) {
									if (csv.getName().contains(pair.toLowerCase() + "-tick-" + this.getYear())) {
										long lines = Files.lines(Path.of(csv.getAbsolutePath())).count();
										System.out.println("TF: " + tframe + "; READ: " + csv.getName() + " - " + lines
												+ " LINES");
										if (lines == 0 && groups.size() > 0) {
											System.out.println(tf.name() + " -> groups: " + groups.size());
											persistsToDB(pair, groups, this.getConn(), sql);
										} else {
											FileReader reader = new FileReader(csv);
											CSVParser parser = new CSVParserBuilder().withSeparator(',').build();
											CSVReader csvReader = new CSVReaderBuilder(reader).withCSVParser(parser)
													.withSkipLines(1).build();
											String[] line;
											while ((line = csvReader.readNext()) != null) {
												long time = Long.parseLong(line[0]);
												long nearest = Util.toNearestTimeInUnixtimeMilis(tf, time);
												List<String[]> items = groups.get(nearest);
												if (items != null) {
													items.add(line);
												} else {
													items = new ArrayList<>();
													items.add(line);
													groups.put(nearest, items);
												}
											}
											csvReader.close();
											reader.close();
										}
									}
								}
								if (groups.size() > 0) {
									System.out.println("Persists the remainder: " + groups.size());
									persistsToDB(pair, groups, this.getConn(), sql);
								}
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

	private void persistsToDB(String pair, Map<Long, List<String[]>> groups, Connection conn, String sql)
			throws Exception {
		Object[] keys = (Object[]) groups.keySet().toArray();
		int size = keys.length;
		PreparedStatement ps = conn.prepareStatement(sql);
		for (int x = 0; x < (size - 1); x++) {
			Object key = keys[x];
			TickData data = new TickData();
			data.setTime((long) key);
			List<String[]> records = groups.get(key);
			data.setTotal(records.size());
			for (String[] record : records) {
				BigDecimal askPrice = BigDecimal.valueOf(Double.parseDouble(record[1]));
				if (BigDecimal.valueOf(data.getHighestAsk()).compareTo(askPrice) < 0) {
					data.setHighestAsk(BigDecimal.valueOf(Double.parseDouble(record[1])).doubleValue());
				}
				BigDecimal bidPrice = BigDecimal.valueOf(Double.parseDouble(record[2]));
				if (BigDecimal.valueOf(data.getLowestBid()).compareTo(bidPrice) > 0) {
					data.setLowestBid(BigDecimal.valueOf(Double.parseDouble(record[2])).doubleValue());
				}
				data.setSpread(substract2Decimals(askPrice, bidPrice).doubleValue());
				BigDecimal askVol = BigDecimal.valueOf(Double.parseDouble(record[3]));
				data.setAskVol(roundADecimal(add2Decimals(BigDecimal.valueOf(data.getAskVol()), askVol)).doubleValue());
				BigDecimal bidVol = BigDecimal.valueOf(Double.parseDouble(record[4]));
				data.setBidVol(roundADecimal(add2Decimals(BigDecimal.valueOf(data.getBidVol()), bidVol)).doubleValue());
				data.setTotalVol(add2Decimals(askVol, bidVol).doubleValue());
			}
			ps.setLong(1, data.getTime());
			ps.setLong(2, data.getTotal());
			ps.setDouble(3, data.getHighestAsk());
			ps.setDouble(4, data.getLowestBid());
			ps.setDouble(5, data.getSpread());
			ps.setDouble(6, data.getAskVol());
			ps.setDouble(7, data.getBidVol());
			ps.setDouble(8, data.getTotalVol());
			ps.addBatch();
			groups.remove(key);
		}
		ps.executeBatch();
	}

	private BigDecimal substract2Decimals(BigDecimal a, BigDecimal b) {
		return a.subtract(b);
	}

	private BigDecimal add2Decimals(BigDecimal a, BigDecimal b) {
		return a.add(b);
	}

	@SuppressWarnings("deprecation")
	private BigDecimal roundADecimal(BigDecimal a) {
		return a.setScale(2, BigDecimal.ROUND_HALF_EVEN);
	}

	private static final String rootPath = ".\\resources\\";
	public static final String[] TFRAMES_STR = { "D1", "H1", "M30", "M15", "M5", "M1" };
	public static final String[] PAIRS = { "AUDUSD", "EURUSD", "GBPUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY",
			"AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD", "CADCHF", "CADJPY", "CHFJPY", "EURAUD", "EURCAD", "EURCHF",
			"EURGBP", "EURJPY", "EURNZD", "GBPAUD", "GBPCAD", "GBPCHF", "GBPJPY", "GBPNZD", "NZDCAD", "NZDCHF",
			"NZDJPY" };

	public static void main(String[] args) throws Exception {
		for (ScrapTime time : getScrapTimes()) {
			WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
			WorkContext ctx = new WorkContext();
			PairTickFiles2Db work = new PairTickFiles2Db(Config.CONN_STRING, Config.DB_USERNAME, Config.DB_PASSWORD,
					rootPath, time.getYear(), time.getMonthStart(), time.getMonthEnd(), TFRAMES_STR, PAIRS);
			WorkFlow flow2 = SequentialFlow.Builder.aNewSequentialFlow().named("Creating pair prices & ticks tables")
					.execute(work).build();
			WorkReport report2 = engine.run(flow2, ctx);
			System.out.println(report2.getStatus());
		}
	}

	private static List<ScrapTime> times;

	public static List<ScrapTime> getScrapTimes() {
		if (times == null) {
			times = new LinkedList<>();
			times.add(new ScrapTime(2022, 1, 12));
			times.add(new ScrapTime(2021, 1, 12));
			times.add(new ScrapTime(2020, 1, 12));
		}
		return times;
	}

}
