package data.anaysis.worker;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.jeasy.flows.engine.WorkFlowEngine;
import org.jeasy.flows.engine.WorkFlowEngineBuilder;
import org.jeasy.flows.work.WorkContext;
import org.jeasy.flows.work.WorkReport;
import org.jeasy.flows.workflow.SequentialFlow;
import org.jeasy.flows.workflow.WorkFlow;

import data.scrapping.common.helper.ECurrency;
import data.scrapping.common.helper.ENewsOrigin;
import data.scrapping.common.helper.Util;
import data.scrapping.common.worker.DbRelatedWorkerBase;
import data.scrapping.prices.vo.ETimeFrame;
import runner.common.Config;

public class NewsVolatilitiesGenerator extends DbRelatedWorkerBase {

	private String selectNews = "SELECT * FROM %s";
	private String countNews = "SELECT COUNT(*) AS 'n' FROM %s";
	private String selectPair = "SELECT * FROM %s AS o WHERE o.time = %s";
	private String countVol = "SELECT COUNT(*) AS 'n' FROM %s AS o WHERE o.m1_time = %s AND o.m5_time = %s AND "
			+ "o.m15_time = %s AND o.m30_time = %s AND o.h1_time = %s";
	private String selectVol = "SELECT o.id, o.uuids FROM %s AS o WHERE o.m1_time = %s AND o.m5_time = %s AND "
			+ "o.m15_time = %s AND o.m30_time = %s AND o.h1_time = %s";
	private String insertVol = "INSERT INTO %s (m1_time, m5_time, m15_time, m30_time, h1_time, uuids, m1_gap, "
			+ "m5_gap, m15_gap, m30_gap, h1_gap) VALUES (?,?,?,?,?,?,?,?,?,?,?)";
	private String updateVol = "UPDATE %s SET uuids = ? WHERE id = ?";

	private int[] years;
	private String[] providers;
	private String[] timeframes;

	public NewsVolatilitiesGenerator(String dbConnString, String dbUsername, String dbPassword, int[] years,
			String[] providers, String[] timeframes) {
		this.setDbConnString(dbConnString);
		this.setDbUsername(dbUsername);
		this.setDbPassword(dbPassword);
		this.years = years;
		this.providers = providers;
		this.timeframes = timeframes;
	}

	@SuppressWarnings("incomplete-switch")
	@Override
	public WorkReport execute(WorkContext workContext) {
		try {
			for (int year : this.years) {
				for (String provider : this.providers) {
					ENewsOrigin ori = ENewsOrigin.valueOf(provider.toUpperCase());
					String newsTbl = "news_" + provider + "_" + year;
					String couNewsSql = String.format(countNews, newsTbl);
					Statement couNewsStm = getConn().createStatement();
					ResultSet couNewsRs = couNewsStm.executeQuery(couNewsSql);
					couNewsRs.next();
					int total = couNewsRs.getInt("n");
					int ongoing = 1;

					String selNewsSql = String.format(selectNews, newsTbl);
					Statement selNewsStm = getConn().createStatement();
					ResultSet selNewsRs = selNewsStm.executeQuery(selNewsSql);

					PreparedStatement insertPs = null, updatePs = null;
					while (selNewsRs.next()) {
						String uuid = selNewsRs.getString("uuid");
						String currency = "";
						switch (ori) {
						case DAILYFX:
							currency = selNewsRs.getString("currency");
							break;
						case DUKASCOPY:
							currency = selNewsRs.getString("currency");
							break;
						case FXSTREET:
							currency = selNewsRs.getString("currency_code");
							break;
						case MQL5:
							currency = selNewsRs.getString("currency_code");
							break;
						}
						long date = 0;
						switch (ori) {
						case DAILYFX:
							date = selNewsRs.getLong("date");
							break;
						case DUKASCOPY:
							date = selNewsRs.getLong("date");
							break;
						case FXSTREET:
							date = selNewsRs.getLong("date");
							break;
						case MQL5:
							date = selNewsRs.getLong("release_date");
							break;
						}
						long m1 = Util.toNearestTimeInUnixtimeSeconds(ETimeFrame.M1, date);
						long m5 = Util.toNearestTimeInUnixtimeSeconds(ETimeFrame.M5, date);
						long m15 = Util.toNearestTimeInUnixtimeSeconds(ETimeFrame.M15, date);
						long m30 = Util.toNearestTimeInUnixtimeSeconds(ETimeFrame.M30, date);
						long h1 = Util.toNearestTimeInUnixtimeSeconds(ETimeFrame.H1, date);

						for (Object pair : pairsFromCurrency(ECurrency.valueOf(currency), ongoing, total)) {
//							System.out.println(year + " - " + provider + " > WORKING ON: " + pair.toString());
							Double m1PipGap = null;
							Double m5PipGap = null;
							Double m15PipGap = null;
							Double m30PipGap = null;
							Double h1PipGap = null;
							for (String tf : this.timeframes) {
								ETimeFrame tframe = ETimeFrame.valueOf(tf);
								String pairTbl = pair.toString() + "_" + tf + "_" + year;
								String selPairSql = "";
								switch (tframe) {
								case H1:
									selPairSql = String.format(selectPair, pairTbl, h1);
									break;
								case M30:
									selPairSql = String.format(selectPair, pairTbl, m30);
									break;
								case M15:
									selPairSql = String.format(selectPair, pairTbl, m15);
									break;
								case M5:
									selPairSql = String.format(selectPair, pairTbl, m5);
									break;
								case M1:
									selPairSql = String.format(selectPair, pairTbl, m1);
									break;
								}
								Statement selPairStm = getConn().createStatement();
								ResultSet selPairRs = selPairStm.executeQuery(selPairSql);
								selPairRs.next();
								switch (tframe) {
								case H1:
									try {
										h1PipGap = selPairRs.getDouble("pip_gap");
									} catch (Exception e) {
									}
									break;
								case M30:
									try {
										m30PipGap = selPairRs.getDouble("pip_gap");
									} catch (Exception e) {
									}
									break;
								case M15:
									try {
										m15PipGap = selPairRs.getDouble("pip_gap");
									} catch (Exception e) {
									}
									break;
								case M5:
									try {
										m5PipGap = selPairRs.getDouble("pip_gap");
									} catch (Exception e) {
									}
									break;
								case M1:
									try {
										m1PipGap = selPairRs.getDouble("pip_gap");
									} catch (Exception e) {
									}
									break;
								}
							}

							String volTbl = pair + "_vol_" + year;
							String countVolSql = String.format(countVol, volTbl, m1, m5, m15, m30, h1);
							Statement countVolStm = getConn().createStatement();
							ResultSet countVolRs = countVolStm.executeQuery(countVolSql);
							countVolRs.next();
							int n = countVolRs.getInt("n");
							if (n == 0) {
								String insertSql = String.format(insertVol, volTbl);
								insertPs = getConn().prepareStatement(insertSql);
								insertPs.setLong(1, m1);
								insertPs.setLong(2, m5);
								insertPs.setLong(3, m15);
								insertPs.setLong(4, m30);
								insertPs.setLong(5, h1);
								insertPs.setString(6, uuid);
								if (m1PipGap != null) {
									insertPs.setDouble(7, m1PipGap);
								} else {
									insertPs.setNull(7, Types.NULL);
								}
								if (m5PipGap != null) {
									insertPs.setDouble(8, m5PipGap);
								} else {
									insertPs.setNull(8, Types.NULL);
								}
								if (m15PipGap != null) {
									insertPs.setDouble(9, m15PipGap);
								} else {
									insertPs.setNull(9, Types.NULL);
								}
								if (m30PipGap != null) {
									insertPs.setDouble(10, m30PipGap);
								} else {
									insertPs.setNull(10, Types.NULL);
								}
								if (h1PipGap != null) {
									insertPs.setDouble(11, h1PipGap);
								} else {
									insertPs.setNull(11, Types.NULL);
								}
								insertPs.execute();
							} else {
								String selecVolSql = String.format(selectVol, volTbl, m1, m5, m15, m30, h1);
								Statement selectVolStm = getConn().createStatement();
								ResultSet selectVolRs = selectVolStm.executeQuery(selecVolSql);
								selectVolRs.next();
								long id = selectVolRs.getLong("id");
								String uuids = selectVolRs.getString("uuids");
								String updateSql = String.format(updateVol, volTbl);
								updatePs = getConn().prepareStatement(updateSql);
								updatePs.setString(1, uuids + ", " + uuid);
								updatePs.setLong(2, id);
								updatePs.execute();
							}
						}
						ongoing += 1;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private Object[] pairsFromCurrency(ECurrency cur, int now, int total) {
		System.out.println("CURRENCY: " + cur.name() + " - " + now + "/" + total);
		List<String> pairs = new ArrayList<>();
		switch (cur) {
		case AUD:
			pairs.add("AUDUSD");
			pairs.add("AUDCAD");
			pairs.add("AUDCHF");
			pairs.add("AUDJPY");
			pairs.add("AUDNZD");
			pairs.add("EURAUD");
			pairs.add("GBPAUD");
			break;
		case CAD:
			pairs.add("USDCAD");
			pairs.add("AUDCAD");
			pairs.add("CADCHF");
			pairs.add("CADJPY");
			pairs.add("EURCAD");
			pairs.add("GBPCAD");
			pairs.add("NZDCAD");
			break;
		case CHF:
			pairs.add("USDCHF");
			pairs.add("AUDCHF");
			pairs.add("CADCHF");
			pairs.add("CHFJPY");
			pairs.add("EURCHF");
			pairs.add("GBPCHF");
			pairs.add("NZDCHF");
			break;
		case EUR:
			pairs.add("EURUSD");
			pairs.add("EURAUD");
			pairs.add("EURCAD");
			pairs.add("EURCHF");
			pairs.add("EURGBP");
			pairs.add("EURJPY");
			pairs.add("EURNZD");
			break;
		case GBP:
			pairs.add("GBPUSD");
			pairs.add("EURGBP");
			pairs.add("GBPAUD");
			pairs.add("GBPCAD");
			pairs.add("GBPCHF");
			pairs.add("GBPJPY");
			pairs.add("GBPNZD");
			break;
		case JPY:
			pairs.add("USDJPY");
			pairs.add("AUDJPY");
			pairs.add("CADJPY");
			pairs.add("CHFJPY");
			pairs.add("EURJPY");
			pairs.add("GBPJPY");
			pairs.add("NZDJPY");
			break;
		case NZD:
			pairs.add("NZDUSD");
			pairs.add("AUDNZD");
			pairs.add("EURNZD");
			pairs.add("GBPNZD");
			pairs.add("NZDCAD");
			pairs.add("NZDCHF");
			pairs.add("NZDJPY");
			break;
		case USD:
			pairs.add("AUDUSD");
			pairs.add("EURUSD");
			pairs.add("GBPUSD");
			pairs.add("NZDUSD");
			pairs.add("USDCAD");
			pairs.add("USDCHF");
			pairs.add("USDJPY");
			break;
		}
		return pairs.toArray();
	}

	private static int[] YEARS = { 2022 };
//	private static int[] YEARS = { 2020, 2021, 2022, 2023 };
	private static String[] NEWS_PROVIDERS = { "dailyfx" };
//	private static String[] NEWS_PROVIDERS = { "dailyfx", "dukascopy", "fxstreet", "mql5" };
	private static String[] TIMEFRAMES = { "H1", "M30", "M15", "M5", "M1" };

	public static void main(String[] args) {
		NewsVolatilitiesGenerator worker = new NewsVolatilitiesGenerator(Config.CONN_STRING, Config.DB_USERNAME,
				Config.DB_PASSWORD, YEARS, NEWS_PROVIDERS, TIMEFRAMES);
		WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
		WorkFlow flow = SequentialFlow.Builder.aNewSequentialFlow().named("Indexing News").execute(worker).build();
		WorkContext ctx = new WorkContext();
		WorkReport report = engine.run(flow, ctx);
		System.out.println(report.getStatus());
	}

}
