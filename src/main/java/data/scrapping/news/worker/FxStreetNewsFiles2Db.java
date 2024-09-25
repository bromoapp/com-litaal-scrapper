package data.scrapping.news.worker;

import java.io.File;
import java.io.Reader;
import java.nio.file.Files;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.UUID;

import org.jeasy.flows.engine.WorkFlowEngine;
import org.jeasy.flows.engine.WorkFlowEngineBuilder;
import org.jeasy.flows.work.DefaultWorkReport;
import org.jeasy.flows.work.WorkContext;
import org.jeasy.flows.work.WorkReport;
import org.jeasy.flows.work.WorkStatus;
import org.jeasy.flows.workflow.SequentialFlow;
import org.jeasy.flows.workflow.WorkFlow;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import data.scrapping.common.helper.Util;
import data.scrapping.common.worker.DbRelatedWorkerBase;
import data.scrapping.news.vo.FxStreetNews;
import runner.common.Config;

public class FxStreetNewsFiles2Db extends DbRelatedWorkerBase {
	private final String tblPrefix = "news_fxstreet_";
	private final SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	private final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'");
	private final SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.'4Z'");
	private final SimpleDateFormat sdf4 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.ss'Z'");
	private final SimpleDateFormat sdf5 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss[.0-9]'Z'");

	public FxStreetNewsFiles2Db(String dbConnString, String dbUsername, String dbPassword, String folder, int year) {
		this.setDbConnString(dbConnString);
		this.setDbUsername(dbUsername);
		this.setDbPassword(dbPassword);
		this.setPath(folder);
		this.setYear(year);
	}

	@Override
	public WorkReport execute(WorkContext ctx) {
		Gson gson = new Gson();
		System.out.println("START PERSISTING TO DB...");
		try {
			String insert = "INSERT INTO " + tblPrefix + this.getYear()
					+ " (uuid, id, event_id, date_utc, date, period_date_utc, period_date,"
					+ "period_type, actual, revise, consensus, ratio_deviation, previous, is_better_than_expected, name,"
					+ "country_code, currency_code, unit, potency, volatility, is_allday, is_tentative, is_preliminary,"
					+ "is_report, is_speech, last_updated) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			File yearDir = new File(this.getPath() + this.getYear());
			if (yearDir.isDirectory()) {
				File[] files = yearDir.listFiles();
				for (File jfile : files) {
					if (jfile.isFile()) {
						System.out.println("READING: " + jfile.getName());
						Reader reader = Files.newBufferedReader(jfile.toPath());
						JsonArray jarray = (JsonArray) JsonParser.parseReader(reader);
						for (JsonElement je : jarray) {
							PreparedStatement ps = this.getConn().prepareStatement(insert);
							int tot = 0;
							FxStreetNews o = gson.fromJson(je, FxStreetNews.class);
//							System.out.println(o.toString());
							// Check if currency code match with acceptable codes
							if (Util.isCurrencyUsable(o.getCurrencyCode())) {
								String check = "SELECT COUNT(*) AS 'n' FROM " + tblPrefix + this.getYear()
										+ " AS o WHERE o.id = '" + o.getId() + "'";
								Statement stmt = this.getConn().createStatement();
								ResultSet rs = stmt.executeQuery(check);
								if (rs.next()) {
									tot = rs.getInt("n");
								}
								if (tot > 0) {
									// Skip
								} else {
									ps.setString(1, UUID.randomUUID().toString());
									ps.setString(2, o.getId());
									ps.setString(3, o.getEventId());
									ps.setString(4, o.getDateUtc());
									if (o.getDateUtc() != null) {
										Long value = null;
										try {
											value = sdf1.parse(o.getDateUtc()).getTime() / 1000L;
										} catch (Exception e) {
										}
										try {
											value = sdf2.parse(o.getDateUtc()).getTime() / 1000L;
										} catch (Exception e) {
										}
										try {
											value = sdf3.parse(o.getDateUtc()).getTime() / 1000L;
										} catch (Exception e) {
										}
										try {
											value = sdf4.parse(o.getDateUtc()).getTime() / 1000L;
										} catch (Exception e) {
										}
										try {
											value = sdf5.parse(o.getDateUtc()).getTime() / 1000L;
										} catch (Exception e) {
										}
										ps.setLong(5, add7Hours(value));
									} else {
										ps.setObject(5, null);
									}
									ps.setString(6, o.getPeriodDateUtc());
									if (o.getPeriodDateUtc() != null) {
										if (o.getPeriodDateUtc().length() == 20) {
											ps.setLong(7,
													add7Hours(sdf1.parse(o.getPeriodDateUtc()).getTime() / 1000L));
										}
										if (o.getPeriodDateUtc().length() == 24) {
											ps.setLong(7,
													add7Hours(sdf2.parse(o.getPeriodDateUtc()).getTime() / 1000L));
										}
									} else {
										ps.setObject(7, null);
									}
									ps.setString(8, o.getPeriodType());
									if (o.getActual() != null) {
										ps.setDouble(9, o.getActual());
									} else {
										ps.setObject(9, null);
									}
									if (o.getRevise() != null) {
										ps.setDouble(10, o.getRevise());
									} else {
										ps.setObject(10, null);
									}
									if (o.getConsensus() != null) {
										ps.setDouble(11, o.getConsensus());
									} else {
										ps.setObject(11, null);
									}
									if (o.getRatioDeviation() != null) {
										ps.setDouble(12, o.getRatioDeviation());
									} else {
										ps.setObject(12, null);
									}
									if (o.getPrevious() != null) {
										ps.setDouble(13, o.getPrevious());
									} else {
										ps.setObject(13, null);
									}
									if (o.getIsBetterThanExpected() != null) {
										ps.setBoolean(14, o.getIsBetterThanExpected());
									} else {
										ps.setObject(14, null);
									}
									ps.setString(15, o.getName());
									ps.setString(16, o.getCountryCode());
									ps.setString(17, o.getCurrencyCode());
									ps.setString(18, o.getUnit());
									ps.setString(19, o.getPotency());
									ps.setString(20, o.getVolatility());
									if (o.getIsAllDay() != null) {
										ps.setBoolean(21, o.getIsAllDay());
									} else {
										ps.setObject(21, null);
									}
									if (o.getIsTentative() != null) {
										ps.setBoolean(22, o.getIsTentative());
									} else {
										ps.setObject(22, null);
									}
									if (o.getIsPreliminary() != null) {
										ps.setBoolean(23, o.getIsPreliminary());
									} else {
										ps.setObject(23, null);
									}
									if (o.getIsReport() != null) {
										ps.setBoolean(24, o.getIsReport());
									} else {
										ps.setObject(24, null);
									}
									if (o.getIsSpeech() != null) {
										ps.setBoolean(25, o.getIsSpeech());
									} else {
										ps.setObject(25, null);
									}
									if (o.getLastUpdated() != null) {
										ps.setLong(26, o.getLastUpdated());
									} else {
										ps.setObject(26, null);
									}
									ps.execute();
								}
							}
						}
						reader.close();
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

	private static String fxstreetPath = ".\\resources\\FXSTREET\\";

	public static void main(String[] args) throws Exception {
		FxStreetNewsFiles2Db persister = new FxStreetNewsFiles2Db(Config.CONN_STRING, Config.DB_USERNAME,
				Config.DB_PASSWORD, fxstreetPath, 2021);
		NewsFilesMover filesMover = new NewsFilesMover(fxstreetPath, 2021);
		FxStreetNewsResultCalc calculator = new FxStreetNewsResultCalc(Config.CONN_STRING, Config.DB_USERNAME,
				Config.DB_PASSWORD, fxstreetPath, 2021);

		WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
		WorkFlow flow = SequentialFlow.Builder.aNewSequentialFlow().named("Scrapping FxStreet News").execute(persister)
				.then(filesMover).then(calculator).build();
		WorkContext ctx = new WorkContext();
		WorkReport report = engine.run(flow, ctx);
		System.out.println(report.getStatus());

//		String date = "2022-10-04T15:02:27.73Z";
//		SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss[.*]'Z'");
//		System.out.println(sdf3.parse(date).toString());
	}
}
