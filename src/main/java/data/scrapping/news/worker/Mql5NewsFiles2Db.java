package data.scrapping.news.worker;

import java.io.File;
import java.io.Reader;
import java.nio.file.Files;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;

import org.jeasy.flows.work.DefaultWorkReport;
import org.jeasy.flows.work.WorkContext;
import org.jeasy.flows.work.WorkReport;
import org.jeasy.flows.work.WorkStatus;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import data.scrapping.common.helper.Util;
import data.scrapping.common.worker.DbRelatedWorkerBase;
import data.scrapping.news.vo.Mql5News;

public class Mql5NewsFiles2Db extends DbRelatedWorkerBase {
	private final String tblPrefix = "news_mql5_";

	public Mql5NewsFiles2Db(String dbConnString, String dbUsername, String dbPassword, String folder, int year) {
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
					+ " (uuid, id, event_type, time_mode, processed, url, "
					+ "event_name, importance, currency_code, forecast_value, previous_value, old_previous_value,"
					+ " actual_value, release_date, impact_direction, impact_value, impact_value_f, country,"
					+ " country_name, full_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
			PreparedStatement ps = this.getConn().prepareStatement(insert);
			File yearDir = new File(this.getPath() + this.getYear());
			if (yearDir.isDirectory()) {
				File[] files = yearDir.listFiles();
				for (File jfile : files) {
					if (jfile.isFile()) {
						System.out.println("READING: " + jfile.getName());
						Reader reader = Files.newBufferedReader(jfile.toPath());
						JsonArray jarray = (JsonArray) JsonParser.parseReader(reader);
						for (JsonElement je : jarray) {
							int tot = 0;
							// Check if currency code match with acceptable codes
							Mql5News news = gson.fromJson(je.toString(), Mql5News.class);
							if (Util.isCurrencyUsable(news.getCurrencyCode())) {
								String check = "SELECT COUNT(*) AS 'n' FROM " + tblPrefix + this.getYear()
										+ " AS o WHERE o.id = " + news.getId();
								Statement stmt = this.getConn().createStatement();
								ResultSet rs = stmt.executeQuery(check);
								if (rs.next()) {
									tot = rs.getInt("n");
								}
								if (tot > 0) {
									// System.out.println("SKIP ID: " + news.getId());
								} else {
									try {
										ps.setString(1, UUID.randomUUID().toString());
										// ---- id
										ps.setLong(2, news.getId());
										// ---- event_type
										ps.setInt(3, news.getEventType());
										// ---- time_mode
										ps.setInt(4, news.getTimeMode());
										// ---- processed
										ps.setInt(5, news.getProcessed());
										// ---- url
										ps.setString(6, news.getUrl());
										// ---- event_name
										ps.setString(7, news.getEventName());
										// ---- importance
										ps.setString(8, news.getImportance());
										// ---- currency_code
										ps.setString(9, news.getCurrencyCode());
										// ---- forecast_value
										if (news.getForecastValue() != null && news.getForecastValue().length() > 0) {
											if (news.getForecastValue().trim().equalsIgnoreCase("-")) {
												ps.setString(10, null);
											} else {
												ps.setString(10, news.getForecastValue());
											}
										} else {
											ps.setString(10, null);
										}
										// ---- previous_value
										if (news.getPreviousValue() != null && news.getPreviousValue().length() > 0) {
											ps.setString(11, news.getPreviousValue());
										} else {
											ps.setString(11, null);
										}
										// ---- old_previous_value
										if (news.getOldPreviousValue() != null
												&& news.getOldPreviousValue().length() > 0) {
											ps.setString(12, news.getOldPreviousValue());
										} else {
											ps.setString(12, null);
										}
										// ---- actual_value
										if (news.getActualValue() != null && news.getActualValue().length() > 0) {
											ps.setString(13, news.getActualValue());
										} else {
											ps.setString(13, null);
										}
										// ---- release_date
										ps.setLong(14, news.getReleaseDate() / 1000L);
										// ---- impact_direction
										ps.setInt(15, news.getImpactDirection());
										// ---- impact_value
										if (news.getImpactValue() != null && news.getImpactValue().length() > 0) {
											ps.setString(16, news.getImpactValue());
										} else {
											ps.setString(16, null);
										}
										// ---- impact_value_f
										if (news.getImpactValueF() != null && news.getImpactValueF().length() > 0) {
											ps.setString(17, news.getImpactValueF());
										} else {
											ps.setString(17, null);
										}
										// ---- country
										String countryCode = Util.to2DCountryCode(Integer.parseInt(news.getCountry()));
										ps.setString(18, countryCode);
										// ---- country_name
										if (news.getCountryName() != null && news.getCountryName().length() > 0) {
											ps.setString(19, news.getCountryName());
										} else {
											ps.setString(19, null);
										}
										// ---- full_date
										ps.setString(20, news.getFullDate());
										ps.execute();
									} catch (Exception e) {
										System.out.println(news.toString());
										e.printStackTrace();
									}
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

}
