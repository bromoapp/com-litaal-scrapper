package data.scrapping.news.worker;

import java.io.File;
import java.io.Reader;
import java.nio.file.Files;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
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
import data.scrapping.news.vo.DailyFxNews;

public class DailyFxNewsFiles2Db extends DbRelatedWorkerBase {
	private final String tblPrefix = "news_dailyfx_";
	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");

	public DailyFxNewsFiles2Db(String dbConnString, String dbUsername, String dbPassword, String folder, int year) {
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
					+ " (uuid, id, ticker, symbol, date, title, description, "
					+ "importance, previous, previous_val, forecast, forecast_val, country, actual, actual_val,"
					+ " all_day_event, currency, reference, revised, mean_actual, mean_previous, last_update) "
					+ "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
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
							DailyFxNews o = gson.fromJson(je, DailyFxNews.class);
							// Check if currency code match with acceptable codes
							if (Util.isCurrencyUsable(o.getCurrency())) {
								String check = "SELECT COUNT(*) AS 'n' FROM " + tblPrefix + this.getYear()
										+ " AS o WHERE o.id = " + Long.parseLong(o.getId());
								Statement stmt = getConn().createStatement();
								ResultSet rs = stmt.executeQuery(check);
								if (rs.next()) {
									tot = rs.getInt("n");
								}
								if (tot > 0) {
									// Skip
								} else {
									ps.setString(1, UUID.randomUUID().toString());
									ps.setLong(2, Long.parseLong(o.getId()));
									if (o.getTicker() != null && o.getTicker().length() > 0) {
										ps.setString(3, o.getTicker());
									} else {
										ps.setObject(3, null);
									}
									if (o.getSymbol() != null && o.getSymbol().length() > 0) {
										ps.setString(4, o.getSymbol());
									} else {
										ps.setObject(4, null);
									}
									ps.setLong(5, add7Hours(sdf.parse(o.getDate()).getTime() / 1000L));
									ps.setString(6, o.getTitle());
									if (o.getDescription() != null && o.getDescription().length() > 0) {
										ps.setString(7, o.getDescription());
									} else {
										ps.setObject(7, null);
									}
									ps.setString(8, o.getImportance());
									if (o.getPrevious() != null && o.getPrevious().length() > 0) {
										ps.setString(9, o.getPrevious());
										String pre = o.getPrevious().replaceAll("[^0-9.-]", "");
										long count = pre.chars().filter(ch -> ch == '.').count();
										if (count > 1) {
											for (int n = 0; n < count - 1; n++) {
												pre = pre.replaceFirst("\\.", "");
											}
										}
										ps.setDouble(10, Double.parseDouble(pre));
									} else {
										ps.setObject(9, null);
										ps.setObject(10, null);
									}
									if (o.getForecast() != null && o.getForecast().length() > 0) {
										ps.setString(11, o.getForecast());
										String fore = o.getForecast().replaceAll("[^0-9.-]", "");
										long count = fore.chars().filter(ch -> ch == '.').count();
										if (count > 1) {
											for (int n = 0; n < count - 1; n++) {
												fore = fore.replaceFirst("\\.", "");
											}
										}
										ps.setDouble(12, Double.parseDouble(fore));
									} else {
										ps.setObject(11, null);
										ps.setObject(12, null);
									}
									String countryCode = Util.to2DCountryCode(o.getCountry());
									ps.setString(13, countryCode);
									if (o.getActual() != null && o.getActual().length() > 0) {
										ps.setString(14, o.getActual());
										String act = o.getActual().replaceAll("[^0-9.-]", "");
										long count = act.chars().filter(ch -> ch == '.').count();
										if (count > 1) {
											for (int n = 0; n < count - 1; n++) {
												act = act.replaceFirst("\\.", "");
											}
										}
										ps.setDouble(15, Double.parseDouble(act));
									} else {
										ps.setObject(14, null);
										ps.setObject(15, null);
									}
									ps.setBoolean(16, o.getAllDayEvent());
									ps.setString(17, o.getCurrency());
									if (o.getReference() != null && o.getReference().length() > 0) {
										ps.setString(18, o.getReference());
									} else {
										ps.setObject(18, null);
									}
									if (o.getRevised() != null && o.getRevised().length() > 0) {
										ps.setString(19, o.getRevised());
									} else {
										ps.setObject(19, null);
									}
									if (o.getEconomicMeaning() != null) {
										if (o.getEconomicMeaning().getActual() != null
												&& o.getEconomicMeaning().getActual().length() > 0) {
											ps.setString(20, o.getEconomicMeaning().getActual());
										} else {
											ps.setObject(20, null);
										}
										if (o.getEconomicMeaning().getPrevious() != null
												&& o.getEconomicMeaning().getPrevious().length() > 0) {
											ps.setString(21, o.getEconomicMeaning().getPrevious());
										} else {
											ps.setObject(21, null);
										}
									} else {
										ps.setObject(20, null);
										ps.setObject(21, null);
									}
									ps.setString(22, o.getLastUpdate());
									ps.execute();
								}
							}
							reader.close();
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

}
