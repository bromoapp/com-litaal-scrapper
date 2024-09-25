package data.scrapping.news.worker;

import java.io.File;
import java.io.FileReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;

import org.jeasy.flows.work.DefaultWorkReport;
import org.jeasy.flows.work.WorkContext;
import org.jeasy.flows.work.WorkReport;
import org.jeasy.flows.work.WorkStatus;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import data.scrapping.common.helper.Util;
import data.scrapping.common.worker.DbRelatedWorkerBase;

public class DukasNewsFiles2Db extends DbRelatedWorkerBase {
	private final String tblPrefix = "news_dukascopy_";

	public DukasNewsFiles2Db(String folder, String dbConnString, String dbUsername, String dbPassword, int year) {
		this.setDbConnString(dbConnString);
		this.setDbUsername(dbUsername);
		this.setDbPassword(dbPassword);
		this.setPath(folder);
		this.setYear(year);
	}

	@Override
	public WorkReport execute(WorkContext ctx) {
		System.out.println("START PERSISTING TO DB...");
		try {
			String sql1 = "INSERT INTO " + tblPrefix + this.getYear()
					+ " (uuid, date, id, country, currency, title, periodicity, show_description, description, ";
			sql1 += "impact, actual, actual_norm, forecast, forecast_norm, previous, previous_norm, value_order, value_format, ";
			sql1 += "tag, historical_count, effect, dd_source, dd_measures, dd_usual_effect, dd_frequency, dd_next_release, ";
			sql1 += "dd_derived_via, dd_acro) ";
			sql1 += "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			PreparedStatement ps = this.getConn().prepareStatement(sql1);

			File yearDir = new File(this.getPath() + this.getYear());
			if (yearDir.isDirectory()) {
				File[] files = yearDir.listFiles();
				for (File csvFile : files) {
					if (csvFile.isFile()) {
						System.out.println("READING: " + csvFile.getName());
						FileReader reader = new FileReader(csvFile);
						CSVParser parser = new CSVParserBuilder().withSeparator('~').build();
						CSVReader csvReader = new CSVReaderBuilder(reader).withCSVParser(parser).build();
						List<String[]> all = csvReader.readAll();
						for (String[] raw : all) {
							try {
								// Check if currency code match with acceptable codes
								if (Util.isCurrencyUsable(raw[3].trim())) {
									int tot = 0;
									long id = Long.parseLong(raw[1].trim());
									String sql2 = "SELECT COUNT(*) AS 'total' FROM news_dukascopy_" + this.getYear()
											+ " AS o WHERE o.id = " + id;
									Statement stmt = this.getConn().createStatement();
									ResultSet rs = stmt.executeQuery(sql2);
									if (rs.next()) {
										tot = rs.getInt("total");
									}
									if (tot > 0) {
//										System.out.println("SKIP ID: " + id);
									} else {
										// ---- uuid
										ps.setString(1, UUID.randomUUID().toString());
										// ---- date
										ps.setLong(2, Long.parseLong(raw[0].trim()));
										// ---- id
										ps.setLong(3, Long.parseLong(raw[1].trim()));
										// ---- country
										if (raw[2].trim().equalsIgnoreCase("GB")) {
											ps.setString(4, Util.to2DCountryCode(raw[2].trim()));
										} else {
											ps.setString(4, raw[2].trim());
										}
										// ---- currency
										ps.setString(5, raw[3].trim());
										// ---- title
										ps.setString(6, raw[4].trim());
										// ---- periodicity
										if (raw[5] != null && raw[5].length() > 0) {
											ps.setString(7, raw[5].trim());
											if (raw[5].trim().equalsIgnoreCase("null")) {
												ps.setString(7, null);
											}
										} else {
											ps.setString(7, null);
										}
										// ---- show_desc
										if (raw[6] != null && raw[6].length() > 0) {
											ps.setBoolean(8, Boolean.parseBoolean(raw[6].trim()));
										} else {
											ps.setBoolean(8, false);
										}
										// ---- desc
										if (raw[7] != null && raw[7].length() > 0) {
											ps.setString(9, raw[7].trim());
										} else {
											ps.setString(9, null);
										}
										// ---- impact
										if (raw[8] != null && raw[8].length() > 0) {
											ps.setInt(10, Integer.parseInt(raw[8].trim()));
										} else {
											ps.setInt(10, -1);
										}
										// ---- actual
										if (raw[9] != null && raw[9].length() > 0) {
											ps.setString(11, raw[9].trim());
											if (raw[9].trim().equalsIgnoreCase("null")) {
												ps.setString(11, null);
											}
										} else {
											ps.setString(11, null);
										}
										// ---- actual_norm
										if (raw[10] != null && raw[10].length() > 0) {
											ps.setString(12, raw[10].trim());
											if (raw[10].trim().equalsIgnoreCase("null")) {
												ps.setString(12, null);
											}
										} else {
											ps.setString(12, null);
										}
										// ---- forecast
										if (raw[11] != null && raw[11].length() > 0) {
											ps.setString(13, raw[11].trim());
											if (raw[11].trim().equalsIgnoreCase("null")) {
												ps.setString(13, null);
											}
										} else {
											ps.setString(13, null);
										}
										// ---- forecast_norm
										if (raw[12] != null && raw[12].length() > 0) {
											ps.setString(14, raw[12].trim());
											if (raw[12].trim().equalsIgnoreCase("null")) {
												ps.setString(14, null);
											}
										} else {
											ps.setString(14, null);
										}
										// ---- previous
										if (raw[13] != null && raw[13].length() > 0) {
											ps.setString(15, raw[13].trim());
											if (raw[13].trim().equalsIgnoreCase("null")) {
												ps.setString(15, null);
											}
										} else {
											ps.setString(15, null);
										}
										// ---- previous_norm
										if (raw[14] != null && raw[14].length() > 0) {
											ps.setString(16, raw[14].trim());
											if (raw[14].trim().equalsIgnoreCase("null")) {
												ps.setString(16, null);
											}
										} else {
											ps.setString(16, null);
										}
										// ---- value_order
										if (raw[15] != null && raw[15].length() > 0) {
											ps.setString(17, raw[15].trim());
											if (raw[15].trim().equalsIgnoreCase("null")) {
												ps.setString(17, null);
											}
										} else {
											ps.setString(17, null);
										}
										// ---- value_order_format
										if (raw[16] != null && raw[16].length() > 0) {
											ps.setString(18, raw[16].trim());
											if (raw[16].trim().equalsIgnoreCase("null")) {
												ps.setString(18, null);
											}
										} else {
											ps.setString(18, null);
										}
										// ---- tag
										if (raw[17] != null && raw[17].length() > 0) {
											ps.setString(19, raw[17].trim());
											if (raw[17].trim().equalsIgnoreCase("null")) {
												ps.setString(19, null);
											}
										} else {
											ps.setString(19, null);
										}
										// ---- historical_count
										if (raw[18] != null && raw[18].length() > 0) {
											ps.setInt(20, Integer.parseInt(raw[18]));
										} else {
											ps.setInt(20, 0);
										}
										// ---- effect
										if (raw[19] != null && raw[19].length() > 0) {
											ps.setInt(21, Integer.parseInt(raw[19]));
										} else {
											ps.setInt(21, 0);
										}
										// ---- dd_source
										if (raw[20] != null && raw[20].length() > 0) {
											ps.setString(22, raw[20].trim());
											if (raw[20].trim().equalsIgnoreCase("null")) {
												ps.setString(22, null);
											}
										} else {
											ps.setString(22, null);
										}
										// ---- dd_measures
										if (raw[21] != null && raw[21].length() > 0) {
											ps.setString(23, raw[21].trim());
											if (raw[21].trim().equalsIgnoreCase("null")) {
												ps.setString(23, null);
											}
										} else {
											ps.setString(23, null);
										}
										// ---- dd_usual_effect
										if (raw[22] != null && raw[22].length() > 0) {
											ps.setString(24, raw[22].trim());
											if (raw[22].trim().equalsIgnoreCase("null")) {
												ps.setString(24, null);
											}
										} else {
											ps.setString(24, null);
										}
										// ---- dd_frequency
										if (raw[23] != null && raw[23].length() > 0) {
											ps.setString(25, raw[23].trim());
											if (raw[23].trim().equalsIgnoreCase("null")) {
												ps.setString(25, null);
											}
										} else {
											ps.setString(25, null);
										}
										// ---- dd_next_release
										if (raw[24] != null && raw[24].length() > 0) {
											ps.setString(26, raw[24].trim());
											if (raw[24].trim().equalsIgnoreCase("null")) {
												ps.setString(26, null);
											}
										} else {
											ps.setString(26, null);
										}
										// ---- dd_derived_via
										if (raw[25] != null && raw[25].length() > 0) {
											ps.setString(27, raw[25].trim());
											if (raw[25].trim().equalsIgnoreCase("null")) {
												ps.setString(27, null);
											}
										} else {
											ps.setString(27, null);
										}
										// ---- dd_derived_via
										if (raw[26] != null && raw[26].length() > 0) {
											ps.setString(28, raw[26].trim());
											if (raw[26].trim().equalsIgnoreCase("null")) {
												ps.setString(28, null);
											}
										} else {
											ps.setString(28, null);
										}
										ps.execute();
									}
								}
							} catch (Exception e) {
								throw e;
							}
						}
						csvReader.close();
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
