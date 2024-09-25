package data.scrapping.news.worker;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.jeasy.flows.work.DefaultWorkReport;
import org.jeasy.flows.work.WorkContext;
import org.jeasy.flows.work.WorkReport;
import org.jeasy.flows.work.WorkStatus;

import data.scrapping.common.worker.DbRelatedWorkerBase;
import data.scrapping.news.vo.EReality;
import data.scrapping.news.vo.Fact;

public class FxStreetNewsResultCalc extends DbRelatedWorkerBase {

	public FxStreetNewsResultCalc(String dbConnString, String dbUsername, String dbPassword, String folder, int year) {
		this.setDbConnString(dbConnString);
		this.setDbUsername(dbUsername);
		this.setDbPassword(dbPassword);
		this.setPath(folder);
		this.setYear(year);
	}

	@Override
	public WorkReport execute(WorkContext ctx) {
		System.out.println("START CALCULATING...");
		try {
			System.out.println("PROCESSING YEAR DATA: " + this.getYear());
			String selectSql = "SELECT a.uuid, a.actual, a.previous, a.consensus, a.is_better_than_expected "
					+ "FROM news_fxstreet_" + this.getYear() + " AS a WHERE a.actual IS NOT NULL AND a.previous "
					+ "IS NOT NULL ORDER BY date ASC;";
			String insertSql = "INSERT INTO news_result_fxstreet_" + this.getYear()
					+ " (uuid, avp, avp_diff, avf, avf_diff, fvp, fvp_diff, effect) VALUES (?,?,?,?,?,?,?,?)";
			Statement selectPs = this.getConn().createStatement();
			ResultSet selectRs = selectPs.executeQuery(selectSql);
			while (selectRs.next()) {
				boolean act_vs_prev = false;
				boolean act_vs_fore = false;
				boolean fore_vs_prev = false;
				Fact fact = null;
				String uuid = null;
				BigDecimal actual = null;
				BigDecimal previous = null;
				BigDecimal forecast = null;
				Boolean effect = null;

				uuid = selectRs.getString("uuid");

				int total = 0;
				String countSql = "SELECT COUNT(*) AS 'total' FROM news_result_fxstreet_" + this.getYear()
						+ " AS a WHERE a.uuid = '" + uuid + "'";
				Statement countPs = this.getConn().createStatement();
				ResultSet countRs = countPs.executeQuery(countSql);
				countRs.next();
				total = countRs.getInt("total");
				if (total == 0) {
					if (selectRs.getString("actual") != null) {
						fact = new Fact();
						fact.setUuid(uuid);

						actual = selectRs.getBigDecimal("actual");
						if (selectRs.getString("previous") != null) {
							previous = selectRs.getBigDecimal("previous");
							act_vs_prev = true;
						}
						if (selectRs.getString("consensus") != null) {
							forecast = selectRs.getBigDecimal("consensus");
							act_vs_fore = true;
						}
						if (previous != null && forecast != null) {
							fore_vs_prev = true;
						}
						if (selectRs.getString("is_better_than_expected") != null) {
							effect = selectRs.getBoolean("is_better_than_expected");
						}
						if (act_vs_prev) {// ------------- AVP fact -----------
							if (actual.compareTo(previous) == 0) {
								fact.setAvp(EReality.AEP);
								fact.setAvpDiff(0.0);
							}
							if (actual.compareTo(previous) > 0) {
								fact.setAvp(EReality.ABP);
								fact.setAvpDiff(actual.subtract(previous).doubleValue());
							}
							if (actual.compareTo(previous) < 0) {
								fact.setAvp(EReality.ASP);
								fact.setAvpDiff(previous.subtract(actual).doubleValue());
							}
						}
						if (act_vs_fore) {// ------------- AVF fact -----------
							if (actual.compareTo(forecast) == 0) {
								fact.setAvf(EReality.AEF);
								fact.setAvfDiff(0.0);
							}
							if (actual.compareTo(forecast) > 0) {
								fact.setAvf(EReality.ABF);
								fact.setAvfDiff(actual.subtract(forecast).doubleValue());
							}
							if (actual.compareTo(forecast) < 0) {
								fact.setAvf(EReality.ASF);
								fact.setAvfDiff(forecast.subtract(actual).doubleValue());
							}
						}
						if (fore_vs_prev) {// ------------- FVP fact -----------
							if (forecast.compareTo(previous) == 0) {
								fact.setFvp(EReality.FEP);
								fact.setFvpDiff(0.0);
							}
							if (forecast.compareTo(previous) > 0) {
								fact.setFvp(EReality.FBP);
								fact.setFvpDiff(forecast.subtract(previous).doubleValue());
							}
							if (forecast.compareTo(previous) < 0) {
								fact.setFvp(EReality.FSP);
								fact.setFvpDiff(previous.subtract(forecast).doubleValue());
							}
						}
						fact.setEffect(0);
						if (effect != null) {
							if (effect) {
								fact.setEffect(1);
							} else {
								fact.setEffect(-1);
							}
						} else {
							fact.setEffect(0);
						}
						if (fact != null) {
							try {
								PreparedStatement insertPs = this.getConn().prepareStatement(insertSql);
								insertPs.setString(1, fact.getUuid());
								insertPs.setString(2, fact.getAvp().name());
								insertPs.setDouble(3, fact.getAvpDiff());
								if (fact.getAvf() != null) {
									insertPs.setString(4, fact.getAvf().name());
									insertPs.setDouble(5, fact.getAvfDiff());
								} else {
									insertPs.setString(4, null);
									insertPs.setDouble(5, 0.0);
								}
								if (fact.getFvp() != null) {
									insertPs.setString(6, fact.getFvp().name());
									insertPs.setDouble(7, fact.getFvpDiff());
								} else {
									insertPs.setString(6, null);
									insertPs.setDouble(7, 0.0);
								}
								insertPs.setInt(8, fact.getEffect());
								insertPs.execute();
							} catch (Exception e) {
								e.printStackTrace();
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

}
