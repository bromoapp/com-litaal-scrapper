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

public class DukasNewsResultCalc extends DbRelatedWorkerBase {

	public DukasNewsResultCalc(String dbConnString, String dbUsername, String dbPassword, String folder, int year) {
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
			String selectSql = "SELECT a.uuid, a.actual_norm, a.previous_norm, a. forecast_norm, a.effect"
					+ " FROM news_dukascopy_" + this.getYear()
					+ " AS a WHERE a.actual_norm IS NOT NULL AND a.previous_norm IS NOT NULL ORDER BY date ASC;";

			String insertSql = "INSERT INTO news_result_dukascopy_" + this.getYear()
					+ " (uuid, avp, avp_diff, avf, avf_diff, fvp, fvp_diff, effect) VALUES (?,?,?,?,?,?,?,?)";

			Statement selectPs = this.getConn().createStatement();
			ResultSet selectRs = selectPs.executeQuery(selectSql);
			while (selectRs.next()) {
				boolean act_vs_prev = false;
				boolean act_vs_fore = false;
				boolean fore_vs_prev = false;
				Fact fact = null;
				String uuid = null;
				BigDecimal actual_norm = null;
				BigDecimal previous_norm = null;
				BigDecimal forecast_norm = null;
				Integer effect = null;

				uuid = selectRs.getString("uuid");

				int total = 0;
				String countSql = "SELECT COUNT(*) AS 'total' FROM news_result_dukascopy_" + this.getYear()
						+ " AS a WHERE a.uuid = '" + uuid + "'";
				Statement countPs = this.getConn().createStatement();
				ResultSet countRs = countPs.executeQuery(countSql);
				countRs.next();
				total = countRs.getInt("total");

				if (total == 0) {
					if (selectRs.getString("actual_norm") != null) {
						fact = new Fact();
						fact.setUuid(uuid);

						actual_norm = selectRs.getBigDecimal("actual_norm");
						if (selectRs.getString("previous_norm") != null) {
							previous_norm = selectRs.getBigDecimal("previous_norm");
							act_vs_prev = true;
						}
						if (selectRs.getString("forecast_norm") != null) {
							forecast_norm = selectRs.getBigDecimal("forecast_norm");
							act_vs_fore = true;
						}
						if (previous_norm != null && forecast_norm != null) {
							fore_vs_prev = true;
						}
						if (selectRs.getString("effect") != null) {
							effect = selectRs.getInt("effect");
						}
					}
					if (act_vs_prev) {// ------------- AVP fact -----------
						if (actual_norm.compareTo(previous_norm) == 0) {
							fact.setAvp(EReality.AEP);
							fact.setAvpDiff(0.0);
						}
						if (actual_norm.compareTo(previous_norm) > 0) {
							fact.setAvp(EReality.ABP);
							fact.setAvpDiff(actual_norm.subtract(previous_norm).doubleValue());
						}
						if (actual_norm.compareTo(previous_norm) < 0) {
							fact.setAvp(EReality.ASP);
							fact.setAvpDiff(previous_norm.subtract(actual_norm).doubleValue());
						}
					}
					if (act_vs_fore) {// ------------- AVF fact -----------
						if (actual_norm.compareTo(forecast_norm) == 0) {
							fact.setAvf(EReality.AEF);
							fact.setAvfDiff(0.0);
						}
						if (actual_norm.compareTo(forecast_norm) > 0) {
							fact.setAvf(EReality.ABF);
							fact.setAvfDiff(actual_norm.subtract(forecast_norm).doubleValue());
						}
						if (actual_norm.compareTo(forecast_norm) < 0) {
							fact.setAvf(EReality.ASF);
							fact.setAvfDiff(forecast_norm.subtract(actual_norm).doubleValue());
						}
					}
					if (fore_vs_prev) {// ------------- FVP fact -----------
						if (forecast_norm.compareTo(previous_norm) == 0) {
							fact.setFvp(EReality.FEP);
							fact.setFvpDiff(0.0);
						}
						if (forecast_norm.compareTo(previous_norm) > 0) {
							fact.setFvp(EReality.FBP);
							fact.setFvpDiff(forecast_norm.subtract(previous_norm).doubleValue());
						}
						if (forecast_norm.compareTo(previous_norm) < 0) {
							fact.setFvp(EReality.FSP);
							fact.setFvpDiff(previous_norm.subtract(forecast_norm).doubleValue());
						}
					}
					fact.setEffect(effect);
//					System.out.println(fact.toString());
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
			this.getConn().close();
			return new DefaultWorkReport(WorkStatus.COMPLETED, ctx);
		} catch (Exception e) {
			e.printStackTrace();
			return new DefaultWorkReport(WorkStatus.FAILED, ctx);
		}
	}

}
