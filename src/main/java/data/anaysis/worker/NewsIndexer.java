package data.anaysis.worker;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

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

public class NewsIndexer extends DbRelatedWorkerBase {

	private static String clearSql = "TRUNCATE indexed_news";
	private static String insertSql = "INSERT INTO indexed_news (title, country, currency, impact, provider) VALUES (?,?,?,?,?)";

	private String[] providers;
	private static HashMap<String, String> sqls;

	public NewsIndexer(String dbConnString, String dbUsername, String dbPassword, String[] providers) {
		this.setDbConnString(dbConnString);
		this.setDbUsername(dbUsername);
		this.setDbPassword(dbPassword);
		this.providers = providers;
	}

	@Override
	public WorkReport execute(WorkContext ctx) {
		try {
			Statement cleanSql = this.getConn().createStatement();
			cleanSql.execute(clearSql);
			for (String provider : this.providers) {
				String tblName = "news_" + provider;
				String sql = String.format(getSqlByProvider().get(provider), tblName);
				System.out.println(sql);
				Statement selectStm = this.getConn().createStatement();
				ResultSet selectRs = selectStm.executeQuery(sql);
				PreparedStatement ps = this.getConn().prepareStatement(insertSql);
				while (selectRs.next()) {
					String title = null;
					String country = null;
					String currency = null;
					int impact = 0;
					if (provider.equalsIgnoreCase("dailyfx")) {
						title = selectRs.getString("title");
						country = selectRs.getString("country");
						currency = selectRs.getString("currency");
						impact = getNewsImpactAsInt(selectRs.getString("importance"));
					}
					if (provider.equalsIgnoreCase("dukascopy")) {
						title = selectRs.getString("title");
						if (selectRs.getString("periodicity") != null) {
							title = title + " | " + selectRs.getString("periodicity");
						}
						country = selectRs.getString("country");
						currency = selectRs.getString("currency");
						impact = selectRs.getInt("impact");
					}
					if (provider.equalsIgnoreCase("fxstreet")) {
						title = selectRs.getString("name");
						country = selectRs.getString("country_code");
						currency = selectRs.getString("currency_code");
						impact = getNewsImpactAsInt(selectRs.getString("volatility"));
					}
					if (provider.equalsIgnoreCase("mql5")) {
						title = selectRs.getString("event_name");
						country = selectRs.getString("country");
						currency = selectRs.getString("currency_code");
						impact = getNewsImpactAsInt(selectRs.getString("importance"));
					}
					ps.setString(1, title);
					ps.setString(2, country);
					ps.setString(3, currency);
					ps.setInt(4, impact);
					ps.setString(5, provider);
					ps.addBatch();
				}
				ps.executeLargeBatch();
			}
			this.getConn().close();
			return new DefaultWorkReport(WorkStatus.COMPLETED, ctx);
		} catch (Exception e) {
			e.printStackTrace();
			return new DefaultWorkReport(WorkStatus.FAILED, ctx);
		}
	}

	private HashMap<String, String> getSqlByProvider() {
		if (sqls == null) {
			sqls = new HashMap<>();
			sqls.put("dukascopy",
					"SELECT DISTINCT(o.title), o.periodicity, o.country, o.currency, o.impact FROM %s AS o WHERE o.title IS NOT NULL ORDER BY o.title ASC");
			sqls.put("mql5",
					"SELECT DISTINCT(o.event_name), o.country, o.currency_code, o.importance FROM %s AS o WHERE o.event_name IS NOT NULL ORDER BY o.event_name ASC;");
			sqls.put("fxstreet",
					"SELECT DISTINCT(o.name), o.country_code, o.currency_code, o.volatility FROM %s AS o WHERE o.name IS NOT NULL ORDER BY o.name ASC;");
			sqls.put("dailyfx",
					"SELECT DISTINCT(o.title), o.country, o.currency, o.importance FROM %s AS o WHERE o.title IS NOT NULL ORDER BY o.title ASC;");
		}
		return sqls;
	}

	private int getNewsImpactAsInt(String impact) {
		if (impact.trim().equalsIgnoreCase("none")) {
			return -1;
		}
		if (impact.trim().equalsIgnoreCase("low")) {
			return 0;
		}
		if (impact.trim().equalsIgnoreCase("medium")) {
			return 1;
		}
		if (impact.trim().equalsIgnoreCase("high")) {
			return 2;
		}
		return 0;
	}

	public static void main(String[] args) {
		NewsIndexer worker = new NewsIndexer(Config.CONN_STRING, Config.DB_USERNAME, Config.DB_PASSWORD,
				Config.NEWS_PROVIDERS);

		WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
		WorkFlow flow = SequentialFlow.Builder.aNewSequentialFlow().named("Indexing News").execute(worker).build();
		WorkContext ctx = new WorkContext();
		WorkReport report = engine.run(flow, ctx);
		System.out.println(report.getStatus());
	}

}
