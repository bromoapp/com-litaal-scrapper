package data.dbschema.worker;

import java.sql.Statement;

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

public class NewsViewsGenerator extends DbRelatedWorkerBase {

	private String[] providers;
	private int[] years;

	public NewsViewsGenerator(String dbConnString, String dbUsername, String dbPassword, int[] years,
			String[] providers) {
		this.setDbConnString(dbConnString);
		this.setDbUsername(dbUsername);
		this.setDbPassword(dbPassword);
		this.providers = providers;
		this.years = years;
	}

	@Override
	public WorkReport execute(WorkContext ctx) {
		System.out.println("EXECUTING CREATE NEWS PROVIDER VIEWS");
		try {
			for (String provider : this.providers) {
				StringBuffer sb = new StringBuffer("CREATE OR REPLACE VIEW news_" + provider + " AS\n");
				for (int year : this.years) {
					sb.append("   SELECT * FROM news_" + provider + "_" + year + "\n");
					sb.append("   UNION ALL\n");
				}
				String ddl = sb.toString().substring(0, sb.toString().length() - "UNION ALL\n".length()).concat(";");
				Statement stmt = this.getConn().createStatement();
				stmt.execute(ddl);
			}
			this.getConn().close();
			return new DefaultWorkReport(WorkStatus.COMPLETED, ctx);
		} catch (Exception e) {
			e.printStackTrace();
			return new DefaultWorkReport(WorkStatus.FAILED, ctx);
		}
	}

	public static int[] YEARS = { 2023, 2022, 2021, 2020 };
	public static String[] NEWS_PROVIDERS = { "dailyfx", "dukascopy", "fxstreet", "mql5" };

	public static void main(String[] args) {
		NewsViewsGenerator worker = new NewsViewsGenerator(Config.CONN_STRING, Config.DB_USERNAME, Config.DB_PASSWORD,
				YEARS, NEWS_PROVIDERS);

		WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
		WorkContext ctx = new WorkContext();
		WorkFlow flow = SequentialFlow.Builder.aNewSequentialFlow().named("Creating Viwws on News Provider Tables")
				.execute(worker).build();
		WorkReport report = engine.run(flow, ctx);
		System.out.println(report.getStatus());
	}

}
