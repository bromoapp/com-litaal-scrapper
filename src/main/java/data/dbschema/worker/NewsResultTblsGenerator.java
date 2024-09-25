package data.dbschema.worker;

import java.io.File;
import java.nio.file.Files;
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

public class NewsResultTblsGenerator extends DbRelatedWorkerBase {

	private final String root = ".\\src\\main\\resources\\";
	private final String sqlFile = "news_result.sql";
	private String[] providers;
	private int[] years;

	public NewsResultTblsGenerator(String dbConnString, String dbUsername, String dbPassword, int[] years,
			String[] providers) {
		this.setDbConnString(dbConnString);
		this.setDbUsername(dbUsername);
		this.setDbPassword(dbPassword);
		this.providers = providers;
		this.years = years;
	}

	@Override
	public WorkReport execute(WorkContext ctx) {
		System.out.println("EXECUTING CREATE NEWS RESULTS TABLES");
		try {
			for (String provider : this.providers) {
				File sql = new File(root + sqlFile);
				if (sql.isFile()) {
					for (int year : this.years) {
						String raw = new String(Files.readAllBytes(sql.toPath()));
						String content = raw.replaceAll("\\$1", provider + "").replaceAll("\\$2", "" + year);
						Statement stmt = this.getConn().createStatement();
						stmt.execute(content);
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

	public static void main(String[] args) {
		NewsResultTblsGenerator worker = new NewsResultTblsGenerator(Config.CONN_STRING, Config.DB_USERNAME,
				Config.DB_PASSWORD, Config.YEARS, Config.NEWS_PROVIDERS);

		WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
		WorkContext ctx = new WorkContext();
		WorkFlow flow = SequentialFlow.Builder.aNewSequentialFlow().named("Creating News Result Tables").execute(worker)
				.build();
		WorkReport report = engine.run(flow, ctx);
		System.out.println(report.getStatus());
	}

}
