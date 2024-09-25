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

public class PairPriceTblsGenerator extends DbRelatedWorkerBase {

	private final String root = ".\\src\\main\\resources\\";
	private final String sqlFile = "pair_price.sql";
	private String[] timeframes;
	private String[] pairs;
	private int[] years;

	public PairPriceTblsGenerator(String dbConnString, String dbUsername, String dbPassword, int[] years,
			String[] pairs, String[] timeframes) {
		this.setDbConnString(dbConnString);
		this.setDbUsername(dbUsername);
		this.setDbPassword(dbPassword);
		this.timeframes = timeframes;
		this.pairs = pairs;
		this.years = years;
	}

	@Override
	public WorkReport execute(WorkContext ctx) {
		System.out.println("EXECUTING CREATE PAIR PRICE TABLES");
		try {
			String file = root + sqlFile;
			File sql = new File(file);
			if (sql.isFile()) {
				for (int year : this.years) {
					for (String pair : this.pairs) {
						for (String tf : this.timeframes) {
							String raw = new String(Files.readAllBytes(sql.toPath()));
							String content = raw.replaceAll("\\$1", pair.toLowerCase() + "").replaceAll("\\$2", tf)
									.replaceAll("\\$3", year + "");
							Statement stmt = this.getConn().createStatement();
							stmt.execute(content);
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

	public static void main(String[] args) {
		PairPriceTblsGenerator worker = new PairPriceTblsGenerator(Config.CONN_STRING, Config.DB_USERNAME,
				Config.DB_PASSWORD, Config.YEARS, Config.PAIRS, Config.TFRAMES_STR);

		WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
		WorkContext ctx = new WorkContext();
		WorkFlow flow = SequentialFlow.Builder.aNewSequentialFlow().named("Creating News Provider Tables")
				.execute(worker).build();
		WorkReport report = engine.run(flow, ctx);
		System.out.println(report.getStatus());

	}
}
