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

public class PairTickTblsGenerator extends DbRelatedWorkerBase {

	private final String root = ".\\src\\main\\resources\\";
	private final String sqlFile = "pair_tick.sql";
	private String[] timeframes;
	private String[] pairs;
	private int[] years;

	public PairTickTblsGenerator(String dbConnString, String dbUsername, String dbPassword, int[] years, String[] pairs,
			String[] timeframes) {
		this.setDbConnString(dbConnString);
		this.setDbUsername(dbUsername);
		this.setDbPassword(dbPassword);
		this.timeframes = timeframes;
		this.pairs = pairs;
		this.years = years;
	}

	@Override
	public WorkReport execute(WorkContext ctx) {
		System.out.println("EXECUTING CREATE PAIR TICK TABLES");
		try {
			String file = root + sqlFile;
			File sql = new File(file);
			if (sql.isFile()) {
				for (int year : this.years) {
					for (String pair : this.pairs) {
						for (String tf : this.timeframes) {
							String raw = new String(Files.readAllBytes(sql.toPath()));
							String create = raw.replaceAll("\\$1", pair.toLowerCase() + "").replaceAll("\\$2", tf)
									.replaceAll("\\$3", year + "");
							Statement stmt = this.getConn().createStatement();
//							System.out.println(create);
							stmt.execute(create);
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
		PairTickTblsGenerator worker = new PairTickTblsGenerator(Config.CONN_STRING, Config.DB_USERNAME,
				Config.DB_PASSWORD, Config.YEARS, Config.PAIRS, Config.TFRAMES_STR);

		WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
		WorkContext ctx = new WorkContext();
		WorkFlow flow2 = SequentialFlow.Builder.aNewSequentialFlow().named("Creating pair prices & ticks tables")
				.execute(worker).build();
		WorkReport report2 = engine.run(flow2, ctx);
		System.out.println(report2.getStatus());
	}

}
