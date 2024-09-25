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

public class PairPriceVolatilityTblsDroper extends DbRelatedWorkerBase {

	private String[] pairs;
	private int[] years;

	public PairPriceVolatilityTblsDroper(String dbConnString, String dbUsername, String dbPassword, int[] years,
			String[] pairs) {
		this.setDbConnString(dbConnString);
		this.setDbUsername(dbUsername);
		this.setDbPassword(dbPassword);
		this.pairs = pairs;
		this.years = years;
	}

	@Override
	public WorkReport execute(WorkContext ctx) {
		System.out.println("EXECUTING CREATE PAIR PRICE TABLES");
		try {
			for (String pair : this.pairs) {
				for (int yr : years) {
					try {
						String drop = "DROP TABLE " + (pair + "_vol_" + yr).toLowerCase() + ";";
						System.out.println(drop);
						Statement stmt = this.getConn().createStatement();
						stmt.execute(drop);
					} catch (Exception er) {
						er.printStackTrace();
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
		PairPriceVolatilityTblsDroper worker = new PairPriceVolatilityTblsDroper(Config.CONN_STRING, Config.DB_USERNAME,
				Config.DB_PASSWORD, Config.YEARS, Config.PAIRS);

		WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
		WorkContext ctx = new WorkContext();
		WorkFlow flow = SequentialFlow.Builder.aNewSequentialFlow().named("Creating News Provider Tables")
				.execute(worker).build();
		WorkReport report = engine.run(flow, ctx);
		System.out.println(report.getStatus());

	}
}
