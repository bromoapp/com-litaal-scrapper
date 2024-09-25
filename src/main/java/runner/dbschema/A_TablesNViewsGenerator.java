package runner.dbschema;

import org.jeasy.flows.engine.WorkFlowEngine;
import org.jeasy.flows.engine.WorkFlowEngineBuilder;
import org.jeasy.flows.work.WorkContext;
import org.jeasy.flows.work.WorkReport;
import org.jeasy.flows.workflow.SequentialFlow;
import org.jeasy.flows.workflow.WorkFlow;

import data.dbschema.worker.NewsResultTblsGenerator;
import data.dbschema.worker.NewsTblsGenerator;
import data.dbschema.worker.NewsViewsGenerator;
import data.dbschema.worker.OtherTblsGenerator;
import data.dbschema.worker.PairPriceTblsGenerator;
import data.dbschema.worker.PairTickTblsGenerator;
import runner.common.Config;

public class A_TablesNViewsGenerator {

	public static int[] YEARS_FOR_VIEWS = { 2023, 2022, 2021, 2020 };

	public static void main(String[] args) {
		WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
		WorkContext ctx = new WorkContext();

		// --- Creating news table by provider per year ---
		NewsTblsGenerator newsTblsGenerator = new NewsTblsGenerator(Config.CONN_STRING, Config.DB_USERNAME,
				Config.DB_PASSWORD, Config.YEARS, Config.NEWS_PROVIDERS);

		// --- Creating news views by provider ---
		NewsViewsGenerator newsViewsGenerator = new NewsViewsGenerator(Config.CONN_STRING, Config.DB_USERNAME,
				Config.DB_PASSWORD, YEARS_FOR_VIEWS, Config.NEWS_PROVIDERS);

		// --- Creating news result table by provider per year ---
		NewsResultTblsGenerator newsResultTblsGenerator = new NewsResultTblsGenerator(Config.CONN_STRING,
				Config.DB_USERNAME, Config.DB_PASSWORD, Config.YEARS, Config.NEWS_PROVIDERS);

		WorkFlow flow1 = SequentialFlow.Builder.aNewSequentialFlow().named("Creating News Tables & Views")
				.execute(newsTblsGenerator).then(newsViewsGenerator).then(newsResultTblsGenerator).build();
		WorkReport report1 = engine.run(flow1, ctx);
		System.out.println(report1.getStatus());

		// --- Creating pair price & ticks table per year ---
		PairPriceTblsGenerator pairPriceTblsGenerator = new PairPriceTblsGenerator(Config.CONN_STRING,
				Config.DB_USERNAME, Config.DB_PASSWORD, Config.YEARS, Config.PAIRS, Config.TFRAMES_STR);
		PairTickTblsGenerator pairTickTblsGenerator = new PairTickTblsGenerator(Config.CONN_STRING, Config.DB_USERNAME,
				Config.DB_PASSWORD, Config.YEARS, Config.PAIRS, Config.TFRAMES_STR);

		WorkFlow flow2 = SequentialFlow.Builder.aNewSequentialFlow().named("Creating pair prices & ticks tables")
				.execute(pairPriceTblsGenerator).then(pairTickTblsGenerator).build();
		WorkReport report2 = engine.run(flow2, ctx);
		System.out.println(report2.getStatus());

		// --- Creating other tables ---
		OtherTblsGenerator otherTblsGenerator = new OtherTblsGenerator(Config.CONN_STRING, Config.DB_USERNAME,
				Config.DB_PASSWORD);
		WorkFlow flow3 = SequentialFlow.Builder.aNewSequentialFlow().named("Creating other tables")
				.execute(otherTblsGenerator).build();
		WorkReport report3 = engine.run(flow3, ctx);
		System.out.println(report3.getStatus());
	}

}
