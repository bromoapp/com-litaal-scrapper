package runner.news;

import org.jeasy.flows.engine.WorkFlowEngine;
import org.jeasy.flows.engine.WorkFlowEngineBuilder;
import org.jeasy.flows.work.WorkContext;
import org.jeasy.flows.work.WorkReport;
import org.jeasy.flows.workflow.SequentialFlow;
import org.jeasy.flows.workflow.WorkFlow;

import data.scrapping.common.vo.ScrapTime;
import data.scrapping.news.worker.DukasNewsFiles2Db;
import data.scrapping.news.worker.DukasNewsResultCalc;
import data.scrapping.news.worker.NewsFilesMover;
import runner.common.Config;

public class B_DukasNewsScrapping {

	// ---- scrapper config ----
	private static String filePath = ".\\resources\\DUKAS\\";

	// ---- csvs to db ----
	private static String connString = Config.CONN_STRING;
	private static String dbUsername = Config.DB_USERNAME;
	private static String dbPassword = Config.DB_PASSWORD;

	public static void main(String[] args) {
		for (ScrapTime time : Config.SCRAP_TIMES) {
//			DukasNewsScrapper scrapper = new DukasNewsScrapper(filePath, time.getYear(), time.getMonthStart(),
//					time.getMonthEnd());
			DukasNewsFiles2Db persister = new DukasNewsFiles2Db(filePath, connString, dbUsername, dbPassword,
					time.getYear());
			NewsFilesMover filesMover = new NewsFilesMover(filePath, time.getYear());
			DukasNewsResultCalc calculator = new DukasNewsResultCalc(connString, dbUsername, dbPassword, filePath,
					time.getYear());

			WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
			WorkFlow flow = SequentialFlow.Builder.aNewSequentialFlow().named("Scrapping Dukascopy News")
					.execute(persister).then(filesMover).then(calculator).build();
			WorkContext ctx = new WorkContext();
			WorkReport report = engine.run(flow, ctx);
			System.out.println(report.getStatus());
		}
	}

}
