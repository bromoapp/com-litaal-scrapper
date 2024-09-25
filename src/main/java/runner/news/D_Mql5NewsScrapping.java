package runner.news;

import org.jeasy.flows.engine.WorkFlowEngine;
import org.jeasy.flows.engine.WorkFlowEngineBuilder;
import org.jeasy.flows.work.WorkContext;
import org.jeasy.flows.work.WorkReport;
import org.jeasy.flows.workflow.SequentialFlow;
import org.jeasy.flows.workflow.WorkFlow;

import data.scrapping.common.vo.ScrapTime;
import data.scrapping.news.worker.Mql5NewsFiles2Db;
import data.scrapping.news.worker.Mql5NewsResultCalc;
import data.scrapping.news.worker.NewsFilesMover;
import runner.common.Config;

public class D_Mql5NewsScrapping {

	// ---- scrapper config ----
	private static String filePath = ".\\resources\\MQL5\\";

	// ---- json to db ----
	private static String connString = Config.CONN_STRING;
	private static String dbUsername = Config.DB_USERNAME;
	private static String dbPassword = Config.DB_PASSWORD;

	public static void main(String[] args) {
		for (ScrapTime time : Config.SCRAP_TIMES) {
//			Mql5NewsScrapper scrapper = new Mql5NewsScrapper(filePath, time.getYear(), time.getMonthStart(),
//					time.getMonthEnd());
			Mql5NewsFiles2Db persister = new Mql5NewsFiles2Db(connString, dbUsername, dbPassword, filePath,
					time.getYear());
			NewsFilesMover filesMover = new NewsFilesMover(filePath, time.getYear());
			Mql5NewsResultCalc calculator = new Mql5NewsResultCalc(connString, dbUsername, dbPassword, filePath,
					time.getYear());

			WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
			WorkFlow flow = SequentialFlow.Builder.aNewSequentialFlow().named("Scrapping Mql5 News").execute(persister)
					.then(filesMover).then(calculator).build();
			WorkContext ctx = new WorkContext();
			WorkReport report = engine.run(flow, ctx);
			System.out.println(report.getStatus());
		}
	}

}
