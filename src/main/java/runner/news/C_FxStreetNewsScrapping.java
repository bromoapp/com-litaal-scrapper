package runner.news;

import org.jeasy.flows.engine.WorkFlowEngine;
import org.jeasy.flows.engine.WorkFlowEngineBuilder;
import org.jeasy.flows.work.WorkContext;
import org.jeasy.flows.work.WorkReport;
import org.jeasy.flows.workflow.SequentialFlow;
import org.jeasy.flows.workflow.WorkFlow;

import data.scrapping.common.vo.ScrapTime;
import data.scrapping.news.worker.FxStreetNewsFiles2Db;
import data.scrapping.news.worker.FxStreetNewsResultCalc;
import data.scrapping.news.worker.NewsFilesMover;
import runner.common.Config;

public class C_FxStreetNewsScrapping {

	// ---- scrapper config ----
	private static String filePath = ".\\resources\\FXSTREET\\";

	// ---- json to db ----
	private static String connString = Config.CONN_STRING;
	private static String dbUsername = Config.DB_USERNAME;
	private static String dbPassword = Config.DB_PASSWORD;

	public static void main(String[] args) {
		for (ScrapTime time : Config.SCRAP_TIMES) {
//			FxStreetNewsScrapper scrapper = new FxStreetNewsScrapper(filePath, time.getYear(), time.getMonthStart(),
//					time.getMonthEnd());
			FxStreetNewsFiles2Db persister = new FxStreetNewsFiles2Db(connString, dbUsername, dbPassword, filePath,
					time.getYear());
			NewsFilesMover filesMover = new NewsFilesMover(filePath, time.getYear());
			FxStreetNewsResultCalc calculator = new FxStreetNewsResultCalc(connString, dbUsername, dbPassword, filePath,
					time.getYear());

			WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
			WorkFlow flow = SequentialFlow.Builder.aNewSequentialFlow().named("Scrapping FxStreet News")
					.execute(persister).then(filesMover).then(calculator).build();
			WorkContext ctx = new WorkContext();
			WorkReport report = engine.run(flow, ctx);
			System.out.println(report.getStatus());
		}
	}

}
