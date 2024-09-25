package runner.prices;

import org.jeasy.flows.engine.WorkFlowEngine;
import org.jeasy.flows.engine.WorkFlowEngineBuilder;
import org.jeasy.flows.work.WorkContext;
import org.jeasy.flows.work.WorkReport;
import org.jeasy.flows.workflow.SequentialFlow;
import org.jeasy.flows.workflow.WorkFlow;

import data.scrapping.common.vo.ScrapTime;
import data.scrapping.prices.worker.PairTickFiles2Db;
import data.scrapping.prices.worker.PairTickFilesMover;
import runner.common.Config;

public class B_PairTickPriceScrapping {
	// ---- scrapper config ----
	private static String rootPath = ".\\resources\\";

	public static void main(String[] args) {
		for (ScrapTime time : Config.SCRAP_TIMES) {
//			PairTickScrapper scrapper = new PairTickScrapper(rootPath, time.getYear(), time.getMonthStart(),
//					time.getMonthEnd(), Config.PAIRS);
			PairTickFiles2Db persister = new PairTickFiles2Db(Config.CONN_STRING, Config.DB_USERNAME,
					Config.DB_PASSWORD, rootPath, time.getYear(), time.getMonthStart(), time.getMonthEnd(),
					Config.TFRAMES_STR, Config.PAIRS);
			PairTickFilesMover mover = new PairTickFilesMover(rootPath, time.getYear(), time.getMonthStart(),
					time.getMonthEnd(), Config.PAIRS);

			WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
			WorkFlow flow = SequentialFlow.Builder.aNewSequentialFlow().named("Scrapping DailyFx News")
					.execute(persister).then(mover).build();
			WorkContext ctx = new WorkContext();
			WorkReport report = engine.run(flow, ctx);
			System.out.println(report.getStatus());
		}
	}

}
