package runner.prices;

import org.jeasy.flows.engine.WorkFlowEngine;
import org.jeasy.flows.engine.WorkFlowEngineBuilder;
import org.jeasy.flows.work.WorkContext;
import org.jeasy.flows.work.WorkReport;
import org.jeasy.flows.workflow.SequentialFlow;
import org.jeasy.flows.workflow.WorkFlow;

import data.scrapping.common.vo.ScrapTime;
import data.scrapping.prices.worker.PairPriceFiles2Db;
import data.scrapping.prices.worker.PairPriceFilesFixer;
import data.scrapping.prices.worker.PairPriceFilesMover;
import runner.common.Config;

public class A_PairBidPriceScrapping {
	// ---- scrapper config ----
	private static String rootPath = ".\\resources\\";

	public static void main(String[] args) {
		for (ScrapTime time : Config.SCRAP_TIMES) {
//			PairPriceScrapper scrapper = new PairPriceScrapper(rootPath, time.getYear(), time.getMonthStart(),
//					time.getMonthEnd(), Config.PAIRS, Config.TFRAMES_STR);
			PairPriceFilesFixer fixer = new PairPriceFilesFixer(rootPath, time.getYear(), Config.PAIRS,
					Config.TFRAMES_STR, Config.TFRAMES_INT);
			PairPriceFiles2Db persister = new PairPriceFiles2Db(Config.CONN_STRING, Config.DB_USERNAME,
					Config.DB_PASSWORD, rootPath, time.getYear(), Config.PAIRS, Config.TFRAMES_STR);
			PairPriceFilesMover mover = new PairPriceFilesMover(rootPath, time.getYear(), Config.PAIRS,
					Config.TFRAMES_STR);

			WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
			WorkFlow flow = SequentialFlow.Builder.aNewSequentialFlow().named("Scrapping DailyFx News").execute(fixer)
					.then(persister).then(mover).build();
			WorkContext ctx = new WorkContext();
			WorkReport report = engine.run(flow, ctx);
			System.out.println(report.getStatus());
		}
	}

}
