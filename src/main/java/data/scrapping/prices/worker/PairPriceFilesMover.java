package data.scrapping.prices.worker;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;

import org.jeasy.flows.engine.WorkFlowEngine;
import org.jeasy.flows.engine.WorkFlowEngineBuilder;
import org.jeasy.flows.work.DefaultWorkReport;
import org.jeasy.flows.work.Work;
import org.jeasy.flows.work.WorkContext;
import org.jeasy.flows.work.WorkReport;
import org.jeasy.flows.work.WorkStatus;
import org.jeasy.flows.workflow.SequentialFlow;
import org.jeasy.flows.workflow.WorkFlow;

import runner.common.Config;

public class PairPriceFilesMover implements Work {

	private String rootPath;
	private int year;
	private String[] pairs;
	private String[] timeframes;

	public PairPriceFilesMover(String rootPath, int year, String[] pairs, String[] timeframes) {
		super();
		this.rootPath = rootPath;
		this.year = year;
		this.pairs = pairs;
		this.timeframes = timeframes;
	}

	@Override
	public WorkReport execute(WorkContext ctx) {
		System.out.println("START MOVING FILES...");
		try {
			if (Config.PAIRS.length > 0) {
				for (String pair : this.pairs) {
					for (String tf : this.timeframes) {
						List<File> movables = new LinkedList<>();
						// ---- create destination folder ----
						String des = this.rootPath + pair + "\\" + this.year + "\\" + tf + "\\done\\";
						if (!Files.exists(Path.of(des))) {
							Files.createDirectories(Path.of(des));
						}

						String src = this.rootPath + pair + "\\" + this.year + "\\" + tf + "\\download\\";
						File srcDir = new File(src);
						if (srcDir.isDirectory()) {
							for (File csv : srcDir.listFiles()) {
								if (csv.isFile()) {
									movables.add(csv);
								}
							}
						}
						if (movables.size() > 0) {
							for (File csv : movables) {
								Files.move(csv.toPath(), Path.of(des + csv.getName()));
							}
						}
					}
				}
			}
			return new DefaultWorkReport(WorkStatus.COMPLETED, ctx);
		} catch (Exception e) {
			e.printStackTrace();
			return new DefaultWorkReport(WorkStatus.FAILED, ctx);
		}
	}

	private static String path = ".\\resources\\";
	private static int yr = 2023;
	public static final String[] PAIRS = { 
			"AUDUSD", "EURUSD", "GBPUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY",
			"AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD", "CADCHF", "CADJPY", "CHFJPY", 
			"EURAUD", "EURCAD", "EURCHF", "EURGBP", "EURJPY", "EURNZD", "GBPAUD", 
//			"GBPCAD", "GBPCHF", "GBPJPY", "GBPNZD", "NZDCAD", "NZDCHF", "NZDJPY" 
			};
	public static final String[] TFRAMES_STR = { "D1", "H1", "M30", "M15", "M5", "M1" };

	public static void main(String[] args) {
		PairPriceFilesMover mover = new PairPriceFilesMover(path, yr, PAIRS, TFRAMES_STR);

		WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
		WorkFlow flow = SequentialFlow.Builder.aNewSequentialFlow().named("Scrapping DailyFx News").execute(mover)
				.build();
		WorkContext ctx = new WorkContext();
		WorkReport report = engine.run(flow, ctx);
		System.out.println(report.getStatus());
	}

}
