package data.scrapping.prices.worker;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jeasy.flows.engine.WorkFlowEngine;
import org.jeasy.flows.engine.WorkFlowEngineBuilder;
import org.jeasy.flows.work.DefaultWorkReport;
import org.jeasy.flows.work.WorkContext;
import org.jeasy.flows.work.WorkReport;
import org.jeasy.flows.work.WorkStatus;
import org.jeasy.flows.workflow.SequentialFlow;
import org.jeasy.flows.workflow.WorkFlow;

import data.scrapping.common.worker.WorkerBase;
import runner.common.Config;

public class PairPriceScrapper extends WorkerBase {

	private final String utcOffset = "0";
	private Map<Integer, String> commands;
	private String[] pairs;
	private String[] tframes;

	public PairPriceScrapper(String folder, int year, int monthStart, int monthEnd, String[] pairs, String[] tframes) {
		super();
		this.setPath(folder);
		this.setYear(year);
		this.setMonthStart(monthStart);
		this.setMonthEnd(monthEnd);
		this.pairs = pairs;
		this.tframes = tframes;
	}

	@Override
	public WorkReport execute(WorkContext ctx) {
		System.out.println("START SCRAPPING...");
		try {
			if (this.getPath() == null || this.getMonthEnd() == 0 || this.getMonthStart() == 0 || this.getYear() == 0) {
				throw new Exception("EMPTY REQUIRED PARAMS!");
			} else {
				if (System.getProperty("os.name").toLowerCase().startsWith("windows")) {
					Process process = null;
					for (int month = this.getMonthStart(); month <= this.getMonthEnd(); month++) {
						String cmd = getCommands().get(month);
						System.out.println(">>>>>>> YEAR: " + this.getYear() + " MONTH: " + month);
						if (Config.PAIRS.length > 0) {
							for (String tf : this.tframes) {
								for (String pair : this.pairs) {
									Path pairFolder = Path.of(this.getPath() + "\\" + pair);
									if (!Files.exists(pairFolder)) {
										Files.createDirectories(pairFolder);
									}
									Path yrFolder = Path.of(this.getPath() + "\\" + pair + "\\" + this.getYear());
									if (!Files.exists(yrFolder)) {
										Files.createDirectories(yrFolder);
									}
									Path tfFolder = Path
											.of(this.getPath() + "\\" + pair + "\\" + this.getYear() + "\\" + tf);
									if (!Files.exists(tfFolder)) {
										Files.createDirectories(tfFolder);
									}

									String execmd = cmd.replaceAll("\\$1", pair.toLowerCase())
											.replaceAll("\\$2", this.getYear() + "")
											.replaceAll("\\$3", (this.getYear() + 1) + "").replaceAll("\\$4", utcOffset)
											.replaceAll("\\$5", tf.toLowerCase());
									System.out.println(execmd);

									List<String> commands = new ArrayList<>();
									commands.add("cmd.exe");
									commands.add("/c");
									commands.add(execmd);

									ProcessBuilder builder = new ProcessBuilder(commands);
									builder.directory(new File(tfFolder.toString()));
									process = builder.start();
									BufferedReader stdInput = new BufferedReader(
											new InputStreamReader(process.getInputStream()));
									String s = null;
									while ((s = stdInput.readLine()) != null) {
										System.out.println(s);
									}
								}
								Thread.sleep(1000);
							}
						}
					}
				} else {
					throw new Exception("OS IS NOT WINDOW!");
				}
			}
			return new DefaultWorkReport(WorkStatus.COMPLETED, ctx);
		} catch (Exception e) {
			e.printStackTrace();
			return new DefaultWorkReport(WorkStatus.FAILED, ctx);
		}
	}

	private Map<Integer, String> getCommands() {
		if (commands == null) {
			commands = new HashMap<>();
			commands.put(1, "npx dukascopy-node -i $1 -from $2-01-01 -to $2-02-01 -v true -f csv -utc $4 -t $5");
			commands.put(2, "npx dukascopy-node -i $1 -from $2-02-01 -to $2-03-01 -v true -f csv -utc $4 -t $5");
			commands.put(3, "npx dukascopy-node -i $1 -from $2-03-01 -to $2-04-01 -v true -f csv -utc $4 -t $5");
			commands.put(4, "npx dukascopy-node -i $1 -from $2-04-01 -to $2-05-01 -v true -f csv -utc $4 -t $5");
			commands.put(5, "npx dukascopy-node -i $1 -from $2-05-01 -to $2-06-01 -v true -f csv -utc $4 -t $5");
			commands.put(6, "npx dukascopy-node -i $1 -from $2-06-01 -to $2-07-01 -v true -f csv -utc $4 -t $5");
			commands.put(7, "npx dukascopy-node -i $1 -from $2-07-01 -to $2-08-01 -v true -f csv -utc $4 -t $5");
			commands.put(8, "npx dukascopy-node -i $1 -from $2-08-01 -to $2-09-01 -v true -f csv -utc $4 -t $5");
			commands.put(9, "npx dukascopy-node -i $1 -from $2-09-01 -to $2-10-01 -v true -f csv -utc $4 -t $5");
			commands.put(10, "npx dukascopy-node -i $1 -from $2-10-01 -to $2-11-01 -v true -f csv -utc $4 -t $5");
			commands.put(11, "npx dukascopy-node -i $1 -from $2-11-01 -to $2-12-01 -v true -f csv -utc $4 -t $5");
			commands.put(12, "npx dukascopy-node -i $1 -from $2-12-01 -to $3-01-01 -v true -f csv -utc $4 -t $5");
		}
		return commands;
	}

	private static String rootPath = ".\\resources\\";
	private static int yr = 2023;
	private static int monthStart = 3;
	private static int monthEnd = 3;

	public static final String[] PAIRS = { "AUDUSD", "EURUSD", "GBPUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY",
			"AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD", "CADCHF", "CADJPY", "CHFJPY", "EURAUD", "EURCAD", "EURCHF",
			"EURGBP", "EURJPY", "EURNZD", "GBPAUD", "GBPCAD", "GBPCHF", "GBPJPY", "GBPNZD", "NZDCAD", "NZDCHF",
			"NZDJPY" };

	public static final String[] TFRAMES_STR = { "D1", "H1", "M30", "M15", "M5", "M1" };

	public static void main(String[] args) {
		PairPriceScrapper scrapper = new PairPriceScrapper(rootPath, yr, monthStart, monthEnd, PAIRS, TFRAMES_STR);
		WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
		WorkFlow flow = SequentialFlow.Builder.aNewSequentialFlow().named("Scrapping DailyFx News").execute(scrapper)
				.build();
		WorkContext ctx = new WorkContext();
		WorkReport report = engine.run(flow, ctx);
		System.out.println(report.getStatus());
	}

}
