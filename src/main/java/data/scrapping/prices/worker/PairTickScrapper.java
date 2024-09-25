package data.scrapping.prices.worker;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormatSymbols;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.jeasy.flows.engine.WorkFlowEngine;
import org.jeasy.flows.engine.WorkFlowEngineBuilder;
import org.jeasy.flows.work.DefaultWorkReport;
import org.jeasy.flows.work.WorkContext;
import org.jeasy.flows.work.WorkReport;
import org.jeasy.flows.work.WorkStatus;
import org.jeasy.flows.workflow.SequentialFlow;
import org.jeasy.flows.workflow.WorkFlow;

import data.scrapping.common.vo.ScrapTime;
import data.scrapping.common.worker.WorkerBase;

public class PairTickScrapper extends WorkerBase {

	private final String cmd = "npx dukascopy-node -i $0 -from $1-$2-$3 -to $4-$5-$6 -v true -f csv -utc $7 -t tick";
	private final String utcOffset = "0";
	private String[] pairs;

	public PairTickScrapper(String folder, int year, int monthStart, int monthEnd, String[] pairs) {
		super();
		this.setPath(folder);
		this.setYear(year);
		this.setMonthStart(monthStart);
		this.setMonthEnd(monthEnd);
		this.pairs = pairs;
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
					String[] months = new DateFormatSymbols().getShortMonths();
					for (int month = this.getMonthStart(); month <= this.getMonthEnd(); month++) {
						System.out.println(">>>>>>> YEAR: " + this.getYear() + " MONTH: " + month);
						for (String pair : this.pairs) {
							// ---- creating destination folder ----
							Path pairFolder = Path.of(this.getPath() + "\\" + pair);
							if (!Files.exists(pairFolder)) {
								Files.createDirectories(pairFolder);
							}
							Path yrFolder = Path.of(pairFolder.toString() + "\\" + this.getYear());
							if (!Files.exists(yrFolder)) {
								Files.createDirectories(yrFolder);
							}
							Path tkFolder = Path.of(yrFolder.toString() + "\\TICK\\");
							if (!Files.exists(tkFolder)) {
								Files.createDirectories(tkFolder);
							}
							Path mnFolder = Path.of(tkFolder.toString() + "\\" + months[month - 1]);
							if (!Files.exists(mnFolder)) {
								Files.createDirectories(mnFolder);
							}
							// ---- reformatting nodejs commands for scrapping ----
							YearMonth ymObj = YearMonth.of(this.getYear(), month);
							int daysInMonth = ymObj.lengthOfMonth();
							for (int date = 1; date <= daysInMonth; date++) {
								String dayDate = this.getYear() + "-" + make2Digits(month) + "-" + make2Digits(date);
								System.out.println(dayDate);
								String execCmd = "";
								if (!dayDate.equalsIgnoreCase(this.getYear() + "-12-31")) {
									if (date < daysInMonth) {
										execCmd = cmd.replaceAll("\\$0", pair.toLowerCase())
												.replaceAll("\\$1", "" + this.getYear())
												.replaceAll("\\$2", make2Digits(month))
												.replaceAll("\\$3", make2Digits(date))
												.replaceAll("\\$4", "" + this.getYear())
												.replaceAll("\\$5", make2Digits(month))
												.replaceAll("\\$6", make2Digits(date + 1))
												.replaceAll("\\$7", utcOffset);
									} else {
										execCmd = cmd.replaceAll("\\$0", pair.toLowerCase())
												.replaceAll("\\$1", "" + this.getYear())
												.replaceAll("\\$2", make2Digits(month))
												.replaceAll("\\$3", make2Digits(date))
												.replaceAll("\\$4", "" + this.getYear())
												.replaceAll("\\$5", make2Digits(month + 1))
												.replaceAll("\\$6", make2Digits(1)).replaceAll("\\$7", utcOffset);
									}
								} else {
									execCmd = cmd.replaceAll("\\$0", pair.toLowerCase())
											.replaceAll("\\$1", "" + this.getYear())
											.replaceAll("\\$2", make2Digits(month))
											.replaceAll("\\$3", make2Digits(date))
											.replaceAll("\\$4", "" + (this.getYear() + 1))
											.replaceAll("\\$5", make2Digits(1)).replaceAll("\\$6", make2Digits(1))
											.replaceAll("\\$7", utcOffset);
								}
								List<String> commands = new ArrayList<>();
								commands.add("cmd.exe");
								commands.add("/c");
								commands.add(execCmd);

								// ---- executing nodejs commands for scrapping ----
								ProcessBuilder builder = new ProcessBuilder(commands);
								builder.directory(new File(mnFolder.toString()));
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

	private static List<ScrapTime> times;

	private static List<ScrapTime> getScrapTimes() {
		if (times == null) {
			times = new LinkedList<>();
			times.add(new ScrapTime(2023, 3, 3));
		}
		return times;
	}

	private static String rootPath = ".\\resources\\";
	public static final String[] PAIRS = { 
//			"AUDUSD", "EURUSD", "GBPUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY",
//			"AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD", "CADCHF", "CADJPY", "CHFJPY", 
//			"EURAUD", "EURCAD", "EURCHF", "EURGBP", "EURJPY", "EURNZD", "GBPAUD", 
			"GBPCAD", "GBPCHF", "GBPJPY", "GBPNZD", "NZDCAD", "NZDCHF", "NZDJPY" 
			};

	public static void main(String[] args) {
		for (ScrapTime time : getScrapTimes()) {
			PairTickScrapper scrapper = new PairTickScrapper(rootPath, time.getYear(), time.getMonthStart(),
					time.getMonthEnd(), PAIRS);
			WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
			WorkFlow flow = SequentialFlow.Builder.aNewSequentialFlow().named("Scrapping Ticks").execute(scrapper)
					.build();
			WorkContext ctx = new WorkContext();
			WorkReport report = engine.run(flow, ctx);
			System.out.println(report.getStatus());
		}
	}

}
