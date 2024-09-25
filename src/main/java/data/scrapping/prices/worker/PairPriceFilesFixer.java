package data.scrapping.prices.worker;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

import org.jeasy.flows.engine.WorkFlowEngine;
import org.jeasy.flows.engine.WorkFlowEngineBuilder;
import org.jeasy.flows.work.DefaultWorkReport;
import org.jeasy.flows.work.WorkContext;
import org.jeasy.flows.work.WorkReport;
import org.jeasy.flows.work.WorkStatus;
import org.jeasy.flows.workflow.SequentialFlow;
import org.jeasy.flows.workflow.WorkFlow;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import data.scrapping.common.worker.WorkerBase;

public class PairPriceFilesFixer extends WorkerBase {

	private final String header = "timestamp,open,high,low,close,volume\n";
	private String[] pairs;
	private String[] strTFrames;
	private int[] intTFrames;

	public PairPriceFilesFixer(String path, int year, String[] pairs, String[] timeframes, int[] inttframes) {
		super();
		this.setPath(path);
		this.setYear(year);
		this.pairs = pairs;
		this.strTFrames = timeframes;
		this.intTFrames = inttframes;
	}

	@Override
	public WorkReport execute(WorkContext ctx) {
		System.out.println("START FIXING...");
		try {
			if (this.getPath() == null || this.getYear() == 0) {
				throw new Exception("EMPTY REQUIRED PARAMS!");
			} else {
				for (String pair : this.pairs) {
					System.out.println("========= PAIR: " + pair + " ==================");
					for (int n = 0; n < this.strTFrames.length; n++) {
						String stf = this.strTFrames[n];
						int itf = intTFrames[n];
						List<File> todelete = new ArrayList<>();
						System.out.println("========= YEAR: " + this.getYear() + " TF: " + stf);
						String path = this.getPath() + "\\" + pair + "\\" + this.getYear() + "\\" + stf
								+ "\\download\\";
						File dir = new File(path);
						if (dir.isDirectory()) {
							for (File csv : dir.listFiles()) {
								todelete.add(csv);
								System.out.print("FIXIN: " + csv.getName());
								String newCsv = path + csv.getName().replaceAll(".csv", "") + "-fixed.csv";
								BufferedWriter bw = new BufferedWriter(new FileWriter(newCsv));
								bw.append(header);

								FileReader freader = new FileReader(csv);
								CSVParser parser = new CSVParserBuilder().withSeparator(',').build();
								CSVReader creader = new CSVReaderBuilder(freader).withSkipLines(1).withCSVParser(parser)
										.build();
								final LinkedList<String[]> list = new LinkedList<>();
								creader.forEach(new Consumer<String[]>() {
									@Override
									public void accept(String[] line) {
										list.add(line);
									}
								});
								int count = 0;
								Long n1 = null;
								Long n2 = null;
								for (int x = 0; x < list.size(); x++) {
									int prev = x - 1;
									if (x > 0) {
										n1 = Long.parseLong(list.get(prev)[0]) / 1000L;
										n2 = Long.parseLong(list.get(x)[0]) / 1000L;

										if (n2 != null && n1 != null) {
											String oldRec = list.get(prev)[0] + "," + list.get(prev)[1] + ","
													+ list.get(prev)[2] + "," + list.get(prev)[3] + ","
													+ list.get(prev)[4] + "," + list.get(prev)[5] + "\n";

											bw.append(oldRec);
											Long n3 = n2 - n1;
											Long gaps = gapByTimeframe(itf);
											Long candles = 0L;
											if (n3 > gaps) {
												Long min = n3 / 60;
												if (!(min > threshold(itf))) {
													Long n4 = n1;
													candles = (min / itf) - 1;
													for (int a = 0; a < candles; a++) {
														count += 1;
														n4 += gapByTimeframe(itf);
														String newRec = n4 * 1000L + ",0.0,0.0,0.0,0.0,0.0\n";
														bw.append(newRec);
													}
												}
											}
										}
										if (x == (list.size() - 1)) {
											String lastRec = list.get(x)[0] + "," + list.get(x)[1] + ","
													+ list.get(x)[2] + "," + list.get(x)[3] + "," + list.get(x)[4] + ","
													+ list.get(x)[5] + "\n";
											bw.append(lastRec);
										}
									}
								}
								bw.close();
								freader.close();
								System.out.println(" => " + count + " added");
							}
							if (!todelete.isEmpty()) {
								for (File f : todelete) {
									f.delete();
								}
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

	private long gapByTimeframe(int tf) {
		return tf * 60;
	}

	private long threshold(int tf) {
		if (1 <= tf && tf <= 5) {
			return 60;// 1 hour
		}
		if (15 <= tf && tf <= 30) {
			return 180;// 3 hours
		}
		return 360;
	}

	private static String rootPath = ".\\resources\\";
	private static int yr = 2023;
	public static final String[] PAIRS = { "AUDUSD", "EURUSD", "GBPUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY",
			"AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD", "CADCHF", "CADJPY", "CHFJPY", "EURAUD", "EURCAD", "EURCHF",
			"EURGBP", "EURJPY", "EURNZD", "GBPAUD", "GBPCAD", "GBPCHF", "GBPJPY", "GBPNZD", "NZDCAD", "NZDCHF",
			"NZDJPY" };
	public static final String[] TFRAMES_STR = { "D1", "H1", "M30", "M15", "M5", "M1" };
	public static final int[] TFRAMES_INT = { 1440, 60, 30, 15, 5, 1 };

	public static void main(String[] args) {
		PairPriceFilesFixer fixer = new PairPriceFilesFixer(rootPath, yr, PAIRS, TFRAMES_STR, TFRAMES_INT);
		WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
		WorkFlow flow = SequentialFlow.Builder.aNewSequentialFlow().named("Fixing prices").execute(fixer).build();
		WorkContext ctx = new WorkContext();
		WorkReport report = engine.run(flow, ctx);
		System.out.println(report.getStatus());
	}

}
