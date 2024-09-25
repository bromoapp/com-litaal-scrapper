package data.scrapping.prices.worker;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormatSymbols;
import java.util.LinkedList;
import java.util.List;

import org.jeasy.flows.work.DefaultWorkReport;
import org.jeasy.flows.work.WorkContext;
import org.jeasy.flows.work.WorkReport;
import org.jeasy.flows.work.WorkStatus;

import data.scrapping.common.worker.WorkerBase;

public class PairTickFilesMover extends WorkerBase {

	private String[] pairs;

	public PairTickFilesMover(String folder, int year, int monthStart, int monthEnd, String[] pairs) {
		super();
		this.setPath(folder);
		this.setYear(year);
		this.setMonthStart(monthStart);
		this.setMonthEnd(monthEnd);
		this.pairs = pairs;
	}

	@Override
	public WorkReport execute(WorkContext ctx) {
		System.out.println("START MOVING FILES...");
		try {
			String[] months = new DateFormatSymbols().getShortMonths();
			for (int month = this.getMonthStart(); month <= this.getMonthEnd(); month++) {
				for (String pair : this.pairs) {
					List<File> movables = new LinkedList<>();
					// ---- create destination folder ----
					String des = this.getPath() + "\\" + pair + "\\" + this.getYear() + "\\" + "\\TICK\\"
							+ months[month - 1] + "\\done\\";
					if (!Files.exists(Path.of(des))) {
						Files.createDirectories(Path.of(des));
					}

					String src = this.getPath() + "\\" + pair + "\\" + this.getYear() + "\\" + "\\TICK\\"
							+ months[month - 1] + "\\download\\";
					File dir = new File(src);
					if (dir.isDirectory()) {
						for (File csv : dir.listFiles()) {
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
			return new DefaultWorkReport(WorkStatus.COMPLETED, ctx);
		} catch (Exception e) {
			e.printStackTrace();
			return new DefaultWorkReport(WorkStatus.FAILED, ctx);
		}
	}

}
