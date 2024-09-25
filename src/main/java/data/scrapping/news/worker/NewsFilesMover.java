package data.scrapping.news.worker;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;

import org.jeasy.flows.work.DefaultWorkReport;
import org.jeasy.flows.work.Work;
import org.jeasy.flows.work.WorkContext;
import org.jeasy.flows.work.WorkReport;
import org.jeasy.flows.work.WorkStatus;

public class NewsFilesMover implements Work {

	private String srcFolder;
	private String desFolder;

	public NewsFilesMover(String srcFolder, int year) {
		super();
		this.srcFolder = srcFolder + "\\" + year + "\\";
		this.desFolder = srcFolder + "\\" + year + "\\done\\";
	}

	@Override
	public WorkReport execute(WorkContext ctx) {
		System.out.println("START MOVING FILES...");
		try {
			Path srcPath = Path.of(srcFolder);
			if (Files.exists(srcPath)) {
				List<File> movables = new LinkedList<>();
				Path desPath = Path.of(desFolder);
				if (!Files.exists(desPath)) {
					Files.createDirectory(desPath);
				}
				File srcDir = new File(srcFolder);
				if (srcDir.isDirectory()) {
					File[] files = srcDir.listFiles();
					for (File file : files) {
						if (file.isFile()) {
							movables.add(file);
						}
					}
				}
				if (movables.size() > 0) {
					for (File file : movables) {
						Files.move(file.toPath(), Path.of(desFolder + file.getName()));
					}
				}
				return new DefaultWorkReport(WorkStatus.COMPLETED, ctx);
			} else {
				throw new Exception(srcFolder + " NOT EXISTS!");
			}
		} catch (Exception e) {
			e.printStackTrace();
			return new DefaultWorkReport(WorkStatus.FAILED, ctx);
		}
	}

}
