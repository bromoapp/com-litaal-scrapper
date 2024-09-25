package data.dbschema.worker;

import java.io.File;
import java.nio.file.Files;
import java.sql.Statement;

import org.jeasy.flows.work.DefaultWorkReport;
import org.jeasy.flows.work.WorkContext;
import org.jeasy.flows.work.WorkReport;
import org.jeasy.flows.work.WorkStatus;

import data.scrapping.common.worker.DbRelatedWorkerBase;

public class OtherTblsGenerator extends DbRelatedWorkerBase {

	private final String root = ".\\src\\main\\resources\\";
	private final String[] filenames = { "access_token.sql", "credential.sql", "indexed_news.sql" };

	public OtherTblsGenerator(String dbConnString, String dbUsername, String dbPassword) {
		this.setDbConnString(dbConnString);
		this.setDbUsername(dbUsername);
		this.setDbPassword(dbPassword);
	}

	@Override
	public WorkReport execute(WorkContext ctx) {
		System.out.println("EXECUTING CREATE OTHER TABLES");
		try {
			for (String filename : filenames) {
				String sqlFile = root + filename;
				File sql = new File(sqlFile);
				if (sql.isFile()) {
					String content = new String(Files.readAllBytes(sql.toPath()));
					Statement stmt = this.getConn().createStatement();
					stmt.execute(content);
				}
			}
			this.getConn().close();
			return new DefaultWorkReport(WorkStatus.COMPLETED, ctx);
		} catch (Exception e) {
			e.printStackTrace();
			return new DefaultWorkReport(WorkStatus.FAILED, ctx);
		}
	}

}
