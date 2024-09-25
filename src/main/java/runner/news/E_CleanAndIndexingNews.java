package runner.news;

import org.jeasy.flows.engine.WorkFlowEngine;
import org.jeasy.flows.engine.WorkFlowEngineBuilder;
import org.jeasy.flows.work.WorkContext;
import org.jeasy.flows.work.WorkReport;
import org.jeasy.flows.workflow.SequentialFlow;
import org.jeasy.flows.workflow.WorkFlow;

import data.anaysis.worker.NewsIndexer;
import runner.common.Config;

public class E_CleanAndIndexingNews {

	public static void main(String[] args) {
		NewsIndexer worker = new NewsIndexer(Config.CONN_STRING, Config.DB_USERNAME, Config.DB_PASSWORD,
				Config.NEWS_PROVIDERS);

		WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
		WorkFlow flow = SequentialFlow.Builder.aNewSequentialFlow().named("Indexing News").execute(worker).build();
		WorkContext ctx = new WorkContext();
		WorkReport report = engine.run(flow, ctx);
		System.out.println(report.getStatus());
	}

}
