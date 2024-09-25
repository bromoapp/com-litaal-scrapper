package data.scrapping.news.worker;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.YearMonth;
import java.util.Random;
import java.util.zip.GZIPInputStream;

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
import runner.common.Config;

public class DailyFxNewsScrapper extends WorkerBase {
	private final String url = "https://www.dailyfx.com/economic-calendar/events/%s";
	private final String cookie = "dfx-cookies-level=3; _gcl_au=1.1.2015291019.1672675643; ak_bmsc=EE106E07E49B3D6BAAAEB180E6AF12EB~000000000000000000000000000000~YAAQrDLdF5U4K0OFAQAAes48cxJCjhw1wrZJ2p+HcKl2nb7aq8C8m5ggpwdQJXw3qye86vNkPYgJB7Iw481gqRl0sj5RkuK8jvv9JiHXXFqrIzMqJ6pWIYwE5IG4YTvp+uir3NAAOdcRC87ef132Dnware/NPRW59uNhpnJbmy7cKzYUAhMfxTJZR6bJvvB7H+9CKw53wk4WDriSonoFyb+zeB0WOApbEA6C8daXMjjIImjV/wgDyZnu9AoFo+zlRJVehKQh/GoEIAQDc3Nu6/6lbKK7a7JO8eZDJAEJpsbgVGszj5hRyE2S9FVxD73EJwnf/BZ55zOJem1mUOfR4NTxE5xteapK18W7sWufFuzr9Dajpek7WDYulJsZVAPUb2VsPpUCGZupfDX+Ff6WVWHiVVF1yVO2gvd8dO0KzoFhQ2tolvpsBVUVjByBu45PgvdNTkhmwvpK0114+Gil2hPMblfBP5grH1yXh4vcaQ6+7btiN5eDXIlj; _gid=GA1.2.1002762757.1672675643; _fbp=fb.1.1672675643380.609571130; __gads=ID=915ef402edd7d0bd:T=1672675663:S=ALNI_MaUqX3HGzfp_mQj3QfRCnHxpone2g; __gpi=UID=00000b9c97a6efcd:T=1672675663:RT=1672675663:S=ALNI_MYjQe_U06AbyQr86r6lGOblTvs0Tg; _cs_mk_ga=0.8724065093026261_1672679216722; _gat_UA-84850635-10=1; _dc_gtm_UA-35659732-1=1; bm_sv=0DFF89C604348EC4803FF85E36BF8BB5~YAAQrDLdF9PPK0OFAQAAkAR4cxJMb4vfrCD3khKyi7p0aMtTuvhsGnCv8DiJ7PcOPp6fodHXs8bWyoHHl6u3EiqWLhrfrLB524Vxxy/8KAL9o961uRAt0uym77in2E8BsEGLdwOdISQAwZcF1TxAhnMfu71mkK/ftEOy3gUqftuMgnVgueda8w1OcTJPgmiQByeR7kW7i91D0Fn0Yh59Zd6qKLnxJ+XaxHc2gu8iAtWAU3Z/q62zNSzMq9UJYQYD8xW0jndtYXPpMA==~1; _ga=GA1.2.1037476038.1672675643; _ga_D138CJ93S1=GS1.1.1672679073.2.1.1672679525.0.0.0";

	public DailyFxNewsScrapper(String folder, int year, int monthStart, int monthEnd) {
		super();
		this.setPath(folder);
		this.setYear(year);
		this.setMonthStart(monthStart);
		this.setMonthEnd(monthEnd);
	}

	@SuppressWarnings({ "static-access" })
	@Override
	public WorkReport execute(WorkContext ctx) {
		System.out.println("START SCRAPPING...");
		try {
			if (this.getPath() == null || this.getMonthEnd() == 0 || this.getMonthStart() == 0 || this.getYear() == 0) {
				throw new Exception("EMPTY REQUIRED PARAMS!");
			} else {
				Path providerPath = Path.of(this.getPath());
				if (!Files.exists(providerPath)) {
					Files.createDirectories(providerPath);
				}
				Path yearFolderPath = Path.of(this.getPath() + "\\" + this.getYear());
				if (!Files.exists(yearFolderPath)) {
					Files.createDirectories(yearFolderPath);
				}
				for (int x = this.getMonthStart(); x <= this.getMonthEnd(); x++) {
					Random ran = new Random();
					int low = 1000;
					int high = 4999;
					int wait = ran.nextInt(high - low) + low;

					String path = this.getPath() + this.getYear() + "\\" + make2Digits(x) + "_events_" + getMonthName(x)
							+ ".json";
					BufferedWriter bw = new BufferedWriter(new FileWriter(path));
					StringBuffer sb = new StringBuffer();
					sb.append("[");
					int days = YearMonth.of(this.getYear(), x).lengthOfMonth();
					System.out.println("Month: " + x + " - Days: " + days);
					for (int y = 1; y <= days; y++) {
						String MM = make2Digits(x);
						String dd = make2Digits(y);
						String date = this.getYear() + "-" + MM + "-" + dd;
						System.out.println("SCRAPPING " + date);
						scrapNow(sb, date);
					}
					sb.append("]");
					bw.append(sb.toString().replaceAll(",\\]", "\\]").replaceAll(",,", ","));
					bw.flush();
					bw.close();
					Thread.currentThread().sleep(wait);
				}
			}
			return new DefaultWorkReport(WorkStatus.COMPLETED, ctx);
		} catch (Exception e) {
			e.printStackTrace();
			return new DefaultWorkReport(WorkStatus.FAILED, ctx);
		}
	}

	private void scrapNow(StringBuffer sb, String date) throws Exception {
		String getUrl = String.format(url, date);
		HttpClient client = HttpClient.newBuilder().followRedirects(Redirect.ALWAYS).build();
		HttpRequest req = HttpRequest.newBuilder().uri(URI.create(getUrl)).header("accept", "*/*")
				.header("accept-encoding", "gzip").header("cookie", cookie)
				.header("referer", "https://www.dailyfx.com/economic-calendar")
				.header("x-requested-with", "XMLHttpRequest")
				.header("sec-ch-ua", "\"Not?A_Brand\";v=\"8\", \"Chromium\";v=\"108\", \"Google Chrome\";v=\"108\"")
				.header("sec-ch-ua-mobile", "?0").header("sec-ch-ua-platform", "Windows")
				.header("sec-fetch-dest", "empty").header("sec-fetch-mode", "cors")
				.header("sec-fetch-site", "same-origin")
				.header("user-agent",
						"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")
				.GET().build();
		HttpResponse<InputStream> res = client.send(req, BodyHandlers.ofInputStream());
		if (res.statusCode() == 200) {
			InputStream raw = res.body();
			GZIPInputStream gis = new GZIPInputStream(raw);
			String rawTxt = new String(gis.readAllBytes(), StandardCharsets.UTF_8);
			sb.append(rawTxt.replaceAll("\\[", "").replaceAll("\\]", ""));
			sb.append(",");
		}
	}

	private static String filePath = ".\\resources\\DAILYFX\\";

	public static void main(String[] args) {
		for (ScrapTime time : Config.SCRAP_TIMES) {
			DailyFxNewsScrapper scrapper = new DailyFxNewsScrapper(filePath, time.getYear(), time.getMonthStart(),
					time.getMonthEnd());
			WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
			WorkFlow flow = SequentialFlow.Builder.aNewSequentialFlow().named("Scrapping DailyFx News")
					.execute(scrapper).build();
			WorkContext ctx = new WorkContext();
			WorkReport report = engine.run(flow, ctx);
			System.out.println(report.getStatus());
		}
	}
}
