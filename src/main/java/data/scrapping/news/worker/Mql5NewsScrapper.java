package data.scrapping.news.worker;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.time.YearMonth;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
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

public class Mql5NewsScrapper extends WorkerBase {
	private final String url = "https://www.mql5.com/en/economic-calendar/content";
	private final int GMT_PLUS = 0;
	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

	public Mql5NewsScrapper(String folder, int year, int monthStart, int monthEnd) {
		super();
		this.setPath(folder);
		this.setYear(year);
		this.setMonthStart(monthStart);
		this.setMonthEnd(monthEnd);
	}

	@SuppressWarnings({ "deprecation", "static-access" })
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

					int days = YearMonth.of(this.getYear(), x).lengthOfMonth();
					String MM = make2Digits(x);
					String dd = make2Digits(1);
					String timestamp = "";
					if (String.valueOf(GMT_PLUS).length() == 1) {
						timestamp = "T0" + GMT_PLUS + ":00:00";
					} else {
						timestamp = "T" + GMT_PLUS + "00:00";
					}
					String datetime = this.getYear() + "-" + MM + "-" + dd + timestamp;
					Date start = sdf.parse(datetime);

					Date end = (Date) start.clone();
					end.setDate(days);
					end.setHours(23 + GMT_PLUS);
					end.setMinutes(59);
					end.setSeconds(59);

					System.out.println(sdf.format(start) + " to " + sdf.format(end));

					scrapNow(sb, start, end);
					bw.append(sb.toString());
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

	private void scrapNow(StringBuffer sb, Date start, Date end) throws Exception {
		HttpClient client = HttpClient.newBuilder().followRedirects(Redirect.ALWAYS).build();
		HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url)).header("path", "/en/economic-calendar/content")
				.header("accept-encoding", "gzip").header("accept", "*/*").header("accept-language", "en-US,en;q=0.9")
				.header("content-type", "application/x-www-form-urlencoded")
				.header("cookie",
						"lang=en; _fz_uniq=5004537176767899249; _fz_fvdt=1663120536; utm_source=www.mql5.com; utm_campaign=509.en.password.recovery; auth=1MyP9RhGyvMofHy7Z_wPNYD-MG0kEXssgRjzkqXsPdNxFaZ2kt3_QOOxaKCL1BC4Sfiq_HPkJonocMvPYPUpm71ZGMlUHSXfEB4lOYjebatkWTwf5cGoMMlqDuM99H5K70apbwrvOzLjBnqeksbtRQ; sid=CfDJ8LeaMSax/8JBkP1YoE3VXN+lJumz80dUNnITj3eQdn/MwaIiqB/JLkC/XAWtvzIIPb4G4kmzXEUuZJYp6rZm9E/WCF4e/gILnesneJ2UB+K/KRNY0BQxtySS6xM7NdsA/bek0RntRMG5nAX5Xi/KadSLlWBB9SKEUJV9yr/HeBlh; _fz_ssn=1671366135921901051")
				.header("origin", "https://www.mql5.com").header("referer", "https://www.mql5.com/en/economic-calendar")
				.header("sec-ch-ua", "\"Not?A_Brand\";v=\"8\", \"Chromium\";v=\"108\", \"Google Chrome\";v=\"108\"")
				.header("sec-ch-ua-mobile", "?0").header("sec-ch-ua-platform", "Windows")
				.header("sec-fetch-dest", "empty").header("sec-fetch-mode", "cors")
				.header("sec-fetch-site", "same-origin")
				.header("user-agent",
						"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")
				.header("x-requested-with", "XMLHttpRequest")
				.POST(HttpRequest.BodyPublishers.ofString(getFormData(sdf.format(start), sdf.format(end)))).build();

		HttpResponse<InputStream> res = client.send(req, BodyHandlers.ofInputStream());
		if (res.statusCode() == 200) {
			InputStream raw = res.body();
			GZIPInputStream gis = new GZIPInputStream(raw);
			String text = new String(gis.readAllBytes(), StandardCharsets.UTF_8);
			sb.append(text);
		}
	}

	private String getFormData(String start, String end) {
		Map<String, String> form = new LinkedHashMap<>();
		form.put("date_mode", "1");
		form.put("from", start);
		form.put("to", end);
		form.put("importance", "14");
		form.put("currencies", "262143");
		return getFormDataAsString(form);
	}

	private String getFormDataAsString(Map<String, String> map) {
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, String> entry : map.entrySet()) {
			if (sb.length() > 0) {
				sb.append("&");
			}
			sb.append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8));
			sb.append("=");
			sb.append(URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
		}
		return sb.toString();
	}

	private static String filePath = ".\\resources\\MQL5\\";

	public static void main(String[] args) {
		for (ScrapTime time : Config.SCRAP_TIMES) {
			Mql5NewsScrapper scrapper = new Mql5NewsScrapper(filePath, time.getYear(), time.getMonthStart(),
					time.getMonthEnd());
			WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
			WorkFlow flow = SequentialFlow.Builder.aNewSequentialFlow().named("Scrapping Mql5 News").execute(scrapper)
					.build();
			WorkContext ctx = new WorkContext();
			WorkReport report = engine.run(flow, ctx);
			System.out.println(report.getStatus());
		}
	}

}
