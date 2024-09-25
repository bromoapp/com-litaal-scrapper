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
import java.text.SimpleDateFormat;
import java.time.YearMonth;
import java.util.Date;
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

public class FxStreetNewsScrapper extends WorkerBase {
	private final int GMT_PLUS = 0;
	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	private final String url = "https://calendar-api.fxstreet.com/en/api/v1/eventDates/%s/%s";
	private final String params = "?&volatilities=NONE&volatilities=LOW&volatilities=MEDIUM&volatilities=HIGH&countries=US&countries=UK&countries=EMU&countries=DE&countries=CN&countries=JP&countries=CA&countries=AU&countries=NZ&countries=CH&countries=FR&countries=IT&countries=ES&countries=UA&categories=8896AA26-A50C-4F8B-AA11-8B3FCCDA1DFD&categories=FA6570F6-E494-4563-A363-00D0F2ABEC37&categories=C94405B5-5F85-4397-AB11-002A481C4B92&categories=E229C890-80FC-40F3-B6F4-B658F3A02635&categories=24127F3B-EDCE-4DC4-AFDF-0B3BD8A964BE&categories=DD332FD3-6996-41BE-8C41-33F277074FA7&categories=7DFAEF86-C3FE-4E76-9421-8958CC2F9A0D&categories=1E06A304-FAC6-440C-9CED-9225A6277A55&categories=33303F5E-1E3C-4016-AB2D-AC87E98F57CA&categories=9C4A731A-D993-4D55-89F3-DC707CC1D596&categories=91DA97BD-D94A-4CE8-A02B-B96EE2944E4C&categories=E9E957EC-2927-4A77-AE0C-F5E4B5807C16";

	public FxStreetNewsScrapper(String folder, int year, int monthStart, int monthEnd) {
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
					int high = 9999;
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
						String timestamp = "";
						if (String.valueOf(GMT_PLUS).length() == 1) {
							timestamp = "T0" + GMT_PLUS + ":00:00";
						} else {
							timestamp = "T" + GMT_PLUS + "00:00";
						}
						String datetime = this.getYear() + "-" + MM + "-" + dd + timestamp;
						Date start = sdf.parse(datetime);

						Date end = (Date) start.clone();
						end.setDate(y);
						end.setHours(23 + GMT_PLUS);
						end.setMinutes(59);
						end.setSeconds(59);
						String startDate = sdf.format(start) + "Z";
						String endDate = sdf.format(end) + "Z";
						System.out.println(startDate + " to " + endDate);

						scrapNow(sb, startDate, endDate);
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

	private void scrapNow(StringBuffer sb, String start, String end) throws Exception {
		String getUrl = String.format(url, start, end) + params;
		HttpClient client = HttpClient.newBuilder().followRedirects(Redirect.ALWAYS).build();
		HttpRequest req = HttpRequest.newBuilder().uri(URI.create(getUrl)).header("accept-encoding", "gzip")
				.header("accept", "application/json").header("accept-language", "en-US,en;q=0.9")
				.header("referer", "https://www.fxstreet.com/")
				.header("sec-ch-ua", "\"Not?A_Brand\";v=\"8\", \"Chromium\";v=\"108\", \"Google Chrome\";v=\"108\"")
				.header("sec-ch-ua-mobile", "?0").header("sec-ch-ua-platform", "Windows")
				.header("sec-fetch-dest", "empty").header("sec-fetch-mode", "cors")
				.header("sec-fetch-site", "same-origin")
				.header("user-agent",
						"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")
				.header("x-requested-with", "XMLHttpRequest").GET().build();
		HttpResponse<InputStream> res = client.send(req, BodyHandlers.ofInputStream());
		if (res.statusCode() == 200) {
			InputStream raw = res.body();
			GZIPInputStream gis = new GZIPInputStream(raw);
			String rawTxt = new String(gis.readAllBytes(), StandardCharsets.UTF_8);
			sb.append(rawTxt.replaceAll("\\[", "").replaceAll("\\]", ""));
			sb.append(",");
		}
	}

	private static String filePath = ".\\resources\\FXSTREET\\";

	public static void main(String[] args) {
		for (ScrapTime time : Config.SCRAP_TIMES) {
			FxStreetNewsScrapper scrapper = new FxStreetNewsScrapper(filePath, time.getYear(), time.getMonthStart(),
					time.getMonthEnd());
			WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
			WorkFlow flow = SequentialFlow.Builder.aNewSequentialFlow().named("Scrapping FxStreet News")
					.execute(scrapper).build();
			WorkContext ctx = new WorkContext();
			WorkReport report = engine.run(flow, ctx);
			System.out.println(report.getStatus());
		}
	}
}
