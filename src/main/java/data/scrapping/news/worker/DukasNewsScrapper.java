package data.scrapping.news.worker;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.time.YearMonth;
import java.util.Date;
import java.util.Random;

import org.jeasy.flows.engine.WorkFlowEngine;
import org.jeasy.flows.engine.WorkFlowEngineBuilder;
import org.jeasy.flows.work.DefaultWorkReport;
import org.jeasy.flows.work.WorkContext;
import org.jeasy.flows.work.WorkReport;
import org.jeasy.flows.work.WorkStatus;
import org.jeasy.flows.workflow.SequentialFlow;
import org.jeasy.flows.workflow.WorkFlow;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import data.scrapping.common.vo.ScrapTime;
import data.scrapping.common.worker.WorkerBase;
import data.scrapping.news.vo.DukasNews;
import runner.common.Config;

public class DukasNewsScrapper extends WorkerBase {
	private final int GMT_PLUS = 7;
	private final SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

	public DukasNewsScrapper(String folder, int year, int monthStart, int monthEnd) {
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
					int days = YearMonth.of(this.getYear(), x).lengthOfMonth();
					System.out.println("Month: " + x + " - Days: " + days);
					for (int y = 1; y <= days; y++) {
						String MM = make2Digits(x);
						String dd = make2Digits(y);
						String timestamp = "";
						if (String.valueOf(GMT_PLUS).length() == 1) {
							timestamp = " 0" + GMT_PLUS + ":00:00";
						} else {
							timestamp = " " + GMT_PLUS + "00:00";
						}
						String datetime = this.getYear() + "-" + MM + "-" + dd + timestamp;
						Date start = sdf1.parse(datetime);

						Date end = (Date) start.clone();
						end.setHours(23 + GMT_PLUS);
						end.setMinutes(59);
						end.setSeconds(59);

						System.out.println(">>> START: " + start.toGMTString() + " - END: " + end.toGMTString());
						makeRequest(bw, start, end);
					}
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

	private void makeRequest(BufferedWriter writer, Date start, Date end) throws Exception {
		// Make a HTTP call
		String url = "https://freeserv-static.dukascopy.com/2.0/index.php?path=economic_calendar_new/getNews&since="
				+ start.getTime() + "&until=" + end.getTime() + "&jsonp=_callbacks____3l9jzf82v";

		HttpClient client = HttpClient.newHttpClient();
		HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url))
				.header("authority", "freeserv-static.dukascopy.com").header("accept", "*/*")
				.header("referer",
						"https://freeserv-static.dukascopy.com/2.0/?path=economic_calendar_new/index&showHeader="
								+ "true&tableBorderColor=#D92626&showPastEvents=false&defaultTimezone=0&defaultRegion="
								+ "0&showColCCY=true&showColImpact=true&showColPrevious=true&showColForecast=true&width="
								+ "100%&height=500&adv=popup&lang=en")
				.header("user-agent",
						"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36")
				.build();
		HttpResponse<String> res = client.send(req, BodyHandlers.ofString());
		if (res.statusCode() == 200) {
			String raw = res.body();
			String json = raw.replace("_callbacks____3l9jzf82v(", "");
			json = json.replace(")", "");

			StringBuffer sb = new StringBuffer();

			JsonArray array = JsonParser.parseString(json).getAsJsonArray();
//			System.out.println("TOTAL >> " + array.size());
			Gson gson = new Gson();
			for (JsonElement el : array) {
				DukasNews ev = gson.fromJson(el, DukasNews.class);

				sb.append(sdf2.parse(ev.getDate()).getTime() / 1000L).append("~");
				sb.append(ev.getId()).append("~");
				sb.append(ev.getCountry()).append("~");
				sb.append(ev.getCurrency()).append("~");
				sb.append(ev.getTitle()).append("~");
				sb.append(ev.getPeriodicity()).append("~");

				if (ev.getShow_description() != null && ev.getShow_description().length() > 0) {
					sb.append(Integer.parseInt(ev.getShow_description()) > 0 ? true : false).append("~");
				} else {
					sb.append(false).append("~");
				}

				if (ev.getDescription() != null) {
					sb.append(ev.getDescription().replaceAll("\n", " ").replaceAll("\r", " ")).append("~");
				} else {
					sb.append("").append("~");
				}

				if (ev.getImpact() != null && ev.getImpact().length() > 0) {
					sb.append(Integer.parseInt(ev.getImpact())).append("~");
				} else {
					sb.append(-1).append("~");
				}

				sb.append(ev.getActual()).append("~");
				sb.append(ev.getActual_norm()).append("~");
				sb.append(ev.getForecast()).append("~");
				sb.append(ev.getForecast_norm()).append("~");
				sb.append(ev.getPrevious()).append("~");
				sb.append(ev.getPrevious_norm()).append("~");
				sb.append(ev.getValue_order()).append("~");
				sb.append(ev.getValue_format()).append("~");
				sb.append(ev.getTag()).append("~");

				if (ev.getHistorical_count() != null && ev.getHistorical_count().length() > 0) {
					sb.append(Integer.parseInt(ev.getHistorical_count())).append("~");
				} else {
					sb.append(0).append("~");
				}

				sb.append(ev.getEffect()).append("~");

				if (ev.getDescription_details() != null) {
					sb.append(ev.getDescription_details().getSource()).append("~");
					sb.append(ev.getDescription_details().getMeasures()).append("~");
					sb.append(ev.getDescription_details().getUsual_effect()).append("~");
					sb.append(ev.getDescription_details().getFrequency()).append("~");
					sb.append(ev.getDescription_details().getNext_release()).append("~");
					sb.append(ev.getDescription_details().getDerived_via()).append("~");
					sb.append(ev.getDescription_details().getAcro()).append("\n");
				} else {
					sb.append("").append("~");
					sb.append("").append("~");
					sb.append("").append("~");
					sb.append("").append("~");
					sb.append("").append("~");
					sb.append("").append("~");
					sb.append("").append("\n");
				}
				writer.append(sb.toString());
			}
		}
	}

	private static String filePath = ".\\resources\\DUKAS\\";

	public static void main(String[] args) {
		for (ScrapTime time : Config.SCRAP_TIMES) {
			DukasNewsScrapper scrapper = new DukasNewsScrapper(filePath, time.getYear(), time.getMonthStart(),
					time.getMonthEnd());
			WorkFlowEngine engine = WorkFlowEngineBuilder.aNewWorkFlowEngine().build();
			WorkFlow flow = SequentialFlow.Builder.aNewSequentialFlow().named("Scrapping Dukascopy News")
					.execute(scrapper).build();
			WorkContext ctx = new WorkContext();
			WorkReport report = engine.run(flow, ctx);
			System.out.println(report.getStatus());
		}
	}
}
