package data.scrapping.common.helper;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import data.scrapping.prices.vo.EPair;
import data.scrapping.prices.vo.ETimeFrame;

public class Util {

	private static String timePattern = "yyyy/MM/dd HH:mm";
	private static DateTimeFormatter timeFormatter;
	private static HashMap<Integer, String> map1;
	private static HashMap<String, String> map2;
	private static Set<String> curr;

	private static DateTimeFormatter timeFormatter() {
		if (timeFormatter == null) {
			timeFormatter = DateTimeFormatter.ofPattern(timePattern);
		}
		return timeFormatter;
	}

	public static long toNearestTimeInUnixtimeMilis(ETimeFrame tf, long stamp) {
		String value = null;
		String monthTxt = null;
		String dayTxt = null;
		String hourTxt = null;
		String minTxt = null;

		LocalDateTime datetime = Instant.ofEpochMilli(stamp).atZone(ZoneId.of("GMT")).toLocalDateTime();
		int year = datetime.getYear();
		int month = datetime.getMonthValue();
		int day = datetime.getDayOfMonth();
		int hour = datetime.getHour();
		int min = datetime.getMinute();

		monthTxt = make2Digits(month);
		dayTxt = make2Digits(day);
		switch (tf) {
		case D1:
			value = year + "/" + monthTxt + "/" + dayTxt + " 00:00";
			break;
		case H1:
			hourTxt = make2Digits(hour);
			if (String.valueOf(hour).length() < 2) {
				hourTxt = "0" + hour;
			}
			value = year + "/" + monthTxt + "/" + dayTxt + " " + hourTxt + ":00";
			break;
		case M1:
			hourTxt = make2Digits(hour);
			minTxt = make2Digits(min);
			value = year + "/" + monthTxt + "/" + dayTxt + " " + hourTxt + ":" + minTxt;
			break;
		case M15:
			hourTxt = make2Digits(hour);
			if (min < 15) {
				min = 0;
			}
			if (15 <= min && min < 30) {
				min = 15;
			}
			if (30 <= min && min < 45) {
				min = 30;
			}
			if (45 <= min && min <= 59) {
				min = 45;
			}
			minTxt = make2Digits(min);
			value = year + "/" + monthTxt + "/" + dayTxt + " " + hourTxt + ":" + minTxt;
			break;
		case M30:
			hourTxt = make2Digits(hour);
			if (min < 30) {
				min = 0;
			}
			if (30 <= min) {
				min = 30;
			}
			minTxt = make2Digits(min);
			value = year + "/" + monthTxt + "/" + dayTxt + " " + hourTxt + ":" + minTxt;
			break;
		case M5:
			hourTxt = make2Digits(hour);
			if (min < 5) {
				min = 0;
			}
			if (5 <= min && min < 10) {
				min = 5;
			}
			if (10 <= min && min < 15) {
				min = 10;
			}
			if (15 <= min && min < 20) {
				min = 15;
			}
			if (20 <= min && min < 25) {
				min = 20;
			}
			if (25 <= min && min < 30) {
				min = 25;
			}
			if (30 <= min && min < 35) {
				min = 30;
			}
			if (35 <= min && min < 40) {
				min = 35;
			}
			if (40 <= min && min < 45) {
				min = 40;
			}
			if (45 <= min && min < 50) {
				min = 45;
			}
			if (50 <= min && min <= 59) {
				min = 50;
			}
			minTxt = make2Digits(min);
			value = year + "/" + monthTxt + "/" + dayTxt + " " + hourTxt + ":" + minTxt;
			break;
		default:
			break;
		}
		LocalDateTime time = LocalDateTime.parse(value, timeFormatter());
		long nearest = time.atZone(ZoneId.of("GMT")).toEpochSecond();
		return nearest;
	}

	public static long toNearestTimeInUnixtimeSeconds(ETimeFrame tf, long stamp) {
		String value = null;
		String monthTxt = null;
		String dayTxt = null;
		String hourTxt = null;
		String minTxt = null;

		LocalDateTime datetime = Instant.ofEpochSecond(stamp).atZone(ZoneId.of("GMT")).toLocalDateTime();
		int year = datetime.getYear();
		int month = datetime.getMonthValue();
		int day = datetime.getDayOfMonth();
		int hour = datetime.getHour();
		int min = datetime.getMinute();

		monthTxt = make2Digits(month);
		dayTxt = make2Digits(day);
		switch (tf) {
		case D1:
			value = year + "/" + monthTxt + "/" + dayTxt + " 00:00";
			break;
		case H1:
			hourTxt = make2Digits(hour);
			if (String.valueOf(hour).length() < 2) {
				hourTxt = "0" + hour;
			}
			value = year + "/" + monthTxt + "/" + dayTxt + " " + hourTxt + ":00";
			break;
		case M1:
			hourTxt = make2Digits(hour);
			minTxt = make2Digits(min);
			value = year + "/" + monthTxt + "/" + dayTxt + " " + hourTxt + ":" + minTxt;
			break;
		case M15:
			hourTxt = make2Digits(hour);
			if (min < 15) {
				min = 0;
			}
			if (15 <= min && min < 30) {
				min = 15;
			}
			if (30 <= min && min < 45) {
				min = 30;
			}
			if (45 <= min && min <= 59) {
				min = 45;
			}
			minTxt = make2Digits(min);
			value = year + "/" + monthTxt + "/" + dayTxt + " " + hourTxt + ":" + minTxt;
			break;
		case M30:
			hourTxt = make2Digits(hour);
			if (min < 30) {
				min = 0;
			}
			if (30 <= min) {
				min = 30;
			}
			minTxt = make2Digits(min);
			value = year + "/" + monthTxt + "/" + dayTxt + " " + hourTxt + ":" + minTxt;
			break;
		case M5:
			hourTxt = make2Digits(hour);
			if (min < 5) {
				min = 0;
			}
			if (5 <= min && min < 10) {
				min = 5;
			}
			if (10 <= min && min < 15) {
				min = 10;
			}
			if (15 <= min && min < 20) {
				min = 15;
			}
			if (20 <= min && min < 25) {
				min = 20;
			}
			if (25 <= min && min < 30) {
				min = 25;
			}
			if (30 <= min && min < 35) {
				min = 30;
			}
			if (35 <= min && min < 40) {
				min = 35;
			}
			if (40 <= min && min < 45) {
				min = 40;
			}
			if (45 <= min && min < 50) {
				min = 45;
			}
			if (50 <= min && min <= 59) {
				min = 50;
			}
			minTxt = make2Digits(min);
			value = year + "/" + monthTxt + "/" + dayTxt + " " + hourTxt + ":" + minTxt;
			break;
		default:
			break;
		}
		LocalDateTime time = LocalDateTime.parse(value, timeFormatter());
		long nearest = time.atZone(ZoneId.of("GMT")).toEpochSecond();
		return nearest;
	}

	public static String make2Digits(int val) {
		if (String.valueOf(val).length() < 2) {
			return "0" + val;
		}
		return String.valueOf(val);
	}

	public static int parseYearFromUnixtime(long time) {
		LocalDateTime ldt = LocalDateTime.ofInstant(Instant.ofEpochMilli(time * 1000L), ZoneId.of("GMT"));
		return ldt.getYear();
	}

	public static String formatArray(Object[] objs) {
		String res = "";
		for (Object obj : objs) {
			if (obj instanceof String) {
				res += "'" + obj + "',";
			} else {
				res += obj + ",";
			}
		}
		res = res.substring(0, res.length() - 1);
		return res;
	}

	public static Object[] getAvailCurrencies(EPair pair) {
		List<String> currencies = new ArrayList<>();
		switch (pair) {
		case AUDUSD:
			currencies.add("AUD");
			currencies.add("USD");
			break;
		case EURUSD:
			currencies.add("EUR");
			currencies.add("USD");
			break;
		case GBPUSD:
			currencies.add("GBP");
			currencies.add("USD");
			break;
		case NZDUSD:
			currencies.add("NZD");
			currencies.add("USD");
			break;
		case USDCAD:
			currencies.add("USD");
			currencies.add("CAD");
			break;
		case USDCHF:
			currencies.add("USD");
			currencies.add("CHF");
			break;
		case USDJPY:
			currencies.add("USD");
			currencies.add("JPY");
			break;
		default:
			break;
		}
		return currencies.toArray();
	}

	public static String to2DCountryCode(int code) {
		return number2Codes().get(code);
	}

	public static HashMap<Integer, String> number2Codes() {
		if (map1 == null) {
			map1 = new HashMap<>();
			map1.put(840, "US");
			map1.put(554, "NZ");
			map1.put(276, "DE");
			map1.put(124, "CA");
			map1.put(380, "IT");
			map1.put(392, "JP");
			map1.put(826, "UK");
			map1.put(36, "AU");
			map1.put(999, "EU");
			map1.put(724, "ES");
			map1.put(250, "FR");
			map1.put(756, "CH");
		}
		return map1;
	}

	public static String to2DCountryCode(String name) {
		return names2Codes().get(name);
	}

	public static HashMap<String, String> names2Codes() {
		if (map2 == null) {
			map2 = new HashMap<>();
			map2.put("GB", "UK");
			map2.put("United States", "US");
			map2.put("United Kingdom", "UK");
			map2.put("Italy", "IT");
			map2.put("Euro Area", "EU");
			map2.put("Austria", "AT");
			map2.put("France", "FR");
			map2.put("Finland", "FI");
			map2.put("Japan", "JP");
			map2.put("Spain", "ES");
			map2.put("Switzerland", "CH");
			map2.put("Australia", "AU");
			map2.put("Ireland", "IE");
			map2.put("Portugal", "PT");
			map2.put("Germany", "DE");
			map2.put("Netherlands", "NL");
			map2.put("Canada", "CA");
			map2.put("New Zealand", "NZ");
			map2.put("Greece", "GR");
			map2.put("Belgium", "BE");
			map2.put("European Union", "EU");
			map2.put("US", "US");
			map2.put("UK", "UK");
			map2.put("IT", "IT");
			map2.put("EU", "EU");
			map2.put("AT", "AT");
			map2.put("FR", "FR");
			map2.put("FI", "FI");
			map2.put("JP", "JP");
			map2.put("ES", "ES");
			map2.put("CH", "CH");
			map2.put("AU", "AU");
			map2.put("IE", "IE");
			map2.put("PT", "PT");
			map2.put("GE", "DE");
			map2.put("NL", "NL");
			map2.put("CA", "CA");
			map2.put("NZ", "NZ");
			map2.put("GR", "GR");
			map2.put("BE", "BE");
			map2.put("EU", "EU");
			map2.put("EMU", "EU");
		}
		return map2;
	}

	public static boolean isCurrencyUsable(String curr) {
		return currencies().contains(curr);
	}

	public static Set<String> currencies() {
		if (curr == null) {
			curr = new HashSet<>();
			curr.add("AUD");
			curr.add("EUR");
			curr.add("GBP");
			curr.add("NZD");
			curr.add("CAD");
			curr.add("CHF");
			curr.add("JPY");
			curr.add("USD");
		}
		return curr;
	}

}
