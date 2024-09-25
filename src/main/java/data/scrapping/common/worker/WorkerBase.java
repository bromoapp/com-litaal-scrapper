package data.scrapping.common.worker;

import java.text.DateFormatSymbols;

import org.jeasy.flows.work.Work;

public abstract class WorkerBase implements Work {

	private String path;
	private int year;
	private int monthStart;
	private int monthEnd;

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonthStart() {
		return monthStart;
	}

	public void setMonthStart(int monthStart) {
		this.monthStart = monthStart;
	}

	public int getMonthEnd() {
		return monthEnd;
	}

	public void setMonthEnd(int monthEnd) {
		this.monthEnd = monthEnd;
	}

	protected String getMonthName(int num) {
		num = num - 1;
		String month = "wrong";
		DateFormatSymbols dfs = new DateFormatSymbols();
		String[] months = dfs.getShortMonths();
		if (num >= 0 && num <= 11) {
			month = months[num];
		}
		return month.toUpperCase();
	}

	protected String make2Digits(int number) {
		if (String.valueOf(number).length() < 2) {
			return "0" + number;
		} else {
			return "" + number;
		}
	}

	protected Long add7Hours(Long date) {
		return date + (7 * 60 * 60);
	}

}
