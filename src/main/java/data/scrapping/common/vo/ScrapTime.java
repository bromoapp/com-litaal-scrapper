package data.scrapping.common.vo;

public class ScrapTime {

	private int year;
	private int monthStart;
	private int monthEnd;

	public ScrapTime(int year, int monthStart, int monthEnd) {
		super();
		this.year = year;
		this.monthStart = monthStart;
		this.monthEnd = monthEnd;
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

}
