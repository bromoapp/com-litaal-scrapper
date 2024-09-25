package data.scrapping.news.vo;

public class DailyFxMeaning {

	private String actual;
	private String previous;

	public String getActual() {
		return actual;
	}

	public void setActual(String actual) {
		this.actual = actual;
	}

	public String getPrevious() {
		return previous;
	}

	public void setPrevious(String previous) {
		this.previous = previous;
	}

	@Override
	public String toString() {
		return "Meaning [actual=" + actual + ", previous=" + previous + "]";
	}

}
