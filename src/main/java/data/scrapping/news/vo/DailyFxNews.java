package data.scrapping.news.vo;

public class DailyFxNews {

	private String id;
	private String ticker;
	private String symbol;
	private String date;
	private String title;
	private String description;
	private String importance;
	private String previous;
	private String forecast;
	private String country;
	private String actual;
	private Boolean allDayEvent;
	private String currency;
	private String reference;
	private String revised;
	private DailyFxMeaning economicMeaning;
	private String lastUpdate;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getTicker() {
		return ticker;
	}

	public void setTicker(String ticker) {
		this.ticker = ticker;
	}

	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getImportance() {
		return importance;
	}

	public void setImportance(String importance) {
		this.importance = importance;
	}

	public String getPrevious() {
		return previous;
	}

	public void setPrevious(String previous) {
		this.previous = previous;
	}

	public String getForecast() {
		return forecast;
	}

	public void setForecast(String forecast) {
		this.forecast = forecast;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getActual() {
		return actual;
	}

	public void setActual(String actual) {
		this.actual = actual;
	}

	public Boolean getAllDayEvent() {
		return allDayEvent;
	}

	public void setAllDayEvent(Boolean allDayEvent) {
		this.allDayEvent = allDayEvent;
	}

	public String getCurrency() {
		return currency;
	}

	public void setCurrency(String currency) {
		this.currency = currency;
	}

	public String getReference() {
		return reference;
	}

	public void setReference(String reference) {
		this.reference = reference;
	}

	public String getRevised() {
		return revised;
	}

	public void setRevised(String revised) {
		this.revised = revised;
	}

	public DailyFxMeaning getEconomicMeaning() {
		return economicMeaning;
	}

	public void setEconomicMeaning(DailyFxMeaning economicMeaning) {
		this.economicMeaning = economicMeaning;
	}

	public String getLastUpdate() {
		return lastUpdate;
	}

	public void setLastUpdate(String lastUpdate) {
		this.lastUpdate = lastUpdate;
	}

	@Override
	public String toString() {
		return "DailyFxNews [id=" + id + ", ticker=" + ticker + ", symbol=" + symbol + ", date=" + date + ", title="
				+ title + ", description=" + description + ", importance=" + importance + ", previous=" + previous
				+ ", forecast=" + forecast + ", country=" + country + ", actual=" + actual + ", allDayEvent="
				+ allDayEvent + ", currency=" + currency + ", reference=" + reference + ", revised=" + revised
				+ ", lastUpdate=" + lastUpdate + "]";
	}

}
