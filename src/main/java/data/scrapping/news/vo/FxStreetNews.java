package data.scrapping.news.vo;

public class FxStreetNews {

	private String id;
	private String eventId;
	private String dateUtc;
	private String periodDateUtc;
	private String periodType;
	private Double actual;
	private Double revise;
	private Double consensus;
	private Double ratioDeviation;
	private Double previous;
	private Boolean isBetterThanExpected;
	private String name;
	private String countryCode;
	private String currencyCode;
	private String unit;
	private String potency;
	private String volatility;
	private Boolean isAllDay;
	private Boolean isTentative;
	private Boolean isPreliminary;
	private Boolean isReport;
	private Boolean isSpeech;
	private Long lastUpdated;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getEventId() {
		return eventId;
	}

	public void setEventId(String eventId) {
		this.eventId = eventId;
	}

	public String getDateUtc() {
		return dateUtc;
	}

	public void setDateUtc(String dateUtc) {
		this.dateUtc = dateUtc;
	}

	public String getPeriodDateUtc() {
		return periodDateUtc;
	}

	public void setPeriodDateUtc(String periodDateUtc) {
		this.periodDateUtc = periodDateUtc;
	}

	public String getPeriodType() {
		return periodType;
	}

	public void setPeriodType(String periodType) {
		this.periodType = periodType;
	}

	public Double getActual() {
		return actual;
	}

	public void setActual(Double actual) {
		this.actual = actual;
	}

	public Double getRevise() {
		return revise;
	}

	public void setRevise(Double revise) {
		this.revise = revise;
	}

	public Double getConsensus() {
		return consensus;
	}

	public void setConsensus(Double consensus) {
		this.consensus = consensus;
	}

	public Double getRatioDeviation() {
		return ratioDeviation;
	}

	public void setRatioDeviation(Double ratioDeviation) {
		this.ratioDeviation = ratioDeviation;
	}

	public Double getPrevious() {
		return previous;
	}

	public void setPrevious(Double previous) {
		this.previous = previous;
	}

	public Boolean getIsBetterThanExpected() {
		return isBetterThanExpected;
	}

	public void setIsBetterThanExpected(Boolean isBetterThanExpected) {
		this.isBetterThanExpected = isBetterThanExpected;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getCountryCode() {
		return countryCode;
	}

	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}

	public String getCurrencyCode() {
		return currencyCode;
	}

	public void setCurrencyCode(String currencyCode) {
		this.currencyCode = currencyCode;
	}

	public String getUnit() {
		return unit;
	}

	public void setUnit(String unit) {
		this.unit = unit;
	}

	public String getPotency() {
		return potency;
	}

	public void setPotency(String potency) {
		this.potency = potency;
	}

	public String getVolatility() {
		return volatility;
	}

	public void setVolatility(String volatility) {
		this.volatility = volatility;
	}

	public Boolean getIsAllDay() {
		return isAllDay;
	}

	public void setIsAllDay(Boolean isAllDay) {
		this.isAllDay = isAllDay;
	}

	public Boolean getIsTentative() {
		return isTentative;
	}

	public void setIsTentative(Boolean isTentative) {
		this.isTentative = isTentative;
	}

	public Boolean getIsPreliminary() {
		return isPreliminary;
	}

	public void setIsPreliminary(Boolean isPreliminary) {
		this.isPreliminary = isPreliminary;
	}

	public Boolean getIsReport() {
		return isReport;
	}

	public void setIsReport(Boolean isReport) {
		this.isReport = isReport;
	}

	public Boolean getIsSpeech() {
		return isSpeech;
	}

	public void setIsSpeech(Boolean isSpeech) {
		this.isSpeech = isSpeech;
	}

	public Long getLastUpdated() {
		return lastUpdated;
	}

	public void setLastUpdated(Long lastUpdated) {
		this.lastUpdated = lastUpdated;
	}

	@Override
	public String toString() {
		return "FxStreetNews [id=" + id + ", eventId=" + eventId + ", dateUtc=" + dateUtc + "]";
	}

}
