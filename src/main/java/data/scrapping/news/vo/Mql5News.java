package data.scrapping.news.vo;

public class Mql5News {

	private long Id;
	private int EventType;
	private int TimeMode;
	private int Processed;
	private String Url;
	private String EventName;
	private String Importance;
	private String CurrencyCode;
	private String ForecastValue;
	private String PreviousValue;
	private String OldPreviousValue;
	private String ActualValue;
	private long ReleaseDate;
	private int ImpactDirection;
	private String ImpactValue;
	private String ImpactValueF;
	private String Country;
	private String CountryName;
	private String FullDate;

	public Mql5News() {
		super();
	}

	public long getId() {
		return Id;
	}

	public void setId(long id) {
		Id = id;
	}

	public int getEventType() {
		return EventType;
	}

	public void setEventType(int eventType) {
		EventType = eventType;
	}

	public int getTimeMode() {
		return TimeMode;
	}

	public void setTimeMode(int timeMode) {
		TimeMode = timeMode;
	}

	public int getProcessed() {
		return Processed;
	}

	public void setProcessed(int processed) {
		Processed = processed;
	}

	public String getUrl() {
		return Url;
	}

	public void setUrl(String url) {
		Url = url;
	}

	public String getEventName() {
		return EventName;
	}

	public void setEventName(String eventName) {
		EventName = eventName;
	}

	public String getImportance() {
		return Importance;
	}

	public void setImportance(String importance) {
		Importance = importance;
	}

	public String getCurrencyCode() {
		return CurrencyCode;
	}

	public void setCurrencyCode(String currencyCode) {
		CurrencyCode = currencyCode;
	}

	public String getForecastValue() {
		return ForecastValue;
	}

	public void setForecastValue(String forecastValue) {
		ForecastValue = forecastValue;
	}

	public String getPreviousValue() {
		return PreviousValue;
	}

	public void setPreviousValue(String previousValue) {
		PreviousValue = previousValue;
	}

	public String getOldPreviousValue() {
		return OldPreviousValue;
	}

	public void setOldPreviousValue(String oldPreviousValue) {
		OldPreviousValue = oldPreviousValue;
	}

	public String getActualValue() {
		return ActualValue;
	}

	public void setActualValue(String actualValue) {
		ActualValue = actualValue;
	}

	public long getReleaseDate() {
		return ReleaseDate;
	}

	public void setReleaseDate(long releaseDate) {
		ReleaseDate = releaseDate;
	}

	public int getImpactDirection() {
		return ImpactDirection;
	}

	public void setImpactDirection(int impactDirection) {
		ImpactDirection = impactDirection;
	}

	public String getImpactValue() {
		return ImpactValue;
	}

	public void setImpactValue(String impactValue) {
		ImpactValue = impactValue;
	}

	public String getImpactValueF() {
		return ImpactValueF;
	}

	public void setImpactValueF(String impactValueF) {
		ImpactValueF = impactValueF;
	}

	public String getCountry() {
		return Country;
	}

	public void setCountry(String country) {
		Country = country;
	}

	public String getCountryName() {
		return CountryName;
	}

	public void setCountryName(String countryName) {
		CountryName = countryName;
	}

	public String getFullDate() {
		return FullDate;
	}

	public void setFullDate(String fullDate) {
		FullDate = fullDate;
	}

	@Override
	public String toString() {
		return "Mql5News [Id=" + Id + ", EventType=" + EventType + ", TimeMode=" + TimeMode + ", Processed=" + Processed
				+ ", Url=" + Url + ", EventName=" + EventName + ", Importance=" + Importance + ", CurrencyCode="
				+ CurrencyCode + ", ForecastValue=" + ForecastValue + ", PreviousValue=" + PreviousValue
				+ ", OldPreviousValue=" + OldPreviousValue + ", ActualValue=" + ActualValue + ", ReleaseDate="
				+ ReleaseDate + ", ImpactDirection=" + ImpactDirection + ", ImpactValue=" + ImpactValue
				+ ", ImpactValueF=" + ImpactValueF + ", Country=" + Country + ", CountryName=" + CountryName
				+ ", FullDate=" + FullDate + "]";
	}

}
