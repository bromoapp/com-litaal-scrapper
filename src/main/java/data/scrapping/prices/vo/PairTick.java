package data.scrapping.prices.vo;

public class PairTick {

	private Long id;
	private Long time;
	private Integer totalTicks;
	private Double highestAsk;
	private Double lowestBid;
	private Double spread;
	private Double askVol;
	private Double bidVol;
	private Integer totalVol;
	private Double pipsGap;

	public PairTick() {
		super();
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}

	public Integer getTotalTicks() {
		return totalTicks;
	}

	public void setTotalTicks(Integer totalTicks) {
		this.totalTicks = totalTicks;
	}

	public Double getHighestAsk() {
		return highestAsk;
	}

	public void setHighestAsk(Double highestAsk) {
		this.highestAsk = highestAsk;
	}

	public Double getLowestBid() {
		return lowestBid;
	}

	public void setLowestBid(Double lowestBid) {
		this.lowestBid = lowestBid;
	}

	public Double getSpread() {
		return spread;
	}

	public void setSpread(Double spread) {
		this.spread = spread;
	}

	public Double getAskVol() {
		return askVol;
	}

	public void setAskVol(Double askVol) {
		this.askVol = askVol;
	}

	public Double getBidVol() {
		return bidVol;
	}

	public void setBidVol(Double bidVol) {
		this.bidVol = bidVol;
	}

	public Integer getTotalVol() {
		return totalVol;
	}

	public void setTotalVol(Integer totalVol) {
		this.totalVol = totalVol;
	}

	public Double getPipsGap() {
		return pipsGap;
	}

	public void setPipsGap(Double pipsGap) {
		this.pipsGap = pipsGap;
	}

}
