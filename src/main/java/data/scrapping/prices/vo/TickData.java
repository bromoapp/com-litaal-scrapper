package data.scrapping.prices.vo;

public class TickData {

	private long time;
	private long total;
	private Double highestAsk;
	private Double lowestBid;
	private Double spread;
	private Double bidVol;
	private Double askVol;
	private Double totalVol;

	public TickData() {
		super();
		this.highestAsk = 0.0;
		this.lowestBid = 100000000.0;
		this.bidVol = 0.0;
		this.askVol = 0.0;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public long getTotal() {
		return total;
	}

	public void setTotal(long total) {
		this.total = total;
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

	public Double getBidVol() {
		return bidVol;
	}

	public void setBidVol(Double bidVol) {
		this.bidVol = bidVol;
	}

	public Double getAskVol() {
		return askVol;
	}

	public void setAskVol(Double askVol) {
		this.askVol = askVol;
	}

	public Double getTotalVol() {
		return totalVol;
	}

	public void setTotalVol(Double totalVol) {
		this.totalVol = totalVol;
	}

	@Override
	public String toString() {
		return "TickData [time=" + time + ", total=" + total + ", highestAsk=" + highestAsk + ", lowestBid=" + lowestBid
				+ ", spread=" + spread + ", bidVol=" + bidVol + ", askVol=" + askVol + ", totalVol=" + totalVol + "]";
	}

}
