package runner.common;

public class Paging {
	private int total;
	private int offset;

	public Paging() {
		super();
	}

	public Paging(int total, int offset) {
		super();
		this.total = total;
		this.offset = offset;
	}

	public int getTotal() {
		return total;
	}

	public void setTotal(int total) {
		this.total = total;
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	@Override
	public String toString() {
		return "Paging [total=" + total + ", offset=" + offset + "]";
	}

}
