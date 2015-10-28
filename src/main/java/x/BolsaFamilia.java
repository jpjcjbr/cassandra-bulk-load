package x;

public class BolsaFamilia {

	private String month;
	
	private String uf;
	
	private String city;
	
	private String receiver;
	
	private Double value;
	
	private String description;

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public BolsaFamilia(String month, String uf, String city, String receiver, Double value, String description) {
		this.month = month;
		this.uf = uf;
		this.city = city;
		this.receiver = receiver;
		this.value = value;
		this.description = description;
	}

	public String getUf() {
		return uf;
	}

	public void setUf(String uf) {
		this.uf = uf;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getReceiver() {
		return receiver;
	}

	public void setReceiver(String receiver) {
		this.receiver = receiver;
	}

	public Double getValue() {
		return value;
	}

	public void setValue(Double value) {
		this.value = value;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
}
