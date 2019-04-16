package mautic;
import java.io.InputStream;
import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

public class MauticUser {
	private String mauticUserName;
	private String mauticPassword;
	private String mauticApi;
	private String mauticSegmentApi;
	private JsonObject apiRequestObj;
	
	public MauticUser(String mauticUserName, String mauticPassword, String mauticApi) {
		this.mauticUserName = mauticUserName;
		this.mauticPassword = mauticPassword;
		this.mauticApi = mauticApi;	
	}
	
	public MauticUser(String mauticUserName, String mauticPassword, String mauticApi, String segmentApi) {
		this.mauticUserName = mauticUserName;
		this.mauticPassword = mauticPassword;
		this.mauticApi = mauticApi;	
		this.mauticSegmentApi = segmentApi;
	}
	
	
	public void setRecord(String line, String mauticFields) {
		String[] elements = line.split("\\|");
		String[] fieldNames = mauticFields.split("\\|");
		String email;
		StringBuilder jsonBuilder = new StringBuilder("{");
		for(int i=0;i<fieldNames.length;i++) {
			String fName = fieldNames[i];
			StringBuilder fValue = new StringBuilder("");
			if (fieldNames[i].toLowerCase().contains("email")) {
				String[] emails = new String[2];
				if (elements[i].contains(",")) {
					emails = elements[i].split(",");
				} else if (elements[i].contains(";")) {
					emails = elements[i].split(";");
				} else {
					if (!elements[i].toLowerCase().contains("sms") && !elements[i].toLowerCase().contains("oplin")) {
						fValue.append(elements[i]);
					}
				}
				
				for (String em : emails) {
					if (em!=null && !em.toLowerCase().contains("sms") && !em.toLowerCase().contains("oplin")) {
						fValue.append(em);
						break;
					}
				}
			} else {
				fValue.append(elements[i]);
			}
			
			jsonBuilder.append("\"");jsonBuilder.append(fName);jsonBuilder.append("\":");
			jsonBuilder.append("\"");jsonBuilder.append(fValue.toString());jsonBuilder.append("\"");
			if(i!=fieldNames.length-1)
				jsonBuilder.append(",");
		}
		jsonBuilder.append("}");
		JsonReader reader = Json.createReader(new StringReader(jsonBuilder.toString()));
		apiRequestObj = reader.readObject();
		reader.close();
	}

	public String getMauticUserName() {
		return mauticUserName;
	}

	public void setMauticUserName(String mauticUserName) {
		this.mauticUserName = mauticUserName;
	}

	public String getMauticPassword() {
		return mauticPassword;
	}

	public void setMauticPassword(String mauticPassword) {
		this.mauticPassword = mauticPassword;
	}

	public String getMauticApi() {
		return mauticApi;
	}

	public void setMauticApi(String mauticApi) {
		this.mauticApi = mauticApi;
	}

	public JsonObject getApiRequestObj() {
		return apiRequestObj;
	}

	public void setApiRequestObj(JsonObject apiRequestObj) {
		this.apiRequestObj = apiRequestObj;
	}
	
	
}
