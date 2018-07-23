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
	private JsonObject apiRequestObj;
	
	public MauticUser(String mauticClient) {
		switch(mauticClient) {
		
		case "ferguson":
			mauticUserName = "";
			mauticPassword = "";
			mauticApi = "";
			break;
		case "lvcc":
			mauticUserName = "";
			mauticPassword = "";
			mauticApi = "";
			break;
		default:
			System.out.println("Unkown mautic client");
			break;
		}
	}
	
	public void setRecord(String line) {
		String[] elemets = line.split("|");
		String[] fieldNames = "firstname|lastname|email|ptype".split("|");
		StringBuilder jsonBuilder = new StringBuilder("{");
		for(int i=0;i<fieldNames.length;i++) {
			jsonBuilder.append("\"");jsonBuilder.append(fieldNames[i]);jsonBuilder.append("\":");
			jsonBuilder.append("\"");jsonBuilder.append(elemets[i]);jsonBuilder.append("\"");
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
