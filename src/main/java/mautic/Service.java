package mautic;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Base64;

import javax.json.Json;
import javax.json.JsonObject;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

public class Service {

	public Service() {}
	
	public JsonObject create(MauticUser mUser) throws ClientProtocolException, IOException {
		String authString = mUser.getMauticUserName() + ":" + mUser.getMauticPassword();
		String authString_encoded =  Base64.getEncoder().encodeToString(authString.getBytes());
		DefaultHttpClient httpclient = new DefaultHttpClient();
		String url = mUser.getMauticApi();
		if(url.endsWith("/")) {
			url = url + "contacts/new";
		}else {
			url = url + "/contacts/new";
		}
		HttpPost httpPost = new HttpPost(url);
		httpPost.setHeader("Authorization", "Basic " + authString_encoded);
		StringEntity entity = new StringEntity(mUser.getApiRequestObj().toString());
		httpPost.setEntity(entity);
		httpPost.setHeader("Accept", "application/json");
		httpPost.setHeader("Content-type", "application/json");
		
		HttpResponse response = httpclient.execute(httpPost);
        JsonObject responseObj = Json.createReader(new InputStreamReader(response.getEntity().getContent())).readObject();
        return responseObj;
	}
	
	public JsonObject addToSegment(MauticUser mUser, String segmentId, int contactId) throws ClientProtocolException, IOException {
		String authString = mUser.getMauticUserName() + ":" + mUser.getMauticPassword();
		String authString_encoded =  Base64.getEncoder().encodeToString(authString.getBytes());
		DefaultHttpClient httpclient = new DefaultHttpClient();
		String url = mUser.getMauticApi();
		if(url.endsWith("/")) {
			url = url + "segments/" + Integer.parseInt(segmentId) + "/contact/" + contactId + "/add";
		}else {
			url = url + "/segments/" + Integer.parseInt(segmentId) + "/contact/" + contactId + "/add";
		}
		HttpPost httpPost = new HttpPost(url);
		httpPost.setHeader("Authorization", "Basic " + authString_encoded);
		StringEntity entity = new StringEntity(mUser.getApiRequestObj().toString());
		httpPost.setEntity(entity);
		httpPost.setHeader("Accept", "application/json");
		httpPost.setHeader("Content-type", "application/json");
		
		HttpResponse response = httpclient.execute(httpPost);
        JsonObject responseObj = Json.createReader(new InputStreamReader(response.getEntity().getContent())).readObject();
        return responseObj;
	}
}
