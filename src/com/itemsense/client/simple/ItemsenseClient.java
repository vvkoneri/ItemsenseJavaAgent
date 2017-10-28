package com.itemsense.client.simple;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;

import javax.net.ssl.SSLContext;

import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.thingworx.relationships.RelationshipTypes.ThingworxEntityTypes;
import com.thingworx.types.collections.ValueCollection;

public class ItemsenseClient {

	static ThingworxClient client = ThingworxClientFactory.getClient();
	static String lastRecordedTime = null;
	private static final Logger LOG = LoggerFactory.getLogger(ItemsenseClient.class);

	public static void main(String[] args) throws JSONException, InterruptedException {
		// TODO Auto-generated method stub
		while (true) {
			
				String jobId = getJobId();
				
				if(jobId==null) {
					continue;
				}
				
				HttpResponse<String> response = getHttpResponseCall(jobId);
				
				if(response == null) {
					continue;
				}
				
				LOG.debug(response.getBody());
				
				ArrayList<JSONObject> itemObjects = getItems(response);
				
				

				if (!itemObjects.isEmpty()) {
					try {
						groupAndPush(itemObjects);
					} catch (Exception e) {
						System.out.println("Exception caused due to " + e.getMessage());
					}
				}
			Thread.sleep(5000);
		}
	}

	public static String getDateInIso8601String(Date date) {
		TimeZone tz = TimeZone.getTimeZone("UTC");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		df.setTimeZone(tz);
		return df.format(date);
	}

	public static Date getDateInInIso8601(String dateString) throws Exception {
		TimeZone tz = TimeZone.getTimeZone("UTC");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		df.setTimeZone(tz);
		return df.parse(dateString);

	}

	public static void groupAndPush(ArrayList<JSONObject> filterItemsNotAbsent) throws Exception {

		Date latestTime = null;
		HashMap<String, JSONObject> latestEpcs = new HashMap<String, JSONObject>();
		for (int idx = 0; idx < filterItemsNotAbsent.size(); idx++) {
			JSONObject item = filterItemsNotAbsent.get(idx);
			if (latestEpcs.isEmpty()) {
				latestEpcs.put(item.getString("epc"), item);
				latestTime = getDateInInIso8601(item.getString("lastModifiedTime"));
				continue;
			}

			if (latestEpcs.containsKey(item.getString("epc"))) {
				JSONObject epcItem = latestEpcs.get(item.getString("epc"));
				Date existingDate = getDateInInIso8601(item.getString("lastModifiedTime"));
				Date newDate = getDateInInIso8601(item.getString("lastModifiedTime"));

				System.out.println(existingDate + " " + newDate);

				if (newDate.getTime() > existingDate.getTime()) {

					latestEpcs.put(item.getString("epc"), item);
				}

				if (newDate.getTime() > latestTime.getTime()) {
					latestTime = newDate;
				}
			} else {
				latestEpcs.put(item.getString("epc"), item);
				Date newDate = getDateInInIso8601(item.getString("lastModifiedTime"));
				if (newDate.getTime() > latestTime.getTime()) {
					latestTime = newDate;
				}
			}
		}

		Iterator it = latestEpcs.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			JSONObject item = (JSONObject) pair.getValue();
			ValueCollection valueCol = new ValueCollection();
			valueCol.SetStringValue("rf_Id", item.getString("epc"));
			valueCol.SetStringValue("zone_Id", item.getString("zone"));
			// valueCol.SetStringValue("itemsense_Timestamp",
			// item.getString("lastModifiedTime"));
			client.writeProperties(ThingworxEntityTypes.Things, "Antenna_01", valueCol, 5000);
			System.out.println(pair.getKey() + " = " + ((JSONObject) pair.getValue()).getString("lastModifiedTime"));
		}
		
		lastRecordedTime = getDateInIso8601String(latestTime);

	}

	public static void killRfids(String rfId) {

		try {
			SSLContext sslcontext = SSLContexts.custom().loadTrustMaterial(null, new TrustSelfSignedStrategy()).build();

			SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslcontext,
					SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
			CloseableHttpClient httpclient = HttpClients.custom().setSSLSocketFactory(sslsf).build();
			Unirest.setHttpClient(httpclient);

			HttpResponse<String> killResponse = Unirest
					.post("https://13.93.204.147/Thingworx/Things/Antenna_01/Services/DeleteRfidEntry?rf_Id=" + rfId)
					.header("appkey", "e6159a5e-34c0-45fa-9a5a-a8f7f366b6df").header("Content-type", "application/json")
					.asString();

		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
			e.printStackTrace();
		} catch (UnirestException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public static HttpResponse<String> getHttpResponseCall(String jobId) {
		
		HttpResponse<String> response = null;
		try {
		if (lastRecordedTime == null || lastRecordedTime.isEmpty()) {
			response = Unirest
					.get(ItemsenseConfig.ITEMSENSE_BASE_ITEMS_URL + jobId)
					.basicAuth(ItemsenseConfig.ITEMSENSE_USERNAME, ItemsenseConfig.ITEMSENSE_PASSWORD)
					.header("Content-Type", "application/json").asString();
		} else {
			response = Unirest
					.get(ItemsenseConfig.ITEMSENSE_BASE_ITEMS_URL + jobId + "&fromTime="+lastRecordedTime)
					.basicAuth(ItemsenseConfig.ITEMSENSE_USERNAME, ItemsenseConfig.ITEMSENSE_PASSWORD)
					.header("Content-Type", "application/json").asString();
		}
		} catch (Exception e) {
			LOG.error("Exception caused due " + e.getMessage());
		}
		
		return response;
		
	}
	
	public static String getJobId() {
		
		try {
		HttpResponse<String> response = Unirest
				.get(ItemsenseConfig.ITEMSENSE_BASE_JOBS_URL)
				.basicAuth(ItemsenseConfig.ITEMSENSE_USERNAME, ItemsenseConfig.ITEMSENSE_PASSWORD)
				.header("Content-Type", "application/json").asString();
		JSONObject jobs = new JSONObject(response.getBody());
		JSONArray jobArr = jobs.getJSONArray("jobs");
		
		for(int jb_idx=0; jb_idx < jobArr.length() ; jb_idx++) {
			JSONObject jobObj = jobArr.getJSONObject(jb_idx);
			if(jobObj.getString("status").equalsIgnoreCase("RUNNING")) {
				return jobObj.getString("id");	
			}
		}
		} catch(Exception e) {
			LOG.error("Exception caused due " + e.getMessage());
		}
		
		return null;
	}
	
	public static ArrayList<JSONObject> getItems(HttpResponse<String> response) {
		
		JSONObject itemObj = new JSONObject(response.getBody());
		JSONArray items = itemObj.getJSONArray("items");
		ArrayList<JSONObject> itemObjects = new ArrayList<JSONObject>();
		
		
		for (int i = 0; i < items.length(); i++) {
			JSONObject item = items.getJSONObject(i);
			String zone = item.getString("zone");
			String rfid = item.getString("epc");
			if (zone.equalsIgnoreCase("ABSENT")) {
				killRfids(rfid);
				continue;
			}

			itemObjects.add(item);

		}
		
		return itemObjects;
		
	}

}
