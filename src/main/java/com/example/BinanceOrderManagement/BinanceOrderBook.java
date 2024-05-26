package com.example.BinanceOrderManagement;


import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BinanceOrderBook {
	private static final String API_BASE_URL = "https://api.binance.com";
	private static final String WS_BASE_URL = "wss://stream.binance.com:9443/ws/";
	private static final int DEPTH = 5;
	private static final List<String> TRADING_PAIRS = Arrays.asList("btcusdt", "ethusdt");

	private static class OrderBook {
		TreeMap<Double, Double> bids = new TreeMap<>(Collections.reverseOrder());
		TreeMap<Double, Double> asks = new TreeMap<>();
	}

	private static final ConcurrentHashMap<String, OrderBook> orderBooks = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<String, Double> previousVolumes = new ConcurrentHashMap<>();

	public static void main(String[] args) {
		Gson gson = new Gson();
		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

		for (String pair : TRADING_PAIRS) {
			orderBooks.put(pair, new OrderBook());
			initializeOrderBook(pair, gson);
			connectWebSocket(pair, gson);
		}

		executor.scheduleAtFixedRate(() -> {
			for (String pair : TRADING_PAIRS) {
				double currentVolume = calculateTotalVolume(pair);
				double previousVolume = previousVolumes.getOrDefault(pair, 0.0);
				double volumeChange = currentVolume - previousVolume;

				System.out.println("Order book for " + pair.toUpperCase());
				printOrderBook(pair);
				System.out.println("Total volume change in USDT: " + volumeChange + "\n");

				previousVolumes.put(pair, currentVolume);
			}
		}, 10, 10, TimeUnit.SECONDS);
	}

	private static void initializeOrderBook(String pair, Gson gson) {
		try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
			HttpGet request = new HttpGet(API_BASE_URL + "/api/v3/depth?symbol=" + pair.toUpperCase() + "&limit=" + DEPTH);
			CloseableHttpResponse response = httpClient.execute(request);
			String responseBody = EntityUtils.toString(response.getEntity());

			JsonObject json = gson.fromJson(responseBody, JsonObject.class);
			OrderBook orderBook = orderBooks.get(pair);

			JsonArray bidsArray = json.getAsJsonArray("bids");
			for (int i = 0; i < bidsArray.size(); i++) {
				JsonArray entry = bidsArray.get(i).getAsJsonArray();
				double price = entry.get(0).getAsDouble();
				double quantity = entry.get(1).getAsDouble();
				orderBook.bids.put(price, quantity);
			}

			JsonArray asksArray = json.getAsJsonArray("asks");
			for (int i = 0; i < asksArray.size(); i++) {
				JsonArray entry = asksArray.get(i).getAsJsonArray();
				double price = entry.get(0).getAsDouble();
				double quantity = entry.get(1).getAsDouble();
				orderBook.asks.put(price, quantity);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void connectWebSocket(String pair, Gson gson) {
		String wsUrl = WS_BASE_URL + pair + "@depth";
		WebSocketClient client = new WebSocketClient(URI.create(wsUrl)) {
			@Override
			public void onOpen(ServerHandshake handshakedata) {
				System.out.println("WebSocket connection opened for " + pair.toUpperCase());
			}

			@Override
			public void onMessage(String message) {
				JsonObject json = gson.fromJson(message, JsonObject.class);
				OrderBook orderBook = orderBooks.get(pair);

				JsonArray bidsArray = json.getAsJsonArray("b");
				for (int i = 0; i < bidsArray.size(); i++) {
					JsonArray entry = bidsArray.get(i).getAsJsonArray();
					double price = entry.get(0).getAsDouble();
					double quantity = entry.get(1).getAsDouble();
					if (quantity == 0) {
						orderBook.bids.remove(price);
					} else {
						orderBook.bids.put(price, quantity);
					}
				}

				JsonArray asksArray = json.getAsJsonArray("a");
				for (int i = 0; i < asksArray.size(); i++) {
					JsonArray entry = asksArray.get(i).getAsJsonArray();
					double price = entry.get(0).getAsDouble();
					double quantity = entry.get(1).getAsDouble();
					if (quantity == 0) {
						orderBook.asks.remove(price);
					} else {
						orderBook.asks.put(price, quantity);
					}
				}
			}

			@Override
			public void onClose(int code, String reason, boolean remote) {
				System.out.println("WebSocket connection closed for " + pair.toUpperCase());
			}

			@Override
			public void onError(Exception ex) {
				ex.printStackTrace();
			}
		};

		client.connect();
	}

	private static double calculateTotalVolume(String pair) {
		OrderBook orderBook = orderBooks.get(pair);
		double totalVolume = 0.0;

		for (Map.Entry<Double, Double> entry : orderBook.bids.entrySet()) {
			totalVolume += entry.getKey() * entry.getValue();
		}

		for (Map.Entry<Double, Double> entry : orderBook.asks.entrySet()) {
			totalVolume += entry.getKey() * entry.getValue();
		}

		return totalVolume;
	}

	private static void printOrderBook(String pair) {
		OrderBook orderBook = orderBooks.get(pair);
		System.out.println("Bids:");
		orderBook.bids.forEach((price, quantity) -> System.out.println(price + " : " + quantity));

		System.out.println("Asks:");
		orderBook.asks.forEach((price, quantity) -> System.out.println(price + " : " + quantity));
	}
}
