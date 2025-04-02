package com.ymchatbot;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Utility class to validate and standardize message formats
 */
public class MessageValidator {

    /**
     * Validates and standardizes a message to ensure it's a valid JSON array
     * 
     * @param messageBody The raw message body string
     * @return A standardized JSONArray
     * @throws JSONException If the message cannot be parsed as JSON
     */
    public static JSONArray validateAndStandardize(String messageBody) throws JSONException {
        if (messageBody == null || messageBody.trim().isEmpty()) {
            return new JSONArray();
        }

        try {
            // Try parsing as JSONArray first
            return new JSONArray(messageBody);
        } catch (JSONException e) {
            try {
                // If not an array, try as a single JSONObject and wrap it
                JSONObject jsonObject = new JSONObject(messageBody);
                JSONArray array = new JSONArray();
                array.put(jsonObject);
                return array;
            } catch (JSONException e2) {
                LoggerUtil.error("Message is neither a valid JSON array nor object: " +
                        messageBody.substring(0, Math.min(100, messageBody.length())) + "...");
                throw e2; // Re-throw the exception to be handled by the caller
            }
        }
    }

    /**
     * Extracts campaign ID from a message item
     * 
     * @param item JSONObject representing a message item
     * @return campaign ID
     * @throws JSONException if campaign ID cannot be found
     */
    public static int extractCampaignId(JSONObject item) throws JSONException {
        try {
            if (item.has("data") && !item.isNull("data")) {
                return item.getJSONObject("data").getInt("messenger_bot_broadcast_serial");
            }
        } catch (JSONException e) {
            // Ignore and try alternatives
        }

        try {
            return item.getInt("messenger_bot_broadcast_serial");
        } catch (JSONException e) {
            try {
                return item.getInt("campaign_id");
            } catch (JSONException e2) {
                throw new JSONException("Unable to find campaign ID in message");
            }
        }
    }

    /**
     * Normalizes a request object to ensure it has the expected structure
     * 
     * @param item The message item to normalize
     * @return A normalized message item with proper request structure
     * @throws JSONException If the message cannot be normalized
     */
    public static JSONObject normalizeRequestStructure(JSONObject item) throws JSONException {
        // If item already has a proper request structure, return it
        if (item.has("request") && item.getJSONObject("request").has("url")) {
            return item;
        }

        // Otherwise, attempt to build a proper structure
        JSONObject normalized = new JSONObject(item.toString()); // Clone to avoid modifying original

        // If the item has a URL directly
        if (item.has("url")) {
            JSONObject request = new JSONObject();
            request.put("url", item.getString("url"));

            // If the item has data, move it into the request
            if (item.has("data")) {
                request.put("data", item.get("data"));
            } else {
                // Create an empty data object
                request.put("data", new JSONObject());
            }

            normalized.put("request", request);
        }

        return normalized;
    }

    /**
     * Validates a message from the update-status queue
     * 
     * @param messageBody The raw message body string
     * @return A validated JSONObject
     * @throws JSONException If the message cannot be parsed as JSON
     */
    public static JSONObject validateUpdateStatusMessage(String messageBody) throws JSONException {
        if (messageBody == null || messageBody.trim().isEmpty()) {
            throw new JSONException("Empty message body");
        }

        try {
            JSONObject message = new JSONObject(messageBody);

            // Validate required fields
            if (!message.has("messenger_bot_broadcast_serial")) {
                throw new JSONException("Missing required field: messenger_bot_broadcast_serial");
            }

            return message;
        } catch (JSONException e) {
            LoggerUtil.error("Invalid update status message: " +
                    messageBody.substring(0, Math.min(100, messageBody.length())) + "...");
            throw e;
        }
    }
}