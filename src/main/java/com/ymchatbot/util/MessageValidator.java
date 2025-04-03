package com.ymchatbot.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Utility class to validate and standardize message formats using Jackson
 */
public class MessageValidator {
   
    @SuppressWarnings("unused")
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Validates and standardizes a message to ensure it's a valid JSON array
     * 
     * @param messageBody The raw message body string
     * @param mapper The ObjectMapper instance to use
     * @return A standardized ArrayNode
     * @throws JsonProcessingException If the message cannot be parsed as JSON
     */
    public static ArrayNode validateAndStandardize(String messageBody, ObjectMapper mapper) throws JsonProcessingException {
        if (messageBody == null || messageBody.trim().isEmpty()) {
            return mapper.createArrayNode();
        }

        try {
            // Try parsing as ArrayNode first
            return (ArrayNode) mapper.readTree(messageBody);
        } catch (JsonProcessingException e) {
            try {
                // If not an array, try as a single ObjectNode and wrap it
                ObjectNode jsonObject = (ObjectNode) mapper.readTree(messageBody);
                ArrayNode array = mapper.createArrayNode();
                array.add(jsonObject);
                return array;
            } catch (JsonProcessingException e2) {
                LoggerUtil.error("Message is neither a valid JSON array nor object: " +
                        messageBody.substring(0, Math.min(100, messageBody.length())) + "...");
                throw e2; // Re-throw the exception to be handled by the caller
            }
        }
    }

    /**
     * Extracts campaign ID from a message item
     * 
     * @param item ObjectNode representing a message item
     * @return campaign ID
     * @throws IllegalArgumentException if campaign ID cannot be found
     */
    public static int extractCampaignId(ObjectNode item) {
        try {
            if (item.has("data") && !item.get("data").isNull()) {
                return item.get("data").get("messenger_bot_broadcast_serial").asInt();
            }
        } catch (Exception e) {
            // Ignore and try alternatives
        }

        try {
            return item.get("messenger_bot_broadcast_serial").asInt();
        } catch (Exception e) {
            try {
                return item.get("campaign_id").asInt();
            } catch (Exception e2) {
                throw new IllegalArgumentException("Unable to find campaign ID in message");
            }
        }
    }

    /**
     * Normalizes a request object to ensure it has the expected structure
     * 
     * @param item The message item to normalize
     * @param mapper The ObjectMapper instance to use
     * @return A normalized message item with proper request structure
     */
    public static ObjectNode normalizeRequestStructure(ObjectNode item, ObjectMapper mapper) {
        // If item already has a proper request structure, return it
        if (item.has("request") && item.get("request").has("url")) {
            return item;
        }

        // Otherwise, attempt to build a proper structure
        ObjectNode normalized = item.deepCopy();

        // If the item has a URL directly
        if (item.has("url")) {
            ObjectNode request = mapper.createObjectNode();
            request.put("url", item.get("url").asText());

            // If the item has data, move it into the request
            if (item.has("data")) {
                request.set("data", item.get("data"));
            } else {
                // Create an empty data object
                request.set("data", mapper.createObjectNode());
            }

            normalized.set("request", request);
        }

        return normalized;
    }

    /**
     * Validates a message from the update-status queue
     * 
     * @param messageBody The raw message body string
     * @param mapper The ObjectMapper instance to use
     * @return A validated ObjectNode
     * @throws JsonProcessingException If the message cannot be parsed as JSON
     */
    public static ObjectNode validateUpdateStatusMessage(String messageBody, ObjectMapper mapper) throws JsonProcessingException {
        if (messageBody == null || messageBody.trim().isEmpty()) {
            throw new IllegalArgumentException("Empty message body");
        }

        try {
            ObjectNode message = (ObjectNode) mapper.readTree(messageBody);

            // Validate required fields
            if (!message.has("messenger_bot_broadcast_serial")) {
                throw new IllegalArgumentException("Missing required field: messenger_bot_broadcast_serial");
            }

            return message;
        } catch (JsonProcessingException e) {
            LoggerUtil.error("Invalid update status message: " +
                    messageBody.substring(0, Math.min(100, messageBody.length())) + "...");
            throw e;
        }
    }
}