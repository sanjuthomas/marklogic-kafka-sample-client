package org.sanju.marklogic.kafka.client.beans;

import java.util.Date;

/**
 *
 * @author Sanju Thomas
 *
 */
public class JSONQuoteRequest extends MarkLogicDocument {

    public JSONQuoteRequest(final String id, final String symbol,
            final int quantity, final Client client, final Date timestamp) {
        super(DocumentType.JSON);
        this.id = id;
        this.symbol = symbol;
        this.quantity = quantity;
        this.client = client;
        this.timestamp = timestamp;
        super.url(buildUrl());
    }

    private String id;
    private String symbol;
    private int quantity;
    private Client client;
    private Date timestamp;

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public Client getClient() {
        return this.client;
    }

    public void setClient(final Client client) {
        this.client = client;
    }

    public String getId() {
        return this.id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public String getSymbol() {
        return this.symbol;
    }

    public void setSymbol(final String symbol) {
        this.symbol = symbol;
    }

    public int getQuantity() {
        return this.quantity;
    }

    public void setQuantity(final int quantity) {
        this.quantity = quantity;
    }

    public String buildUrl() {
        final StringBuilder builder = new StringBuilder();
        builder.append("/");
        builder.append(this.client.getId());
        builder.append("/");
        builder.append(this.client.getAccount().getId());
        builder.append("/");
        builder.append(this.getId());
        builder.append(".json");
        return builder.toString();
    }
}
