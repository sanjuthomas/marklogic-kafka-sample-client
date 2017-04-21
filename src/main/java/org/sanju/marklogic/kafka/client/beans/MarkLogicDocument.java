package org.sanju.marklogic.kafka.client.beans;

import java.util.UUID;

/**
 * 
 * @author Sanju Thomas
 * 
 * To make the url and content type available in the message.
 *
 */
public abstract class MarkLogicDocument {
    
    private String url;
    private String type;
    
    protected MarkLogicDocument(DocumentType t){
        this.type = t.getV();
    }
    
    public String getUrl() {
        return null == url ? UUID.randomUUID().toString() : url;
    }

    public String getType() {
        return type;
    }
    
    protected void url(String url){
        this.url = url;
    }

    public enum DocumentType{
        
        JSON("application/json"),
        XML("application/xml");
        
        private String v;
        
        public String getV() {
            return v;
        }
        DocumentType(String v){
            this.v =v;
        }
    }
}
