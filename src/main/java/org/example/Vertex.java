package org.example;

public class Vertex {
    private String verName;
    private Edge edgeLink;

    public Vertex(String verName){
        this.verName = verName;
        edgeLink = null;
    }

    public void setVerName(String verName){
        this.verName = verName;
    }

    public void setEdgeLink(Edge edgeLink){
        this.edgeLink = edgeLink;
    }

    public String getVerName(){
        return this.verName;
    }

    public Edge getEdgeLink(){
        return edgeLink;
    }

    @Override
    public String toString() {
        Edge head = this.edgeLink;
        StringBuilder edgeMessage = new StringBuilder();
        while(head != null){
            edgeMessage.append(edgeLink.toString()).append("\n");
            head = head.getBroEdge();
        }
        return "Vertex:" + this.verName + "edgeMessage:" + edgeMessage;
    }
}
