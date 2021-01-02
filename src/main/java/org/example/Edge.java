package org.example;

public class Edge {
    private String headName;
    private Edge broEdge;
    private int upperLimit;
    private int lowerLimit;

    public Edge(Edge broEdge, String headName, int upperLimit, int lowerLimit){
        this.headName = headName;
        this.upperLimit = upperLimit;
        this.lowerLimit = lowerLimit;
        this.broEdge = broEdge;
    }

    public void setBroEdge(Edge broName){
        this.broEdge = broName;
    }

    public void setHeadName(String headName){
        this.headName = headName;
    }

    public void setUpperLimit(int upperLimit){
        this.upperLimit = upperLimit;
    }

    public void setLowerLimit(int lowerLimit){
        this.lowerLimit = lowerLimit;
    }

    public String getHeadName(){
        return this.headName;
    }

    public Edge getBroEdge(){
        return this.broEdge;
    }

    public int getUpperLimit(){
        return this.upperLimit;
    }

    public int getLowerLimit(){
        return this.lowerLimit;
    }

    //tailName:B, weight:(2,5)
    @Override
    public String toString() {
        return "headName:" + this.headName + ", weight:(" + this.lowerLimit + "," + this.upperLimit + ")";
    }
}
