package org.example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TimeGraph {
    int verNum;
    int edgeNum;
    HashMap<String, Vertex> verMap;

    public TimeGraph(){
        this.verNum = 0;
        this.edgeNum = 0;
        this.verMap = new HashMap<>();
    }

    public boolean addVertex(String verName){
        if(verName.equals("")){
            return false;
        }
        if(this.verMap.containsKey(verName)){
            System.out.println("Vertex " + verName +" has been create");
            return false;
        }else{
            Vertex newVertex = new Vertex(verName);
            verMap.put(verName, newVertex);
            this.verNum++;
            return true;
        }
    }

    public boolean addEdge(String tailName, String headName, int upperLimit, int lowerLimit){
        if(!this.verMap.containsKey(tailName)){
            System.out.println(tailName + "had not been created");
            if(!this.addVertex(tailName)){
                System.out.println("Create vertex " + tailName + " is fail");
                return false;
            }
        }
        if(!this.verMap.containsKey(headName)){
            if(!this.addVertex(headName)){
                System.out.println("Create vertex " + headName + " is fail");
                return false;
            }
        }
        Vertex targetVertex = this.verMap.get(tailName);
        Edge current = targetVertex.getEdgeLink();
        Edge newEdge = new Edge(current,headName,upperLimit,lowerLimit);
        targetVertex.setEdgeLink(newEdge);
        this.edgeNum++;
        return true;
    }

    public List<Edge> targetEdges(String tailName){
        if(!this.verMap.containsKey(tailName)){
            System.out.println("Vertex: " + tailName + " do not exist");
            return null;
        }

        Edge tailEdgeLink = verMap.get(tailName).getEdgeLink();
        if(tailEdgeLink != null){
            List<Edge> targetList = new ArrayList<>();
            while(tailEdgeLink != null){
                targetList.add(tailEdgeLink);
                tailEdgeLink = tailEdgeLink.getBroEdge();
            }
            return targetList;
        }
        return null;
    }

    public void setVerNum(int verNum){
        this.verNum = verNum;
    }

    public void setEdgeNum(int edgeNum){
        this.edgeNum = edgeNum;
    }

    public void setVerMap(HashMap<String, Vertex> verMap){
        this.verMap = verMap;
    }

    public int getVerNum(){
        return this.verNum;
    }

    public int getEdgeNum(){
        return this.edgeNum;
    }

    public HashMap<String, Vertex> getVerMap(){
        return this.verMap;
    }

    @Override
    public String toString() {
        StringBuilder resultString = new StringBuilder();
        for(Vertex vertex : this.verMap.values()){
            resultString.append(vertex.toString()).append("\n");
        }
        return "" + resultString;
    }
}
