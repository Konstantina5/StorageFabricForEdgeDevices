package com.thesis.sqlite.messages.kafka.base;

import com.thesis.sqlite.dto.nodes.NodeInfos;

public class NodeInfo {
    private final String nodeName;
    private final NodeInfos nodeInfo;

    public NodeInfo(String nodeName, NodeInfos nodeInfo) {
        this.nodeName = nodeName;
        this.nodeInfo = nodeInfo;
    }

    public String getNodeName() {
        return nodeName;
    }

    public NodeInfos getNodeInfo() {
        return nodeInfo;
    }
}
