package com.thesis.sqlite.messages.kafka;

import com.thesis.sqlite.dto.nodes.NodeInfos;

public class NodeAdded {
    private final String nodeName;
    private final NodeInfos nodeInfo;

    public NodeAdded(String nodeName, NodeInfos nodeInfo) {
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
