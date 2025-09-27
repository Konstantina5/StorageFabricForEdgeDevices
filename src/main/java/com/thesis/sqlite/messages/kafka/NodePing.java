package com.thesis.sqlite.messages.kafka;

import com.thesis.sqlite.dto.nodes.NodeInfos;
import com.thesis.sqlite.messages.kafka.base.NodeInfo;

public class NodePing extends NodeInfo {
    public NodePing(String nodeName, NodeInfos nodeInfo) {
        super(nodeName, nodeInfo);
    }
}
