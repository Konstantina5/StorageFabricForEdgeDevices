package com.thesis.sqlite.controllers;

import com.thesis.sqlite.components.NodesInfoManager;
import com.thesis.sqlite.results.Client;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api/info")
public class NodeInfo {
    @Autowired
    private NodesInfoManager nodesInfoManager;

    @GetMapping
    public ResponseEntity<?> getAll() {
        return Client.Results.ok(nodesInfoManager.getTableInfos().values());
    }

}
