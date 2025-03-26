package com.example.kafka.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author JungleXia
 * @version 1.0
 * @title IndexController
 * @description first page
 * @date 2025/3/26 14:09:24
 */
@RestController
public class IndexController {

    @RequestMapping(value = "/index", method = RequestMethod.GET)
    public String sendMsg(String topicName) {
        return "hello success";
    }
}
