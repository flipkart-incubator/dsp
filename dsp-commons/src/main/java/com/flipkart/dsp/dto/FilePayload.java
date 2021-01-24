package com.flipkart.dsp.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class FilePayload implements Payload {
    private String content;
}
