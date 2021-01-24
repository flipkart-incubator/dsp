package com.flipkart.dsp.dto.sg;

import com.flipkart.dsp.models.sg.SignalDataType;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SignalDTO {
    private String id;
    private SignalDataType signalDataType;
}
