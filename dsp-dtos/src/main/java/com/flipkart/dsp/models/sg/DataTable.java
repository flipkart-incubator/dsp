package com.flipkart.dsp.models.sg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.Set;

/**
 */

@Data
@AllArgsConstructor
@EqualsAndHashCode(exclude = {"description","dataSource","tableCreationConfig","sgUseCase","signalBaseEntitySet"})
@NoArgsConstructor
public class DataTable {
    private String id;
    private String description;
    private DataSource dataSource;
}
