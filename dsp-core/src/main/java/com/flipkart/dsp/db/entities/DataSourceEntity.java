package com.flipkart.dsp.db.entities;

import com.flipkart.dsp.entities.sg.core.BaseEntity;
import com.flipkart.dsp.models.sg.DataSourceConfiguration;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.Parameter;
import org.hibernate.annotations.Type;

import javax.persistence.*;
import java.io.Serializable;

/**
 */

@Data
@Entity
@DynamicUpdate
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@Table(name = "data_sources", uniqueConstraints = @UniqueConstraint(columnNames = {"configuration"}))
public class DataSourceEntity extends BaseEntity implements Serializable {

    @Id
    @Column(length = 50)
    private String id;

    @Type(type = "com.flipkart.dsp.utils.JsonAbstractDataType",
            parameters = {@Parameter(name = "classType",
                    value = "com.flipkart.dsp.models.sg.DataSourceConfiguration")})
    @Column(name = "configuration", nullable = false)
    private DataSourceConfiguration dataSourceConfiguration;
}
