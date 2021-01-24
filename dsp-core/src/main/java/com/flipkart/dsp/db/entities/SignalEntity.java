package com.flipkart.dsp.db.entities;

import com.flipkart.dsp.entities.sg.core.BaseEntity;
import com.flipkart.dsp.models.sg.SignalDataType;
import com.flipkart.dsp.models.sg.SignalDefinition;
import lombok.*;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.Type;

import javax.persistence.*;
import java.util.Objects;

/**
 */

@Data
@Builder
@Entity
@DynamicUpdate
@NoArgsConstructor
@AllArgsConstructor
@javax.persistence.Table(name = "signals", uniqueConstraints = @UniqueConstraint(columnNames = {"name","data_type","signal_definition", "base_entity"}))
public class SignalEntity extends BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(length = 50)
    private String name;

    @Column(name = "data_type", nullable = false)
    @Enumerated(EnumType.STRING)
    private SignalDataType signalDataType;

    @Type(type = "com.flipkart.dsp.utils.JsonAbstractDataType",
            parameters = {@org.hibernate.annotations.Parameter(name = "classType",
                    value = "com.flipkart.dsp.models.sg.SignalDefinition")})
    @Column(name = "signal_definition")
    private SignalDefinition signalDefinition;

    @Column(name = "base_entity", nullable = false)
    private String baseEntity;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SignalEntity)) return false;
        SignalEntity that = (SignalEntity) o;
        return Objects.equals(getName(), that.getName()) &&
                getSignalDataType() == that.getSignalDataType() &&
                Objects.equals(getSignalDefinition(), that.getSignalDefinition()) &&
                Objects.equals(getBaseEntity(), that.getBaseEntity());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getName(), getSignalDataType(), getSignalDefinition(), getBaseEntity());
    }
}
