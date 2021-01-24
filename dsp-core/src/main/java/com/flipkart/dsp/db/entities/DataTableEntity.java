package com.flipkart.dsp.db.entities;

import com.flipkart.dsp.entities.sg.core.BaseEntity;
import lombok.*;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

/**
 */

@Entity
@Getter
@NoArgsConstructor
@DynamicUpdate
@EqualsAndHashCode(callSuper = false, exclude = {"description"})
@Table(name = "data_tables", uniqueConstraints = @UniqueConstraint(columnNames = {"id", "data_source_id"}))
@AllArgsConstructor
public class DataTableEntity extends BaseEntity implements Serializable {

    @Id
    @Column(length = 50)
    private String id;

    @Column
    private String description;

    @ManyToOne
    @JoinColumn(name = "data_source_id", referencedColumnName = "id", nullable = false)
    private DataSourceEntity dataSource;

    @OneToMany(mappedBy = "dataTableEntity")
    private Set<SignalGroupToSignalEntity> signalGroupToSignalEntities;

    public DataTableEntity(String id, String description, DataSourceEntity dataSource) {
        this.id = id;
        this.description = description;
        this.dataSource = dataSource;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DataTableEntity)) return false;
        DataTableEntity that = (DataTableEntity) o;
        return Objects.equals(getId(), that.getId()) &&
                Objects.equals(getDataSource().getId(), that.getDataSource().getId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getDataSource().getId());
    }
}
