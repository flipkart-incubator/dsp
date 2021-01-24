package com.flipkart.dsp.db.entities;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.DynamicUpdate;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.io.Serializable;

/**
 * +
 */
@Data
@Entity
@Builder
@DynamicUpdate
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "external_credentials")
public class ExternalCredentialsEntity implements Serializable {

    @Id
    @Column(name = "client_alias")
    private String clientAlias;

    @Column(name = "details")
    private String details;
}
