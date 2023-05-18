postgres_tabular_storage_dataset_init = """
-- hash index is good for unique values
-- gin index is good for composite types

-- ################
-- ##### DATA #####
-- ################

CREATE TABLE schema_name_placeholder.data
(
    observation_id  uuid DEFAULT gen_random_uuid() NOT NULL,
    observation_uri text DEFAULT NULL,
    PRIMARY KEY (observation_id)
);

ALTER TABLE IF EXISTS schema_name_placeholder.data
    OWNER to user_placeholder;

-- ####################
-- ##### VERSIONS #####
-- ####################

CREATE TABLE schema_name_placeholder.versions
(
    id                      bigserial NOT NULL,
    name                    text DEFAULT NULL,
    dimension_names_postfix text default NULL,
    PRIMARY KEY (id)
);

ALTER TABLE IF EXISTS schema_name_placeholder.versions
    OWNER to user_placeholder;


CREATE TABLE schema_name_placeholder.version_observations
(
    version_id     bigint NOT NULL,
    observation_id uuid   NOT NULL,
    PRIMARY KEY (version_id, observation_id),
    CONSTRAINT version_id_fk FOREIGN KEY (version_id)
        REFERENCES schema_name_placeholder.versions (id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
        NOT VALID,
    CONSTRAINT observation_id_fk FOREIGN KEY (observation_id)
        REFERENCES schema_name_placeholder.data (observation_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE SET NULL
        NOT VALID
);

ALTER TABLE IF EXISTS schema_name_placeholder.version_observations
    OWNER to user_placeholder;


CREATE TABLE schema_name_placeholder.version_dimensions
(
    version_id     bigint NOT NULL,
    dimension_name text   NOT NULL,
    PRIMARY KEY (version_id, dimension_name),
    CONSTRAINT version_id_fk FOREIGN KEY (version_id)
        REFERENCES schema_name_placeholder.versions (id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
        NOT VALID
);

ALTER TABLE IF EXISTS schema_name_placeholder.version_dimensions
    OWNER to user_placeholder;

-- ####################
-- ##### METADATA #####
-- ####################

CREATE TABLE schema_name_placeholder.metadata_value_types
(
    id   bigserial NOT NULL,
    name text      NOT NULL,
    PRIMARY KEY (id)
);

ALTER TABLE IF EXISTS schema_name_placeholder.metadata_value_types
    OWNER to user_placeholder;


CREATE TABLE schema_name_placeholder.metadata
(
    id         bigserial NOT NULL,
    name       text      DEFAULT NULL,
    value      text      DEFAULT NULL,
    value_type bigint    DEFAULT NULL,
    is_private boolean   DEFAULT NULL,
    PRIMARY KEY (id),
    CONSTRAINT value_type_fk FOREIGN KEY (value_type)
        REFERENCES schema_name_placeholder.metadata_value_types (id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE SET NULL
        NOT VALID
);

ALTER TABLE IF EXISTS schema_name_placeholder.metadata
    OWNER to user_placeholder;


CREATE TABLE schema_name_placeholder.version_metadata
(
    version_id  bigint NOT NULL,
    metadata_id bigint NOT NULL,
    PRIMARY KEY (version_id, metadata_id),
    CONSTRAINT version_id_fk FOREIGN KEY (version_id)
        REFERENCES schema_name_placeholder.versions (id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
        NOT VALID,
    CONSTRAINT setting_id_fk FOREIGN KEY (metadata_id)
        REFERENCES schema_name_placeholder.metadata (id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE SET NULL
        NOT VALID
);

ALTER TABLE IF EXISTS schema_name_placeholder.version_metadata
    OWNER to user_placeholder;

"""

