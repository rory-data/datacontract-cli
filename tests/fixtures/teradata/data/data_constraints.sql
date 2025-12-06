CREATE TABLE customer_location (
  id DECIMAL NOT NULL,
  created_by VARCHAR(30) NOT NULL,
  create_date TIMESTAMP NOT NULL,
  changed_by VARCHAR(30),
  change_date TIMESTAMP,
  CONSTRAINT pk_customer_location PRIMARY KEY (id)
);
