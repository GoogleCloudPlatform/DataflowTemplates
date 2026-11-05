CREATE TABLE Users (
  user_id BIGINT NOT NULL,
  event_id VARCHAR(100) NOT NULL,
  full_name VARCHAR(100),
  age INT,
  created_at TIMESTAMP,
  PRIMARY KEY (user_id, event_id)
);

CREATE TABLE AccountRoles (
  role_id BIGINT NOT NULL,
  role_name VARCHAR(100),
  PRIMARY KEY (role_id)
);
