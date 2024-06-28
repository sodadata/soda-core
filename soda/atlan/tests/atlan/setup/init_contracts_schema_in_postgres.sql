CREATE SCHEMA IF NOT EXISTS contracts AUTHORIZATION CURRENT_USER;

DROP TABLE IF EXISTS contracts.students;

CREATE TABLE contracts.students (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    age INT
);

INSERT INTO contracts.students VALUES
('1', 'John Doe',   30),
('2', 'Jack Black', 40),
('3', NULL,         50)
;
