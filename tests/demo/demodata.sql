DROP TABLE IF EXISTS DEMODATA CASCADE;

CREATE TABLE DEMODATA (
  ID VARCHAR(255),
  NAME VARCHAR(255),
  SIZE INT,
  DATE DATE,
  FEEPCT VARCHAR(255),
  COUNTRY VARCHAR(255)
);

INSERT INTO DEMODATA VALUES
  ( 'a76824f0-50c0-11eb-8be8-88e9fe6293fd', 'Paula Landry', 3006, DATE '2021-01-01', '28,42 %', 'UK' ),
  ( 'a7683a3a-50c0-11eb-8be8-88e9fe6293fd', 'Kevin Crawford', 7243, DATE '2021-01-01', '22,75 %', 'Netherlands' ),
  ( 'a76844ee-50c0-11eb-8be8-88e9fe6293fd', 'Kimberly Green', 6589, DATE '2021-01-01', '11,92 %', 'US' ),
  ( 'a7684e62-50c0-11eb-8be8-88e9fe6293fd', 'William Fox', 1972, DATE '2021-01-01', '14,26 %', 'UK' ),
  ( 'a76856dc-50c0-11eb-8be8-88e9fe6293fd', 'Cynthia Gonzales', 3687, DATE '2021-01-01', '18,32 %', 'US' ),
  ( 'a7685f7e-50c0-11eb-8be8-88e9fe6293fd', 'Kim Brown', 1277, DATE '2021-01-01', '16,37 %', 'US' );

INSERT INTO DEMODATA VALUES
  ( 'a7686ae6-50c0-11eb-8be8-88e9fe6293fd', 'Christopher Trevino', 1442, DATE '2021-01-02', '25,13 %', 'US' ),
  ( 'a7687586-50c0-11eb-8be8-88e9fe6293fd', 'Timothy Mcdonald', 3363, DATE '2021-01-02', '22,57 %', 'UK' ),
  ( 'a7687d9c-50c0-11eb-8be8-88e9fe6293fd', 'Peter Dunn', 7870, DATE '2021-01-02', '31,30 %', 'US' ),
  ( 'a7688562-50c0-11eb-8be8-88e9fe6293fd', 'Joshua Thompson', 3553, DATE '2021-01-02', '13,39 %', 'US' ),
  ( 'a7688d1e-50c0-11eb-8be8-88e9fe6293fd', 'Thomas Roberts', 6874, DATE '2021-01-02', '14,45 %', 'UK' ),
  ( 'a76894d0-50c0-11eb-8be8-88e9fe6293fd', 'Daisy Montgomery', 3554, DATE '2021-01-02', '17,02 %', 'US' ),
  ( 'a7689cb4-50c0-11eb-8be8-88e9fe6293fd', 'Lucas Jennings', 2947, DATE '2021-01-02', '24,44 %', 'US' ),
  ( 'a768a466-50c0-11eb-8be8-88e9fe6293fd', 'Kathryn Leon', 2096, DATE '2021-01-02', '24,80 %', 'UK' );

INSERT INTO DEMODATA VALUES
  ( 'a768ad26-50c0-11eb-8be8-88e9fe6293fd', 'Denise Short', 5677, DATE '2021-01-03', '32,03 %', 'US' ),
  ( 'a768b532-50c0-11eb-8be8-88e9fe6293fd', 'Gregory Duffy', 7137, DATE '2021-01-03', '17,87 %', 'Spain' ),
  ( 'a768bcee-50c0-11eb-8be8-88e9fe6293fd', 'Douglas Fritz', 2828, DATE '2021-01-03', '27,01 %', 'US' ),
  ( 'a768c4a0-50c0-11eb-8be8-88e9fe6293fd', 'Eric Davis', 4258, DATE '2021-01-03', '14,86 %', 'US' ),
  ( 'a768cc48-50c0-11eb-8be8-88e9fe6293fd', 'Melissa Simmons', 3804, DATE '2021-01-03', '22,75 %', 'US' ),
  ( 'a768db8e-50c0-11eb-8be8-88e9fe6293fd', 'Carolyn Heath', 2101, DATE '2021-01-03', '30,09 %', 'US' ),
  ( 'a768e62e-50c0-11eb-8be8-88e9fe6293fd', 'Raymond Morales', 4584, DATE '2021-01-03', '20,11 %', 'US' ),
  ( 'a768ee58-50c0-11eb-8be8-88e9fe6293fd', 'Dr. Debra Johnson', 2132, DATE '2021-01-03', '18,30 %', 'US' ),
  ( 'a768f6d2-50c0-11eb-8be8-88e9fe6293fd', 'Victoria Frazier', 3091, DATE '2021-01-03', '13,69 %', 'US' );

INSERT INTO DEMODATA VALUES
  ( 'a768ffc4-50c0-11eb-8be8-88e9fe6293fd', 'Rodney Watson', 7433, DATE '2021-01-04', '34,34 %', 'Netherlands' ),
  ( 'a76907da-50c0-11eb-8be8-88e9fe6293fd', 'Scott Woodard', 6895, DATE '2021-01-04', '35,94 %', 'UK' ),
  ( 'a7691554-50c0-11eb-8be8-88e9fe6293fd', 'Andrew Cole', 5350, DATE '2021-01-04', '13,07 %', 'US' ),
  ( 'a76920a8-50c0-11eb-8be8-88e9fe6293fd', 'Michele Davis', 8618, DATE '2021-01-04', '36,74 %', 'US' ),
  ( 'a7692bca-50c0-11eb-8be8-88e9fe6293fd', 'Cassidy Benton', 1197, DATE '2021-01-04', '14,26 %', 'US' ),
  ( 'a7693462-50c0-11eb-8be8-88e9fe6293fd', 'Kimberly White', 2396, DATE '2021-01-04', '13,23 %', 'US' ),
  ( 'a7694a56-50c0-11eb-8be8-88e9fe6293fd', 'John Clark', 4059, DATE '2021-01-04', '28,87 %', 'Netherlands' ),
  ( 'a7695a8c-50c0-11eb-8be8-88e9fe6293fd', 'Paul Young', 4205, DATE '2021-01-04', '32,62 %', 'UK' );

INSERT INTO DEMODATA VALUES
  ( 'a76968e2-50c0-11eb-8be8-88e9fe6293fd', 'Stephanie Smith', 6207, DATE '2021-01-05', '32,59 %', 'Spain' ),
  ( 'a769763e-50c0-11eb-8be8-88e9fe6293fd', 'David Clay', 9427, DATE '2021-01-05', '36,46 %', 'US' ),
  ( 'a7698052-50c0-11eb-8be8-88e9fe6293fd', 'Jack Bryant', 9932, DATE '2021-01-05', '36,51 %', 'UK' ),
  ( 'a7698b9c-50c0-11eb-8be8-88e9fe6293fd', 'Peter Baker', 1531, DATE '2021-01-05', '30,85 %', 'US' ),
  ( 'a7699362-50c0-11eb-8be8-88e9fe6293fd', 'Cheyenne Brock', 5918, DATE '2021-01-05', '29,60 %', 'Spain' ),
  ( 'a7699ccc-50c0-11eb-8be8-88e9fe6293fd', 'Teresa Hogan', 7861, DATE '2021-01-05', '24,75 %', 'Spain' ),
  ( 'a769a604-50c0-11eb-8be8-88e9fe6293fd', 'Daniel Hanson', 7122, DATE '2021-01-05', '26,35 %', 'US' ),
  ( 'a769b27a-50c0-11eb-8be8-88e9fe6293fd', 'Jeffery Hinton', 6566, DATE '2021-01-05', '24,24 %', 'UK' ),
  ( 'a769ba2c-50c0-11eb-8be8-88e9fe6293fd', 'Michael Swanson', 9115, DATE '2021-01-05', '31,87 %', 'US' ),
  ( 'a769c1e8-50c0-11eb-8be8-88e9fe6293fd', 'Reginald Johns', 9231, DATE '2021-01-05', '20,29 %', 'UK' );

INSERT INTO DEMODATA VALUES
  ( 'a769cd00-50c0-11eb-8be8-88e9fe6293fd', 'David Olsen', 6086, DATE '2021-01-06', '39,43 %', 'US' ),
  ( 'a769d91c-50c0-11eb-8be8-88e9fe6293fd', 'Jade Lopez', 3542, DATE '2021-01-06', '26,49 %', 'UK' ),
  ( 'a769e61e-50c0-11eb-8be8-88e9fe6293fd', 'Jack Morgan', 9685, DATE '2021-01-06', '39,73 %', 'Spain' ),
  ( 'a769f3b6-50c0-11eb-8be8-88e9fe6293fd', 'Angela Kent', 6210, DATE '2021-01-06', '20,26 %', 'US' ),
  ( 'a769fdfc-50c0-11eb-8be8-88e9fe6293fd', 'Jenna Raymond', 4626, DATE '2021-01-06', '15,58 %', 'Spain' ),
  ( 'a76a08ec-50c0-11eb-8be8-88e9fe6293fd', 'Mary Rose', 6530, DATE '2021-01-06', '34,24 %', 'UK' ),
  ( 'a76a11ca-50c0-11eb-8be8-88e9fe6293fd', 'Jared Guerrero', 7198, DATE '2021-01-06', '26,03 %', 'US' ),
  ( 'a76a1aa8-50c0-11eb-8be8-88e9fe6293fd', 'Melvin Banks', 9152, DATE '2021-01-06', '26,83 %', 'US' ),
  ( 'a76a250c-50c0-11eb-8be8-88e9fe6293fd', 'Amy Davis', 7314, DATE '2021-01-06', '15,33 %', 'US' ),
  ( 'a76a2ee4-50c0-11eb-8be8-88e9fe6293fd', 'Justin Thompson', 9985, DATE '2021-01-06', '27,97 %', 'UK' ),
  ( 'a76a397a-50c0-11eb-8be8-88e9fe6293fd', 'Scott Jones', 1304, DATE '2021-01-06', '36,48 %', 'US' ),
  ( 'a76a42a8-50c0-11eb-8be8-88e9fe6293fd', 'John Armstrong', 6434, DATE '2021-01-06', '18,27 %', 'UK' );

INSERT INTO DEMODATA VALUES
  ( 'a76a4e10-50c0-11eb-8be8-88e9fe6293fd', 'Ashley Clayton', 7888, DATE '2021-01-07', '38,84 %', 'UK' ),
  ( 'a76a5950-50c0-11eb-8be8-88e9fe6293fd', 'Jessica Le', 5141, DATE '2021-01-07', '26,69 %', 'US' ),
  ( 'a76a65f8-50c0-11eb-8be8-88e9fe6293fd', 'Renee Campbell', 4145, DATE '2021-01-07', '36,63 %', 'UK' ),
  ( 'a76a700c-50c0-11eb-8be8-88e9fe6293fd', 'Colleen Young', 9664, DATE '2021-01-07', '26,19 %', 'US' ),
  ( 'a76a7a2a-50c0-11eb-8be8-88e9fe6293fd', 'Elizabeth Clark', 6041, DATE '2021-01-07', '26,63 %', 'Spain' ),
  ( 'a76a8240-50c0-11eb-8be8-88e9fe6293fd', 'Jennifer Martinez', 6383, DATE '2021-01-07', '23,29 %', 'US' ),
  ( 'a76a8b50-50c0-11eb-8be8-88e9fe6293fd', 'Thomas Russell', 7563, DATE '2021-01-07', '32,47 %', 'UK' ),
  ( 'a76a9366-50c0-11eb-8be8-88e9fe6293fd', 'Alexander Lara', 1771, DATE '2021-01-07', '21,67 %', 'US' ),
  ( 'a76a9f00-50c0-11eb-8be8-88e9fe6293fd', 'John Terry', 4884, DATE '2021-01-07', '19,71 %', 'UK' ),
  ( 'a76aaafe-50c0-11eb-8be8-88e9fe6293fd', 'Stephen Green', 6059, DATE '2021-01-07', '21,94 %', 'UK' ),
  ( 'a76ab7a6-50c0-11eb-8be8-88e9fe6293fd', 'Brenda Johnson', 5906, DATE '2021-01-07', '21,77 %', 'US' ),
  ( 'a76ac2a0-50c0-11eb-8be8-88e9fe6293fd', 'Monica Keller', 2340, DATE '2021-01-07', '35,57 %', 'UK' );
