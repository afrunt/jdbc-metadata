-- noinspection SqlNoDataSourceInspectionForFile

CREATE SCHEMA TEST;

CREATE TABLE TEST.EMPLOYEE (
  EMPLOYEE_ID   BIGINT AUTO_INCREMENT,
  FIRST_NAME    VARCHAR(100) NOT NULL,
  MIDDLE_NAME   VARCHAR(100),
  LAST_NAME     VARCHAR(100) NOT NULL,
  DATE_OF_BIRTH DATE         NOT NULL,
  DEPARTMENT_ID BIGINT       NOT NULL,
  POSITION_ID   BIGINT       NOT NULL,
  MODIFIED_DATE TIMESTAMP DEFAULT NOW() NOT NULL,
  PHOTO BLOB,
  PRIMARY KEY (EMPLOYEE_ID)
);

CREATE TABLE TEST.DEPARTMENT (
  DEPARTMENT_ID BIGINT AUTO_INCREMENT,
  NAME          VARCHAR(200) NOT NULL,
  PRIMARY KEY (DEPARTMENT_ID)
);

-- this table is in public schema
CREATE TABLE POSITION (
  POSITION_ID BIGINT AUTO_INCREMENT,
  TITLE       VARCHAR(100) NOT NULL,
  PRIMARY KEY (POSITION_ID)
);

-- this table is in public schema
CREATE TABLE SKILL (
  SKILL_ID BIGINT AUTO_INCREMENT,
  TITLE       VARCHAR(100) NOT NULL,
  PRIMARY KEY (SKILL_ID)
);

CREATE TABLE EMPLOYEE_SKILL (
  EMPLOYEE_ID BIGINT,
  SKILL_ID    BIGINT,
  PRIMARY KEY (EMPLOYEE_ID, SKILL_ID)
);




