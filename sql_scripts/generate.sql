DROP TABLE IF EXISTS dim.Time;
CREATE TABLE dim.Time
(
    TimeKey INT  NOT NULL PRIMARY KEY,
    Time    TIME NULL,
    Hour    INT  NULL,
    Minute  INT  NULL
);
DROP TABLE IF EXISTS dim.Date;
CREATE TABLE dim.Date
(
    DateKey INT  NOT NULL PRIMARY KEY,
    Date    DATE NULL,
    Year    INT  NULL,
    Month   INT  NULL,
    Day     INT  NULL
);



DROP TABLE IF EXISTS dim.MedPersonal;
CREATE TABLE dim.MedPersonal
(
    MedID    smallint    NOT NULL PRIMARY KEY,
    Name     VARCHAR(50) NULL,
    position VARCHAR(50) NULL
);
DROP TABLE IF EXISTS dim.Car;
CREATE TABLE dim.Car
(
    CarID INT         NOT NULL PRIMARY KEY,
    Type  VARCHAR(50) NULL
);
DROP TABLE IF EXISTS dim.Patient;
CREATE TABLE dim.Patient
(
    ID_P              INT IDENTITY (1, 1) NOT NULL PRIMARY KEY,
    PatientCode       INT                 NULL,
    PatientFirstName  VARCHAR(50)         NULL,
    PatientLastName   VARCHAR(50)         NULL,
    Address           VARCHAR(50)         NULL,
    Birthday          date                NULL,
    Description       VARCHAR(MAX)        NULL,
    InsuranseType     VARCHAR(30)         NULL,
    StartDate         timestamp           NULL,
    EndDate           timestamp           NULL default null,
    IsCurrent         boolean             NULL default true
);

CREATE TABLE dim.Brigade
(
    ID_B                    INT IDENTITY (1, 1) NOT NULL PRIMARY KEY,
    BrigadeCode             INT                 NULL,
    paramedic1_ID           smallint            NULL,
    paramedic2_ID           smallint            NULL,
    nurse_ID                smallint            NULL,
    ambulanc_driver_name_ID smallint            NULL,
    doctor_ID               smallint            NULL,
    CONSTRAINT FK_CarI FOREIGN KEY (BrigadeCode) REFERENCES dim.Car (CarID),
    CONSTRAINT FK_ID_B FOREIGN KEY (paramedic1_ID) REFERENCES dim.MedPersonal (MedID),
    CONSTRAINT FK_ID_B1 FOREIGN KEY (paramedic2_ID) REFERENCES dim.MedPersonal (MedID),
    CONSTRAINT FK_ID_CD FOREIGN KEY (ambulanc_driver_name_ID) REFERENCES dim.MedPersonal (MedID),
    CONSTRAINT FK_ID_D FOREIGN KEY (doctor_ID) REFERENCES dim.MedPersonal (MedID),
    CONSTRAINT FK_ID_N FOREIGN KEY (nurse_ID) REFERENCES dim.MedPersonal (MedID)

);
---------------------------------------------------------------fac_table_________________________
CREATE TABLE fac.Emergency
(
    DateKey      INT           NOT NULL,
    TimeKey      INT           NOT NULL,
    PatientKey   INT           NOT NULL,
    BrigadeKey   INT           NOT NULL,
    Hospitalized boolean       NULL,
    CallPrice    INT           NULL,
    Discount     NUMERIC(5, 2) NULL,
    CONSTRAINT FK_ID_Brigade FOREIGN KEY (BrigadeKey) REFERENCES dim.Brigade (ID_B),
    CONSTRAINT FK_ID_Date FOREIGN KEY (DateKey) REFERENCES dim.Date (DateKey),
    CONSTRAINT FK_ID_patient FOREIGN KEY (PatientKey) REFERENCES dim.Patient (ID_P),
    CONSTRAINT FK_ID_Time FOREIGN KEY (TimeKey) REFERENCES dim.Time (TimeKey)
);

CREATE OR REPLACE PROCEDURE dim.LoadIntoBr()
AS
$$
BEGIN
    INSERT INTO dim.Brigade(BrigadeCode, paramedic1_ID, paramedic2_ID, nurse_ID, ambulanc_driver_name_ID, doctor_ID)
    SELECT brigade_code, paramedic_code, paramedic2_code, nurse_code, driver_code, doctor_code
    FROM public.call;
END;
$$
    LANGUAGE plpgsql;

CALL dim.LoadIntoBr();
CREATE OR REPLACE PROCEDURE dim.LoadIntoCar()
AS
$$
BEGIN
    DROP TABLE IF EXISTS target;
    CREATE TEMP TABLE target
    (
        like public.call
    );
    INSERT INTO target
    select *
    from public.call;
    DELETE
    FROM target
        USING dim.Car
    WHERE dim.car.carid = target.brigade_code
      AND dim.Car.type = target.types;
    INSERT
    INTO dim.Car (carid, type)
    WITH CTE AS (
        SELECT brigade_code,
               types,
               MAX(CAST(arrive_date as DATE) + CAST(arrive_time as TIME)) dt
        FROM target
        GROUP BY brigade_code, types
    )
    SELECT CTE.brigade_code, CTE.types
    FROM CTE
             INNER JOIN
         (SELECT brigade_code, MAX(dt) dt
          FROm CTE
          GROUP BY brigade_code) ext
         ON CTE.brigade_code = ext.brigade_code
             AND CTE.dt = ext.dt;
END;
$$
    LANGUAGE plpgsql;

CALL dim.LoadIntoCar();

INSERT INTO dim.MedPersonal(MedID, Name, position)
SELECT S.ID, S.Name, S.Type
FROM (
         SELECT T.par       as ID,
                T.par_c     as Name,
                'Paramedic' as Type
         FROM (
                  (
                      SELECT DISTINCT paramedic_code par,
                                      paramedic      par_c
                      FROM call
                      WHERE call.paramedic IS NOT NULL
                  )
                  UNION
                  (
                      SELECT DISTINCT paramedic2_code par,
                                      paramedic2      par_c
                      FROM call
                      WHERE call.paramedic2_code IS NOT NULL
                  )
              ) T
         UNION
         SELECT nurse_code ID,
                nurse      Name,
                'Nurse' as Type
         FROM Call
         WHERE nurse_code IS NOT NULL
         UNION
         SELECT driver_code            ID,
                driver                 Name,
                'CarDriver-Sanitar' as Type
         FROM Call
         WHERE driver_code IS NOT NULL
         UNION
         SELECT doctor_code ID,
                doctor      Name,
                'Doctor' as Type
         FROM Call
         WHERE doctor_code IS NOT NULL) S



CREATE OR REPLACE PROCEDURE dim.LoadIntoPatient()
AS
$$
BEGIN
    DROP TABLE IF EXISTS target1;
    CREATE TEMP TABLE target1
    (
        like public.call
    );
    INSERT INTO target1
    SELECT callid,
           firstname,
           lastname,
           address,
           birthday,
           description,
           arrive_date,
           arrive_time,
           patient_code,
           insurance_types,
           id_hospital,
           brigade_code,
           paramedic,
           paramedic_code,
           paramedic2,
           paramedic2_code,
           nurse,
           nurse_code,
           driver,
           driver_code,
           doctor,
           doctor_code,
           types
    FROM (SELECT call.*,
                 ROW_NUMBER() OVER (PARTITION BY patient_code ORDER BY arrive_date) AS RowNumber
          FROM public.call
         ) AS a
    WHERE a.RowNumber = 1;
    DELETE
    FROM target1
        USING (SELECT *
               FROM dim.patient
               WHERE dim.patient.iscurrent = false) p
    WHERE p.patientcode = target1.patient_code
      AND p.insuransetype = target1.insurance_types;
    UPDATE dim.patient
    SET enddate = current_date - 1,
        iscurrent = false
    WHERE patient.enddate IS NULL
      AND patientcode IN (
        SELECT dim.patient.patientcode
        FROM dim.patient
                 INNER JOIN target1
                            ON dim.patient.patientcode = target1.patient_code
        WHERE dim.patient.enddate IS NULL);
    DELETE
    FROM target1
        USING (SELECT dim.patient.patientcode
               FROM dim.patient) d_product
    WHERE target1.patient_code = d_product.patientcode;
    INSERT
    INTO dim.patient
    (patientcode,
     patientfirstname,
     patientlastname,
     address,
     birthday,
     description,
     insuransetype,
     startdate)
    SELECT patient_code,
           firstname,
           lastname,
           address,
           birthday,
           description,
           insurance_types,
           current_date
    FROM target1;
END;
$$
    LANGUAGE plpgsql;

CALL dim.LoadIntoPatient();



