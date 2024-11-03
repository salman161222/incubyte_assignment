CREATE TABLE curate.customer_Info (
    Customer_Name VARCHAR(255) NOT NULL,
    Customer_ID VARCHAR(18) NOT NULL,
    Customer_Open_Date DATE NOT NULL,
    Last_Consulted_Date DATE,
    Vaccination_Type CHAR(5),
    Doctor_Consulted CHAR(255),
    State CHAR(5),
    Country CHAR(5),
    Post_Code INT,
    Date_of_Birth DATE,
    Active_Customer CHAR(1),
    Age INT,
    Days_since_last_consulted INT,
    PRIMARY KEY (Customer_ID, Country)


) PARTITION BY LIST (Country);
