library(RSQLite)
#Connect to the Source System
db <- dbConnect(SQLite(), dbname="source.db")

#Extract Today's Shippers Data
shippers_extract = dbGetQuery( db,'SELECT * FROM Shippers' )

#Close Source Connection
dbDisconnect(db)

#Connect to the DW System
dwcon <- dbConnect(SQLite(), dbname="datawarehouse.db")

#Insert into S
deletesshippers = dbGetQuery(dwcon,"DELETE FROM S_Shippers" )
dbWriteTable(conn = dwcon, name = "S_Shippers", 
             value =shippers_extract, row.names = FALSE, append = TRUE)
dbGetQuery(dwcon,'SELECT * FROM S_Shippers' )

#Get New and Changed Data
s_table_new_data <- dbGetQuery(dwcon, "SELECT * FROM S_Shippers 
                    WHERE ShipperID NOT IN (SELECT ShipperID FROM M_Shippers)")
s_table_changed_company_name <-dbGetQuery(dwcon, "SELECT s.ShipperID, s.CompanyName,
                    s.Phone FROM S_Shippers s INNER JOIN 
                    M_Shippers m ON s.ShipperID = m.ShipperID
                    WHERE NOT s.CompanyName = m.CompanyName")
s_table_changed_phone_number <-dbGetQuery(dwcon, "SELECT s.ShipperID, s.CompanyName,
                    s.Phone FROM S_Shippers s INNER JOIN 
                    M_Shippers m ON s.ShipperID = m.ShipperID
                    WHERE NOT s.Phone = m.Phone")
s_table_changed_data <- rbind(s_table_changed_company_name,s_table_changed_phone_number)
s_table_extract <- rbind(s_table_new_data,s_table_changed_data)

#Insert into X
deletequery=dbGetQuery(dwcon, "DELETE FROM X_Shippers")
dbWriteTable(conn = dwcon, name = "X_Shippers", value =s_table_extract, row.names = FALSE, append = TRUE)
dbGetQuery(dwcon,'SELECT * FROM X_Shippers' )

#Clean X
#Select Companies with Null Names
x_table_no_companyname = dbGetQuery(dwcon, "SELECT * FROM X_Shippers 
                                  WHERE CompanyName =''") 
x_table_no_companyname$ErrorType = "No Company Name"

#Select Duplicate Companies
x_table_duplicate_companies= dbGetQuery(dwcon, "SELECT * FROM x_shippers 
                                WHERE CompanyName IN
                                (SELECT CompanyName FROM S_Shippers
                                GROUP BY CompanyName HAVING COUNT(CompanyName) > 1)") 
x_table_duplicate_companies$ErrorType = "Duplicate Company Name"
x_table_errors = rbind(x_table_duplicate_companies,x_table_no_companyname)

#Set Unknown to Missing Phone Number
updatequery =dbGetQuery(dwcon, "UPDATE X_Shippers SET Phone='Unknown Phone Number' 
                      WHERE Phone =''") 

#Insert into E
deletequery=dbGetQuery(dwcon, "DELETE FROM E_Shippers")
dbWriteTable(conn = dwcon, name = "E_Shippers", value =x_table_errors, row.names = FALSE, append = TRUE)
dbGetQuery(dwcon,'SELECT * FROM E_Shippers' )

#Select Clean Data
x_table_clean_data = dbGetQuery(dwcon, "SELECT * FROM X_Shippers 
                              WHERE ShipperID NOT IN (SELECT ShipperID FROM E_Shippers)") 

#Insert into C
query=dbGetQuery(dwcon, "DELETE FROM C_Shippers")
dbWriteTable(conn = dwcon, name = "C_Shippers", value =x_table_clean_data, row.names = FALSE, append = TRUE)
dbGetQuery(dwcon, "SELECT * FROM C_Shippers")

#Update M Table
#Select All New From C
c_table_new_data = dbGetQuery(dwcon, "SELECT * from C_Shippers c 
                            WHERE c.ShipperID NOT IN (SELECT m.ShipperID FROM M_shippers m )")
dbWriteTable(conn = dwcon, name = "M_Shippers_Test", value =c_table_new_data, row.names = FALSE, append = TRUE)
#Select All Changed From C
c_table_changed_data = dbGetQuery(dwcon, "SELECT c.* FROM C_Shippers c, M_Shippers m 
                                WHERE c.ShipperID = m.ShipperID and 
                                (c.CompanyName <> m.CompanyName or c.Phone <> m.Phone)")
deletequery = dbGetQuery(dwcon, "DELETE FROM M_Shippers_Test 
                              WHERE ShipperID IN (SELECT m.ShipperID FROM C_Shippers c, M_Shippers m 
                                WHERE c.ShipperID = m.ShipperID and 
                                (c.CompanyName <> m.CompanyName or c.Phone <> m.Phone))") 
dbWriteTable(conn = dwcon, name = "M_Shippers_Test", value =c_table_changed_data, row.names = FALSE, append = TRUE)
dbGetQuery(dwcon, "SELECT * FROM M_Shippers_Test")

#Select From C and Transform to DW Format
c_table_data = dbGetQuery(dwcon, "SELECT ShipperID as [Shipper_ID], 
                        CompanyName as [Shipper_Name], Phone as [Current_Shipper_Phone], 
                        DATE() as [Effective_Date] FROM C_Shippers" )
c_table_data$Previous_Shipper_Phone = "Previous_Shipper_Phone"
c_table_data = c_table_data[,c("Shipper_ID", "Shipper_Name", "Current_Shipper_Phone","Previous_Shipper_Phone", "Effective_Date"  ) ]
#Insert into T
query=dbGetQuery(dwcon, "DELETE FROM T_Shipper")
dbWriteTable(conn = dwcon, name = "T_Shipper", value =c_table_data, row.names = FALSE, append = TRUE)
dbGetQuery(dwcon, "SELECT * FROM T_Shipper")

#Select New from T
t_table_new_data = dbGetQuery(dwcon, "SELECT t.*
                   FROM t_shipper t LEFT JOIN d_shipper d ON t.Shipper_ID = d.Shipper_ID
                  WHERE d.Shipper_ID IS NULL")
t_table_new_data$Current_Row_Ind = 'Y'

#Insert New into I
query=dbGetQuery(dwcon, "DELETE FROM I_Shipper")
dbWriteTable(conn = dwcon, name = "I_Shipper", value =t_table_new_data, row.names = FALSE, append = TRUE)
dbGetQuery(dwcon, "SELECT * FROM I_Shipper")

#Select Changed from T
t_table_changed_data=dbGetQuery(dwcon, "SELECT t.*
                   FROM t_shipper t inner join d_shipper d
                   ON t.Shipper_ID = d.Shipper_ID
                   WHERE (NOT t.Shipper_Name = d.Shipper_Name or NOT 
                   t.Current_Shipper_Phone = d.Current_Shipper_Phone) 
                   AND d.Current_Row_Ind = 'Y'")
t_table_changed_data$Current_Row_Ind = 'Y'

#Insert into U
deletequery=dbGetQuery(dwcon, "DELETE FROM U_Shipper")
dbWriteTable(conn = dwcon, name = "U_Shipper", value =t_table_changed_data, row.names = FALSE, append = TRUE)
dbGetQuery(dwcon, "SELECT * FROM U_Shipper")

#Get Max Whse Key
maxkey <- dbGetQuery(dwcon, "SELECT MAX(Shipper_Key) as MAX FROM D_Shipper")
i_table_data <- dbGetQuery(dwcon, "SELECT * FROM I_Shipper")
i_table_data$Shipper_Key = (maxkey[1,1]+1):(maxkey[1,1]+nrow(i_table_data))

#Insert New into D
i_table_data = i_table_data[,c("Shipper_Key","Shipper_ID","Shipper_Name", "Current_Shipper_Phone", "Previous_Shipper_Phone", "Effective_Date", "Current_Row_Ind" )]
dbWriteTable(conn = dwcon, name = "D_Shipper", value =i_table_data, row.names = FALSE, append = TRUE)
dbGetQuery(dwcon, "SELECT * FROM D_Shipper")

#Select Type 3 from U and D
u_table_type3_data <- dbGetQuery(dwcon, "SELECT u.*
                      from u_shipper u inner join d_shipper d
                      on u.Shipper_ID = d.Shipper_ID
                      where NOT (u.Current_Shipper_Phone = d.Current_Shipper_Phone)
                      and d.Current_Row_Ind = 'Y'")

d_table_type3_data <- dbGetQuery(dwcon, "SELECT d.*
                      from u_shipper u inner join d_shipper d
                      on u.Shipper_ID = d.Shipper_ID
                      where NOT (u.Current_Shipper_Phone = d.Current_Shipper_Phone)
                      and d.Current_Row_Ind = 'Y'")

u_table_type3_data$Previous_Shipper_Phone = d_table_type3_data$Current_Shipper_Phone
u_table_type3_data$Shipper_Key = d_table_type3_data$Shipper_Key
u_table_type3_data = u_table_type3_data[,c("Shipper_Key","Shipper_ID","Shipper_Name", "Current_Shipper_Phone", "Previous_Shipper_Phone", "Effective_Date", "Current_Row_Ind" )]

deletequery = dbGetQuery(dwcon, "DELETE FROM d_shipper 
                              WHERE Shipper_ID IN (SELECT d.Shipper_ID
                      from u_shipper u inner join d_shipper d
                      on u.Shipper_ID = d.Shipper_ID
                      where NOT (u.Current_Shipper_Phone = d.Current_Shipper_Phone)
                      and d.Current_Row_Ind = 'Y')") 
dbWriteTable(conn = dwcon, name = "d_shipper", value =u_table_type3_data, row.names = FALSE, append = TRUE)
dbGetQuery(dwcon, "SELECT * FROM d_shipper")

#Select Type 2
maxkey <- dbGetQuery(dwcon, "SELECT MAX(Shipper_Key) as MAX FROM D_Shipper")
u_table_type2_data <- dbGetQuery(dwcon, "SELECT u.*
                   FROM u_shipper u INNER JOIN d_shipper d
                   ON u.Shipper_ID = d.Shipper_ID
                   WHERE NOT (u.Shipper_Name = d.Shipper_Name) 
                               AND d.Current_Row_Ind = 'Y'")
u_table_type2_data$Shipper_Key = (maxkey[1,1]+1):(maxkey[1,1]+nrow(u_table_type2_data))

#Insert New into D
u_table_type2_data =u_table_type2_data[,c("Shipper_Key","Shipper_ID","Shipper_Name", "Current_Shipper_Phone", "Previous_Shipper_Phone", "Effective_Date", "Current_Row_Ind" )]

dbWriteTable(conn = dwcon, name = "d_shipper", value =u_table_type2_data, row.names = FALSE, append = TRUE)
dbGetQuery(dwcon, "SELECT * FROM D_Shipper")

#Update Shipper Current Value
forupdatedata <- dbGetQuery(dwcon, "SELECT d.*
                            FROM u_shipper u INNER JOIN d_shipper d ON
                            u.Shipper_Id = d.Shipper_Id 
                            WHERE d.Current_Row_Ind = 'Y' AND Not u.Shipper_Name = d.Shipper_Name ")
forupdatedata$Current_Row_Ind = 'N'


deletequery = dbGetQuery(dwcon, "DELETE FROM d_shipper 
                              WHERE Shipper_Key IN (SELECT d.Shipper_Key
                            FROM u_shipper u INNER JOIN d_shipper d ON
                            u.Shipper_Id = d.Shipper_Id 
                            WHERE d.Current_Row_Ind = 'Y' AND Not u.Shipper_Name = d.Shipper_Name)") 

dbWriteTable(conn = dwcon, name = "d_shipper", value =forupdatedata, row.names = FALSE, append = TRUE)
dbGetQuery(dwcon, "SELECT * FROM D_Shipper")
dbDisconnect(dwcon)
