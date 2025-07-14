CREATE TABLE Target_Table_1 ( -- Renamed from source_table1
    id_col1 INT64 NOT NULL, -- Not renamed
    Target_Name_Col_1 STRING(255), -- Renamed from name_col1
    data_col1 STRING(MAX) -- Not renamed
) PRIMARY KEY (id_col1);

CREATE TABLE source_table2 ( -- This table is not renamed
    key_col2 STRING(50) NOT NULL, -- Not renamed
    Target_Category_Col_2 STRING(100), -- Renamed from category_col2
    value_col2 STRING(MAX) -- Not renamed
) PRIMARY KEY (key_col2);
