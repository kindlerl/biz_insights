mv sql/silver/silver_tables.sql                            athena/ddl/silver_tables_all.sql
mv sql/gold/g*_create_external_table*.sql                  athena/ddl/
mv sql/gold/g*_sanity_check.sql                            athena/ddl/
mv sql/gold/GlueCatalog_Repair_Notes.txt                   docs/
mv sql/gold/partition_projection.sql                       docs/
