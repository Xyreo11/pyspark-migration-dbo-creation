# One part - public -> Oracle → PySpark Migration (Microsoft Fabric)

Migration of Oracle PL/SQL warehouse procedures to PySpark notebooks running on
Microsoft Fabric, writing to Delta tables in the Gold lakehouse.


## Status at a glance
29 / 29 procedures migrated. 3 blocked on missing target-table DDL, 100s of tables created

## Purpose
This repo creates tables on the go from any oracle ddl syntax input.
