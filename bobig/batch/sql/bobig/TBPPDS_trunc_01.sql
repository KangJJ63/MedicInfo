/* ALTER TABLE HMBPUSER.TBPPDS2018 TRUNCATE SUBPARTITION TBPPDS2018_00001_A0001_K0001_BFC */
ALTER TABLE TBPPDS{years} TRUNCATE PARTITION TBPPDS{years}_{exec_seq};