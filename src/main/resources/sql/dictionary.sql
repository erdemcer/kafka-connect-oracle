WITH dcc AS
  (SELECT dcc.owner,
    dcc.table_name,
    dcc2.column_name,
    1 PK_COLUMN
  FROM dba_constraints dcc,
    dba_cons_columns dcc2
  WHERE dcc.owner        =dcc2.owner
  AND dcc.table_name     =dcc2.table_name
  AND dcc.constraint_name=dcc2.constraint_name
  AND dcc.constraint_type='P'
  ),
  duq AS
  (SELECT di2.TABLE_OWNER,
    di2.TABLE_NAME,
    di2.COLUMN_NAME ,
    1 UQ_COLUMN
  FROM dba_ind_columns di2
  JOIN dba_indexes di
  ON di.table_owner=di2.TABLE_OWNER
  AND di.table_name=di2.TABLE_NAME
  AND di.uniqueness='UNIQUE'
  AND di.owner     =di2.INDEX_OWNER
  AND di.index_name=di2.INDEX_NAME
  GROUP BY di2.TABLE_OWNER,
    di2.TABLE_NAME,
    di2.COLUMN_NAME
  )
SELECT dc.owner,
  dc.TABLE_NAME,
  dc.COLUMN_NAME,
  dc.NULLABLE,
  dc.DATA_TYPE,
  NVL(dc.DATA_PRECISION,dc.DATA_LENGTH) DATA_LENGTH,
  NVL(dc.DATA_SCALE,0) DATA_SCALE,
  NVL(dc.DATA_PRECISION,0) DATA_PRECISION,
  NVL(x.pk_column,0) pk_column,
  NVL(y.uq_column,0) uq_column
FROM dba_tab_cols dc
LEFT OUTER JOIN dcc x
ON x.owner        =dc.owner
AND x.table_name  =dc.TABLE_NAME
AND dc.COLUMN_NAME=x.column_name
LEFT OUTER JOIN duq y
ON y.table_owner     =dc.owner
AND y.table_name     =dc.TABLE_NAME
AND y.column_name    =dc.COLUMN_NAME
WHERE dC.Owner       =:owner
AND dc.TABLE_NAME    =:tableName
AND dc.HIDDEN_COLUMN ='NO'
AND dc.VIRTUAL_COLUMN='NO'
ORDER BY dc.TABLE_NAME,
  dc.COLUMN_ID