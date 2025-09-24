CREATE TABLE lineitem (
      l_orderkey      INTEGER NOT NULL,
      l_partkey       INTEGER NOT NULL,
      l_suppkey       INTEGER NOT NULL,
      l_linenumber    INTEGER NOT NULL,
      l_quantity      REAL NOT NULL,
      l_extendedprice REAL NOT NULL,
      l_discount      REAL NOT NULL,
      l_tax           REAL NOT NULL,
      l_returnflag    TEXT NOT NULL,
      l_linestatus    TEXT NOT NULL,
      l_shipdate      TEXT NOT NULL,
      l_commitdate    TEXT NOT NULL,
      l_receiptdate   TEXT NOT NULL,
      l_shipinstruct  TEXT NOT NULL,
      l_shipmode      TEXT NOT NULL,
      l_comment       TEXT NOT NULL
);
