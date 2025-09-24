CREATE TABLE orders (
    o_orderkey      INTEGER NOT NULL,
    o_custkey       INTEGER NOT NULL,
    o_orderstatus   TEXT NOT NULL,
    o_totalprice    REAL NOT NULL,
    o_orderdate     TEXT NOT NULL,
    o_orderpriority TEXT NOT NULL,
    o_clerk         TEXT NOT NULL,
    o_shippriority  INTEGER NOT NULL,
    o_comment       TEXT NOT NULL
);
