select l.l_orderkey,
       sum( l.l_extendedprice * (1 - l.l_discount )) as revenue,
       o.o_orderdate,
       o.o_shippriority
from customer as c,
     orders as o,
     lineitem as l
where 1 = 1
  and c.c_custkey = o.o_custkey
  and l.l_orderkey = o.o_orderkey
  and c.c_mktsegment = 'BUILDING'
  and o.o_orderdate < date('1995-03-15')
  and l.l_shipdate > date('1995-03-15')
group by l_orderkey,
         o_orderdate,
         o_shippriority
order by revenue desc,
         o_orderdate limit 10