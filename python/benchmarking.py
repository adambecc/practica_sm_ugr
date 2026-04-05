import psycopg2
import time
import statistics

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="farmacity_dw",
    user="postgres",
    password="1234"
)
cur = conn.cursor()

q1_oltp = """
    SELECT c.nombre,
           EXTRACT(QUARTER FROM p.fecha_pedido) AS trimestre,
           SUM(d.cantidad * d.precio_unitario)
    FROM oltp_ventas.detalle_pedido d
    JOIN oltp_ventas.pedidos p      ON d.pedido_id   = p.pedido_id
    JOIN oltp_ventas.productos pr   ON d.producto_id = pr.producto_id
    JOIN oltp_ventas.categorias c   ON pr.categoria_id = c.categoria_id
    GROUP BY c.nombre, trimestre
"""

q2_oltp = """
    SELECT p.tienda_id,
           EXTRACT(MONTH FROM p.fecha_pedido) AS mes,
           SUM(d.cantidad * d.precio_unitario)
    FROM oltp_ventas.pedidos p
    JOIN oltp_ventas.detalle_pedido d ON p.pedido_id = d.pedido_id
    GROUP BY p.tienda_id, mes
"""

q3_oltp = """
    SELECT pr.nombre,
           SUM(d.cantidad * (d.precio_unitario - cp.coste_unitario)) AS margen
    FROM oltp_ventas.detalle_pedido d
    JOIN oltp_ventas.productos pr          ON d.producto_id = pr.producto_id
    JOIN oltp_inventario.costes_producto cp ON d.producto_id = cp.producto_id
    GROUP BY pr.nombre
    ORDER BY margen DESC
    LIMIT 10
"""

q4_oltp = """
    SELECT t.nombre,
           AVG(e.coste_envio),
           AVG(e.fecha_entrega - e.fecha_envio)
    FROM oltp_logistica.envios e
    JOIN oltp_logistica.transportistas t ON e.transportista_id = t.transportista_id
    GROUP BY t.nombre
"""

q5_oltp = """
    SELECT em.nombre,
           p.tienda_id,
           SUM(d.cantidad * d.precio_unitario)
    FROM oltp_ventas.pedidos p
    JOIN oltp_ventas.detalle_pedido d  ON p.pedido_id  = d.pedido_id
    JOIN oltp_ventas.empleados_ref em  ON p.empleado_id = em.empleado_id
    GROUP BY em.nombre, p.tienda_id
"""

q1_star = """
    SELECT p.categoria, t.trimestre, SUM(f.ingresos)
    FROM olap_star.fact_ventas f
    JOIN olap_star.dim_tiempo   t ON f.tiempo_sk   = t.tiempo_sk
    JOIN olap_star.dim_producto p ON f.producto_sk = p.producto_sk
    GROUP BY p.categoria, t.trimestre
"""

q2_star = """
    SELECT ti.nombre, t.anio, t.mes, SUM(f.ingresos)
    FROM olap_star.fact_ventas f
    JOIN olap_star.dim_tiempo  t  ON f.tiempo_sk  = t.tiempo_sk
    JOIN olap_star.dim_tienda  ti ON f.tienda_sk  = ti.tienda_sk
    GROUP BY ti.nombre, t.anio, t.mes
"""

q3_star = """
    SELECT p.nombre, SUM(f.margen) AS margen_total
    FROM olap_star.fact_ventas f
    JOIN olap_star.dim_producto p ON f.producto_sk = p.producto_sk
    GROUP BY p.nombre
    ORDER BY margen_total DESC
    LIMIT 10
"""

q4_star = """
    SELECT tr.nombre, AVG(fe.coste_envio), AVG(fe.dias_entrega)
    FROM olap_star.fact_envios fe
    JOIN olap_star.dim_transportista tr ON fe.transportista_sk = tr.transportista_sk
    GROUP BY tr.nombre
"""

q5_star = """
    SELECT e.nombre, ti.nombre, t.temporada, SUM(f.ingresos)
    FROM olap_star.fact_ventas f
    JOIN olap_star.dim_empleado e  ON f.empleado_sk = e.empleado_sk
    JOIN olap_star.dim_tienda   ti ON f.tienda_sk   = ti.tienda_sk
    JOIN olap_star.dim_tiempo   t  ON f.tiempo_sk   = t.tiempo_sk
    GROUP BY e.nombre, ti.nombre, t.temporada
"""

q1_snow = """
    SELECT c.nombre, t.trimestre, SUM(f.ingresos)
    FROM olap_snow.fact_ventas f
    JOIN olap_star.dim_tiempo    t ON f.tiempo_sk   = t.tiempo_sk
    JOIN olap_snow.dim_producto  p ON f.producto_sk = p.producto_sk
    JOIN olap_snow.dim_categoria c ON p.categoria_sk = c.categoria_sk
    GROUP BY c.nombre, t.trimestre
"""

q2_snow = """
    SELECT ti.nombre, t.anio, t.mes, SUM(f.ingresos)
    FROM olap_snow.fact_ventas f
    JOIN olap_star.dim_tiempo  t  ON f.tiempo_sk = t.tiempo_sk
    JOIN olap_star.dim_tienda  ti ON f.tienda_sk = ti.tienda_sk
    GROUP BY ti.nombre, t.anio, t.mes
"""

q3_snow = """
    SELECT p.nombre, SUM(f.margen) AS margen_total
    FROM olap_snow.fact_ventas f
    JOIN olap_snow.dim_producto p ON f.producto_sk = p.producto_sk
    GROUP BY p.nombre
    ORDER BY margen_total DESC
    LIMIT 10
"""

q4_snow = """
    SELECT tr.nombre, AVG(fe.coste_envio), AVG(fe.dias_entrega)
    FROM olap_star.fact_envios fe
    JOIN olap_star.dim_transportista tr ON fe.transportista_sk = tr.transportista_sk
    GROUP BY tr.nombre
"""

q5_snow = """
    SELECT e.nombre, ti.nombre, t.temporada, SUM(f.ingresos)
    FROM olap_snow.fact_ventas f
    JOIN olap_star.dim_empleado e  ON f.empleado_sk = e.empleado_sk
    JOIN olap_star.dim_tienda   ti ON f.tienda_sk   = ti.tienda_sk
    JOIN olap_star.dim_tiempo   t  ON f.tiempo_sk   = t.tiempo_sk
    GROUP BY e.nombre, ti.nombre, t.temporada
"""


def medir(query, repeticiones=20):
    tiempos = []
    for _ in range(repeticiones):
        inicio = time.time()
        cur.execute(query)
        cur.fetchall()
        tiempos.append(time.time() - inicio)
    return round(statistics.mean(tiempos) * 1000, 3)


def mejora(oltp, star):
    if oltp and star:
        return f"{round((oltp - star) / oltp * 100)}%"
    return "n/a"


def diferencia(star, snow):
    if star and snow:
        return f"{round((snow - star) / snow * 100)}%"
    return "n/a"


nombres = [
    "Q1 ingresos/trimestre",
    "Q2 drill-down tienda",
    "Q3 top10 productos",
    "Q4 logistica",
    "Q5 empleados"
]

oltp_q = [q1_oltp, q2_oltp, q3_oltp, q4_oltp, q5_oltp]
star_q = [q1_star, q2_star, q3_star, q4_star, q5_star]
snow_q = [q1_snow, q2_snow, q3_snow, q4_snow, q5_snow]

resultados = []
for i in range(5):
    print(f"  [{i+1}/5] {nombres[i]}...")
    oltp = medir(oltp_q[i])
    star = medir(star_q[i])
    snow = medir(snow_q[i])
    resultados.append((oltp, star, snow))

print()
print(f"  {'':22} {'oltp':>7} {'star':>7} {'snow':>7} {'s/o':>7} {'s/s':>7}")
print("  " + "-" * 60)
for i, (oltp, star, snow) in enumerate(resultados):
    print(f"  {nombres[i]:22} {oltp:>7} {star:>7} {snow:>7} {mejora(oltp,star):>7} {diferencia(star,snow):>7}")
print()

cur.close()
conn.close()
