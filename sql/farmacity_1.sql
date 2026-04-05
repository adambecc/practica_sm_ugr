-- PRACTICA 1 - SISTEMAS MULTIDIMENSIONALES
-- FarmaCity Ceuta S.L.
-- Adam Ben Chacha

-- PASO 0: LIMPIEZA (por si se ejecuta más de una vez)
DROP SCHEMA IF EXISTS olap_snow   CASCADE;
DROP SCHEMA IF EXISTS olap_star   CASCADE;
DROP SCHEMA IF EXISTS oltp_finanzas CASCADE;
DROP SCHEMA IF EXISTS oltp_rrhh     CASCADE;
DROP SCHEMA IF EXISTS oltp_logistica CASCADE;
DROP SCHEMA IF EXISTS oltp_inventario CASCADE;
DROP SCHEMA IF EXISTS oltp_ventas    CASCADE;


-- PASO 1: CREAR ESQUEMAS OLTP
CREATE SCHEMA oltp_ventas;
CREATE SCHEMA oltp_inventario;
CREATE SCHEMA oltp_logistica;
CREATE SCHEMA oltp_rrhh;
CREATE SCHEMA oltp_finanzas;

-- PASO 2: DDL — TABLAS OLTP
-- RRHH (primero porque ventas referencia empleados)
CREATE TABLE oltp_rrhh.tiendas (
    tienda_id  SERIAL PRIMARY KEY,
    nombre     VARCHAR(100) NOT NULL,
    zona       VARCHAR(80),
    direccion  VARCHAR(200),
    aforo      INT
);

CREATE TABLE oltp_rrhh.empleados (
    empleado_id SERIAL PRIMARY KEY,
    nombre      VARCHAR(150) NOT NULL,
    cargo       VARCHAR(80),
    tienda_id   INT REFERENCES oltp_rrhh.tiendas(tienda_id),
    fecha_alta  DATE DEFAULT CURRENT_DATE,
    activo      BOOLEAN DEFAULT TRUE
);

CREATE TABLE oltp_rrhh.turnos (
    turno_id    SERIAL PRIMARY KEY,
    nombre      VARCHAR(50),
    hora_inicio TIME,
    hora_fin    TIME
);

CREATE TABLE oltp_rrhh.asignaciones_turno (
    asignacion_id SERIAL PRIMARY KEY,
    empleado_id   INT REFERENCES oltp_rrhh.empleados(empleado_id),
    turno_id      INT REFERENCES oltp_rrhh.turnos(turno_id),
    fecha         DATE NOT NULL,
    tienda_id     INT  REFERENCES oltp_rrhh.tiendas(tienda_id)
);

-- VENTAS
CREATE TABLE oltp_ventas.clientes (
    cliente_id   SERIAL PRIMARY KEY,
    nombre       VARCHAR(150) NOT NULL,
    cif          VARCHAR(10),
    telefono     VARCHAR(20),
    email        VARCHAR(100),
    direccion    VARCHAR(200),
    ciudad       VARCHAR(80),
    region       VARCHAR(80) DEFAULT 'Ceuta',
    tipo_cliente VARCHAR(20) CHECK (tipo_cliente IN ('Local','Online','Transfronterizo')),
    canal        VARCHAR(20) CHECK (canal IN ('Fisico','Online')),
    fecha_alta   DATE DEFAULT CURRENT_DATE
);

CREATE TABLE oltp_ventas.empleados_ref (
    empleado_id SERIAL PRIMARY KEY,
    nombre      VARCHAR(150) NOT NULL,
    tienda_id   INT
);

CREATE TABLE oltp_ventas.categorias (
    categoria_id SERIAL PRIMARY KEY,
    nombre       VARCHAR(100) NOT NULL,
    descripcion  TEXT
);

CREATE TABLE oltp_ventas.productos (
    producto_id  SERIAL PRIMARY KEY,
    nombre       VARCHAR(150) NOT NULL,
    categoria_id INT REFERENCES oltp_ventas.categorias(categoria_id),
    precio_venta NUMERIC(10,2) NOT NULL CHECK (precio_venta > 0),
    marca        VARCHAR(100),
    activo       BOOLEAN DEFAULT TRUE
);

CREATE TABLE oltp_ventas.pedidos (
    pedido_id    SERIAL PRIMARY KEY,
    cliente_id   INT REFERENCES oltp_ventas.clientes(cliente_id),
    empleado_id  INT REFERENCES oltp_ventas.empleados_ref(empleado_id),
    tienda_id    INT,
    fecha_pedido DATE NOT NULL DEFAULT CURRENT_DATE,
    canal        VARCHAR(20) CHECK (canal IN ('Fisico','Online'))
);

CREATE TABLE oltp_ventas.detalle_pedido (
    detalle_id      SERIAL PRIMARY KEY,
    pedido_id       INT REFERENCES oltp_ventas.pedidos(pedido_id),
    producto_id     INT REFERENCES oltp_ventas.productos(producto_id),
    cantidad        INT NOT NULL CHECK (cantidad > 0),
    precio_unitario NUMERIC(10,2) NOT NULL,
    descuento       NUMERIC(5,2) DEFAULT 0
);

CREATE INDEX idx_pedidos_fecha    ON oltp_ventas.pedidos(fecha_pedido);
CREATE INDEX idx_pedidos_cliente  ON oltp_ventas.pedidos(cliente_id);
CREATE INDEX idx_detalle_producto ON oltp_ventas.detalle_pedido(producto_id);

-- INVENTARIO
CREATE TABLE oltp_inventario.proveedores (
    proveedor_id    SERIAL PRIMARY KEY,
    nombre          VARCHAR(150) NOT NULL,
    pais            VARCHAR(80),
    contacto        VARCHAR(100),
    telefono        VARCHAR(20),
    tipo_suministro VARCHAR(50)
);

CREATE TABLE oltp_inventario.categorias_inv (
    categoria_id SERIAL PRIMARY KEY,
    nombre       VARCHAR(100) NOT NULL,
    descripcion  TEXT
);

CREATE TABLE oltp_inventario.productos_inv (
    producto_id  INT PRIMARY KEY,
    proveedor_id INT REFERENCES oltp_inventario.proveedores(proveedor_id),
    categoria_id INT REFERENCES oltp_inventario.categorias_inv(categoria_id),
    stock_actual INT DEFAULT 0 CHECK (stock_actual >= 0),
    stock_minimo INT DEFAULT 5
);

CREATE TABLE oltp_inventario.costes_producto (
    producto_id         INT PRIMARY KEY REFERENCES oltp_inventario.productos_inv(producto_id),
    coste_unitario      NUMERIC(10,2) NOT NULL CHECK (coste_unitario > 0),
    fecha_actualizacion DATE DEFAULT CURRENT_DATE
);

CREATE TABLE oltp_inventario.movimientos_stock (
    movimiento_id SERIAL PRIMARY KEY,
    producto_id   INT REFERENCES oltp_inventario.productos_inv(producto_id),
    tipo          VARCHAR(10) CHECK (tipo IN ('Entrada','Salida')),
    cantidad      INT NOT NULL CHECK (cantidad > 0),
    fecha         DATE NOT NULL DEFAULT CURRENT_DATE,
    motivo        VARCHAR(100)
);

CREATE INDEX idx_mov_producto ON oltp_inventario.movimientos_stock(producto_id);
CREATE INDEX idx_mov_fecha    ON oltp_inventario.movimientos_stock(fecha);

-- LOGISTICA
CREATE TABLE oltp_logistica.transportistas (
    transportista_id SERIAL PRIMARY KEY,
    nombre           VARCHAR(150) NOT NULL,
    telefono         VARCHAR(20),
    cobertura        VARCHAR(100),
    tipo_envio       VARCHAR(50)
);

CREATE TABLE oltp_logistica.zonas_cobertura (
    zona_id              SERIAL PRIMARY KEY,
    transportista_id     INT REFERENCES oltp_logistica.transportistas(transportista_id),
    region               VARCHAR(80),
    tiempo_estimado_dias INT
);

CREATE TABLE oltp_logistica.envios (
    envio_id         SERIAL PRIMARY KEY,
    pedido_id        INT NOT NULL,
    transportista_id INT REFERENCES oltp_logistica.transportistas(transportista_id),
    fecha_envio      DATE,
    fecha_entrega    DATE,
    coste_envio      NUMERIC(10,2),
    region_destino   VARCHAR(100),
    estado           VARCHAR(30) DEFAULT 'Pendiente'
);

CREATE TABLE oltp_logistica.seguimiento_entrega (
    seguimiento_id SERIAL PRIMARY KEY,
    envio_id       INT REFERENCES oltp_logistica.envios(envio_id),
    fecha_evento   TIMESTAMP DEFAULT NOW(),
    estado         VARCHAR(50),
    ubicacion      VARCHAR(100)
);

CREATE INDEX idx_envios_pedido ON oltp_logistica.envios(pedido_id);
CREATE INDEX idx_envios_fecha  ON oltp_logistica.envios(fecha_envio);

-- FINANZAS
CREATE TABLE oltp_finanzas.facturas (
    factura_id    SERIAL PRIMARY KEY,
    pedido_id     INT NOT NULL,
    cliente_id    INT NOT NULL,
    fecha         DATE DEFAULT CURRENT_DATE,
    importe_total NUMERIC(12,2),
    estado        VARCHAR(20) DEFAULT 'Emitida'
);

CREATE TABLE oltp_finanzas.lineas_factura (
    linea_id        SERIAL PRIMARY KEY,
    factura_id      INT REFERENCES oltp_finanzas.facturas(factura_id),
    producto_id     INT,
    cantidad        INT,
    precio_unitario NUMERIC(10,2),
    importe         NUMERIC(12,2)
);

CREATE TABLE oltp_finanzas.pagos (
    pago_id    SERIAL PRIMARY KEY,
    factura_id INT REFERENCES oltp_finanzas.facturas(factura_id),
    fecha_pago DATE,
    importe    NUMERIC(12,2),
    metodo     VARCHAR(50)
);

CREATE TABLE oltp_finanzas.analisis_coste (
    analisis_id   SERIAL PRIMARY KEY,
    producto_id   INT,
    mes           INT,
    anio          INT,
    coste_total   NUMERIC(12,2),
    ingreso_total NUMERIC(12,2),
    margen        NUMERIC(12,2)
);

-- PASO 3: DML — DATOS SINTÉTICOS
-- RRHH: TIENDAS
INSERT INTO oltp_rrhh.tiendas (nombre, zona, direccion, aforo) VALUES
('FarmaCity Centro',     'Centro',      'Calle Real 12, Ceuta',          50),
('FarmaCity El Principe','El Principe', 'Av. España 45, Ceuta',          40),
('FarmaCity Benzu',      'Benzu',       'Calle Benzu 3, Ceuta',          35),
('FarmaCity El Tarajal', 'El Tarajal',  'Calle Tarajal 8, Ceuta',        45);

-- RRHH: TURNOS
INSERT INTO oltp_rrhh.turnos (nombre, hora_inicio, hora_fin) VALUES
('Manana',  '08:00', '15:00'),
('Tarde',   '15:00', '22:00'),
('Partido', '09:00', '13:00');

-- RRHH: EMPLEADOS (20)
INSERT INTO oltp_rrhh.empleados (nombre, cargo, tienda_id, fecha_alta) VALUES
('Javier Sanchez Torres',     'Director',    1, '2015-10-01'),
('Ahmed Ben Moussaoui',       'Encargado',   1, '2018-06-15'),
('Maria Lopez Garcia',        'Dependienta', 1, '2020-03-01'),
('Sofia Ramirez Vidal',       'Dependienta', 1, '2021-07-12'),
('Carlos Moreno Jimenez',     'Encargado',   2, '2019-01-20'),
('Lucia Fernandez Ruiz',      'Dependienta', 2, '2021-09-10'),
('Patricia Gomez Herrera',    'Dependienta', 2, '2022-02-14'),
('Daniel Ortiz Navarro',      'Dependiente', 2, '2020-11-03'),
('Fatima El Amrani Boukili',  'Encargada',   3, '2019-05-20'),
('Ibrahim Lahlou El Idrissi', 'Dependiente', 3, '2021-03-08'),
('Nadia Benchekroun Sabir',   'Dependienta', 3, '2022-06-01'),
('Roberto Diaz Castillo',     'Dependiente', 3, '2023-01-15'),
('Youssef Benali Tazi',       'Encargado',   4, '2018-09-10'),
('Elena Vega Molina',         'Dependienta', 4, '2020-04-22'),
('Karim Ouazzani Chahdi',     'Dependiente', 4, '2021-12-05'),
('Ana Ruiz Perez',            'Dependienta', 4, '2022-08-18'),
('Pedro Gutierrez Leal',      'Administrativo',1,'2017-03-14'),
('Samira Alouach Belhaj',     'Dependienta', 2, '2023-03-20'),
('Miguel Rios Blanco',        'Dependiente', 3, '2023-06-10'),
('Laila Benjelloun Fassi',    'Dependienta', 4, '2023-09-01');

-- RRHH: ASIGNACIONES (50)
INSERT INTO oltp_rrhh.asignaciones_turno (empleado_id, turno_id, fecha, tienda_id) VALUES
(1,1,'2023-01-02',1),(2,1,'2023-01-02',1),(3,2,'2023-01-02',1),(4,3,'2023-01-03',1),
(5,1,'2023-01-02',2),(6,2,'2023-01-02',2),(7,1,'2023-01-03',2),(8,3,'2023-01-03',2),
(9,1,'2023-01-02',3),(10,2,'2023-01-02',3),(11,1,'2023-01-04',3),(12,2,'2023-01-04',3),
(13,1,'2023-01-02',4),(14,2,'2023-01-02',4),(15,1,'2023-01-05',4),(16,3,'2023-01-05',4),
(1,1,'2023-02-01',1),(2,2,'2023-02-01',1),(3,1,'2023-02-02',1),(5,1,'2023-02-01',2),
(6,2,'2023-02-01',2),(9,1,'2023-02-01',3),(13,1,'2023-02-01',4),(14,2,'2023-02-02',4),
(1,1,'2023-03-01',1),(5,1,'2023-03-01',2),(9,1,'2023-03-01',3),(13,1,'2023-03-01',4),
(2,2,'2023-04-01',1),(6,2,'2023-04-01',2),(10,2,'2023-04-01',3),(14,2,'2023-04-01',4),
(3,1,'2023-05-01',1),(7,1,'2023-05-01',2),(11,1,'2023-05-01',3),(15,1,'2023-05-01',4),
(4,3,'2023-06-01',1),(8,3,'2023-06-01',2),(12,3,'2023-06-01',3),(16,3,'2023-06-01',4),
(1,1,'2023-07-03',1),(5,1,'2023-07-03',2),(9,1,'2023-07-03',3),(13,1,'2023-07-03',4),
(2,2,'2023-08-01',1),(6,2,'2023-08-01',2),(17,1,'2023-09-01',1),(18,2,'2023-10-01',2),
(19,1,'2023-11-01',3),(20,2,'2023-12-01',4);

-- VENTAS: CATEGORIAS
INSERT INTO oltp_ventas.categorias (nombre, descripcion) VALUES
('Cosmetica',         'Cremas, maquillaje y cuidado facial'),
('Nutricion',         'Suplementos y complementos vitaminicos'),
('Higiene Personal',  'Jabones, champu y desodorantes'),
('Dermocosmetica',    'Productos de farmacia para la piel'),
('Bebe y Materna',    'Productos para bebes y madres lactantes');

-- VENTAS: PRODUCTOS (20)
INSERT INTO oltp_ventas.productos (nombre, categoria_id, precio_venta, marca) VALUES
('Crema Hidratante Facial SPF30',   1, 18.90, 'Isdin'),
('Serum Vitamina C 30ml',           1, 24.50, 'Sesderma'),
('Mascarilla Hidratante',           1, 12.30, 'Vichy'),
('Base de maquillaje fluida',       1, 21.00, 'Payot'),
('Proteina Whey Vainilla 1kg',      2, 32.00, 'Weider'),
('Omega 3 60 capsulas',             2, 15.80, 'Solgar'),
('Multivitaminico Hombre 30comp',   2, 19.90, 'Centrum'),
('Vitamina D3 2000UI',              2,  9.50, 'Kern Pharma'),
('Gel Ducha Neutro 750ml',          3,  4.20, 'Dove'),
('Champu Anticaida 400ml',          3,  8.70, 'Vichy'),
('Desodorante Rollon 48h',          3,  5.10, 'Rexona'),
('Pasta Dental Blanqueadora 100ml', 3,  3.80, 'Colgate'),
('Hidratante Corporal Atopia 400ml',4, 14.60, 'Aveeno'),
('Fotoprotector 50+ Spray 200ml',   4, 22.00, 'Isdin'),
('Crema Cicatrizante 30g',          4, 11.40, 'Bepanthen'),
('Serum Antirojeces 30ml',          4, 27.50, 'La Roche-Posay'),
('Papilla Cereales Sin Gluten 300g',5,  7.90, 'Blevit'),
('Crema Pañal Protectora 100ml',    5,  8.20, 'Mustela'),
('Leche de Continuacion 800g',      5, 16.50, 'Nidina'),
('Sacaleches Electrico',            5, 89.00, 'Medela');

-- VENTAS: EMPLEADOS REF
INSERT INTO oltp_ventas.empleados_ref (nombre, tienda_id) VALUES
('Ahmed Ben Moussaoui',   1),
('Carlos Moreno Jimenez', 2),
('Fatima El Amrani',      3),
('Youssef Benali Tazi',   4),
('Maria Lopez Garcia',    1),
('Lucia Fernandez Ruiz',  2),
('Elena Vega Molina',     4),
('Ibrahim Lahlou',        3);

-- VENTAS: CLIENTES (50)
INSERT INTO oltp_ventas.clientes (nombre,telefono,email,ciudad,tipo_cliente,canal) VALUES
('Ana Ruiz Perez',          '+34-600-111001','ana.ruiz@mail.com',      'Ceuta',    'Local',           'Fisico'),
('Mohamed Azzaoui',         '+34-611-111002','m.azzaoui@mail.com',     'Ceuta',    'Transfronterizo', 'Fisico'),
('Parafarmacia Sur S.L.',   '+34-956-111003','info@parasur.es',        'Cadiz',    'Online',          'Online'),
('Laura Gonzalez Vidal',    '+34-622-111004','laura.g@mail.com',       'Ceuta',    'Local',           'Fisico'),
('Hicham Larbi',            '+34-633-111005','h.larbi@mail.com',       'Ceuta',    'Transfronterizo', 'Fisico'),
('Farmacia Norte S.L.',     '+34-913-111006','fnorte@mail.es',         'Madrid',   'Online',          'Online'),
('Carmen Reyes Blanco',     '+34-644-111007','carmen.r@mail.com',      'Ceuta',    'Local',           'Fisico'),
('Driss El Alami',          '+34-655-111008','d.elalami@mail.com',     'Ceuta',    'Transfronterizo', 'Fisico'),
('Belen Morales S.L.',      '+34-666-111009','belen@empresa.es',       'Algeciras','Online',          'Online'),
('Nuria Fernandez Gil',     '+34-677-111010','nuria.f@mail.com',       'Ceuta',    'Local',           'Fisico'),
('Abdelkader Tazi',         '+34-688-111011','a.tazi@mail.com',        'Ceuta',    'Transfronterizo', 'Fisico'),
('Tienda Natural S.L.',     '+34-699-111012','tn@tiendanatural.es',    'Sevilla',  'Online',          'Online'),
('Marta Iglesias Vega',     '+34-600-111013','marta.i@mail.com',       'Ceuta',    'Local',           'Fisico'),
('Khalid Bennis',           '+34-611-111014','k.bennis@mail.com',      'Ceuta',    'Transfronterizo', 'Fisico'),
('Sara Molina Duran',       '+34-622-111015','sara.m@mail.com',        'Ceuta',    'Local',           'Fisico'),
('EcoFarma Online S.L.',    '+34-933-111016','eco@ecofarma.es',        'Barcelona','Online',          'Online'),
('Jose Luis Pardo',         '+34-633-111017','jl.pardo@mail.com',      'Ceuta',    'Local',           'Fisico'),
('Zineb Bouabid',           '+34-644-111018','z.bouabid@mail.com',     'Ceuta',    'Transfronterizo', 'Fisico'),
('Isabel Martos Ruiz',      '+34-655-111019','isabel.m@mail.com',      'Ceuta',    'Local',           'Fisico'),
('Hamid El Ouazzani',       '+34-666-111020','h.ouazzani@mail.com',    'Ceuta',    'Transfronterizo', 'Fisico'),
('Farmacia Playa S.L.',     '+34-956-111021','fplaya@mail.es',         'Malaga',   'Online',          'Online'),
('Rosa Navarro Sanz',       '+34-677-111022','rosa.n@mail.com',        'Ceuta',    'Local',           'Fisico'),
('Yassine Chaoui',          '+34-688-111023','y.chaoui@mail.com',      'Ceuta',    'Transfronterizo', 'Fisico'),
('Pilar Serrano Leal',      '+34-699-111024','pilar.s@mail.com',       'Ceuta',    'Local',           'Fisico'),
('Moussa Benali',           '+34-600-111025','m.benali@mail.com',      'Ceuta',    'Transfronterizo', 'Fisico'),
('Natural Health S.L.',     '+34-911-111026','nh@naturalhealth.es',    'Madrid',   'Online',          'Online'),
('Cristina Vargas Cano',    '+34-611-111027','cristina.v@mail.com',    'Ceuta',    'Local',           'Fisico'),
('Omar El Fassi',           '+34-622-111028','o.elfassi@mail.com',     'Ceuta',    'Transfronterizo', 'Fisico'),
('Manuel Rios Torres',      '+34-633-111029','manuel.r@mail.com',      'Ceuta',    'Local',           'Fisico'),
('Salma Lahlou',            '+34-644-111030','s.lahlou@mail.com',      'Ceuta',    'Transfronterizo', 'Fisico'),
('Farmacia Sur Online',     '+34-957-111031','fso@farmasur.es',        'Cordoba',  'Online',          'Online'),
('Antonio Medina Flores',   '+34-655-111032','a.medina@mail.com',      'Ceuta',    'Local',           'Fisico'),
('Khadija Benchekroun',     '+34-666-111033','k.benchekroun@mail.com', 'Ceuta',    'Transfronterizo', 'Fisico'),
('Beatriz Lozano Perez',    '+34-677-111034','beatriz.l@mail.com',     'Ceuta',    'Local',           'Fisico'),
('Rachid Amrani',           '+34-688-111035','r.amrani@mail.com',      'Ceuta',    'Transfronterizo', 'Fisico'),
('VitaShop S.L.',           '+34-934-111036','info@vitashop.es',       'Barcelona','Online',          'Online'),
('Elena Campos Ruiz',       '+34-699-111037','elena.c@mail.com',       'Ceuta',    'Local',           'Fisico'),
('Tarik Boussaid',          '+34-600-111038','t.boussaid@mail.com',    'Ceuta',    'Transfronterizo', 'Fisico'),
('Lucia Herrera Vidal',     '+34-611-111039','lucia.h@mail.com',       'Ceuta',    'Local',           'Fisico'),
('Noura El Idrissi',        '+34-622-111040','n.elidrissi@mail.com',   'Ceuta',    'Transfronterizo', 'Fisico'),
('Farmacia Digital S.L.',   '+34-912-111041','fd@farmaciadigital.es',  'Madrid',   'Online',          'Online'),
('Pablo Jimenez Cruz',      '+34-633-111042','pablo.j@mail.com',       'Ceuta',    'Local',           'Fisico'),
('Houda Sabir',             '+34-644-111043','h.sabir@mail.com',       'Ceuta',    'Transfronterizo', 'Fisico'),
('Silvia Ramos Ortega',     '+34-655-111044','silvia.r@mail.com',      'Ceuta',    'Local',           'Fisico'),
('Abdelilah Fassi',         '+34-666-111045','a.fassi@mail.com',       'Ceuta',    'Transfronterizo', 'Fisico'),
('Salud Natural S.L.',      '+34-958-111046','sn@saludnatural.es',     'Granada',  'Online',          'Online'),
('David Fuentes Blanco',    '+34-677-111047','david.f@mail.com',       'Ceuta',    'Local',           'Fisico'),
('Imane Karimi',            '+34-688-111048','i.karimi@mail.com',      'Ceuta',    'Transfronterizo', 'Fisico'),
('Patricia Vega Soler',     '+34-699-111049','patricia.v@mail.com',    'Ceuta',    'Local',           'Fisico'),
('Younes El Khatib',        '+34-600-111050','y.elkhatib@mail.com',    'Ceuta',    'Transfronterizo', 'Fisico');

-- VENTAS: PEDIDOS (60)
INSERT INTO oltp_ventas.pedidos (cliente_id, empleado_id, tienda_id, fecha_pedido, canal) VALUES
(1,1,1,'2022-03-05','Fisico'),(2,1,1,'2022-04-12','Fisico'),(3,2,2,'2022-05-20','Online'),
(4,2,2,'2022-06-08','Fisico'),(5,3,3,'2022-07-15','Fisico'),(6,3,3,'2022-08-22','Online'),
(7,4,4,'2022-09-10','Fisico'),(8,4,4,'2022-10-18','Fisico'),(9,1,1,'2022-11-05','Online'),
(10,2,2,'2022-12-20','Fisico'),(11,3,3,'2023-01-08','Fisico'),(12,4,4,'2023-01-22','Online'),
(13,1,1,'2023-02-10','Fisico'),(14,2,2,'2023-02-25','Fisico'),(15,3,3,'2023-03-14','Fisico'),
(16,4,4,'2023-03-28','Online'),(17,5,1,'2023-04-05','Fisico'),(18,6,2,'2023-04-19','Fisico'),
(19,7,4,'2023-05-02','Fisico'),(20,8,3,'2023-05-16','Fisico'),(21,1,1,'2023-06-01','Online'),
(22,2,2,'2023-06-15','Fisico'),(23,3,3,'2023-07-04','Fisico'),(24,4,4,'2023-07-18','Fisico'),
(25,5,1,'2023-08-01','Fisico'),(26,6,2,'2023-08-15','Online'),(27,7,4,'2023-09-03','Fisico'),
(28,8,3,'2023-09-17','Fisico'),(29,1,1,'2023-10-01','Fisico'),(30,2,2,'2023-10-15','Online'),
(31,3,3,'2023-11-02','Fisico'),(32,4,4,'2023-11-16','Fisico'),(33,5,1,'2023-12-01','Fisico'),
(34,6,2,'2023-12-15','Online'),(35,7,4,'2024-01-10','Fisico'),(36,8,3,'2024-01-24','Fisico'),
(37,1,1,'2024-02-07','Fisico'),(38,2,2,'2024-02-21','Online'),(39,3,3,'2024-03-06','Fisico'),
(40,4,4,'2024-03-20','Fisico'),(41,5,1,'2024-04-03','Fisico'),(42,6,2,'2024-04-17','Online'),
(43,7,4,'2024-05-01','Fisico'),(44,8,3,'2024-05-15','Fisico'),(45,1,1,'2024-06-05','Fisico'),
(46,2,2,'2024-06-19','Online'),(47,3,3,'2024-07-10','Fisico'),(48,4,4,'2024-07-24','Fisico'),
(49,5,1,'2024-08-07','Fisico'),(50,6,2,'2024-08-21','Online'),(51,7,4,'2024-09-04','Fisico'),
(52,8,3,'2024-09-18','Fisico'),(53,1,1,'2024-10-02','Fisico'),(54,2,2,'2024-10-16','Online'),
(55,3,3,'2024-11-06','Fisico'),(56,4,4,'2024-11-20','Fisico'),(57,5,1,'2024-12-04','Fisico'),
(58,6,2,'2024-12-18','Online'),(59,7,4,'2025-01-08','Fisico'),(60,8,3,'2025-01-22','Fisico');

-- VENTAS: DETALLE PEDIDO (120 lineas)
INSERT INTO oltp_ventas.detalle_pedido (pedido_id, producto_id, cantidad, precio_unitario, descuento) VALUES
(1,1,2,18.90,0),(1,9,3,4.20,0),(2,14,1,22.00,5),(2,10,2,8.70,0),
(3,5,1,32.00,10),(3,6,2,15.80,0),(4,2,1,24.50,0),(4,13,2,14.60,0),
(5,17,3,7.90,0),(5,18,1,8.20,0),(6,7,2,19.90,5),(6,8,4,9.50,0),
(7,3,2,12.30,0),(7,11,3,5.10,0),(8,19,1,16.50,0),(8,20,1,89.00,0),
(9,4,1,21.00,0),(9,16,1,27.50,10),(10,1,3,18.90,0),(10,15,2,11.40,0),
(11,12,5,3.80,0),(11,9,4,4.20,0),(12,6,3,15.80,5),(12,7,1,19.90,0),
(13,2,2,24.50,0),(13,14,1,22.00,0),(14,5,1,32.00,0),(14,10,1,8.70,0),
(15,1,4,18.90,5),(15,13,1,14.60,0),(16,17,2,7.90,0),(16,18,2,8.20,0),
(17,3,1,12.30,0),(17,11,2,5.10,0),(18,8,3,9.50,0),(18,16,1,27.50,0),
(19,4,2,21.00,0),(19,15,1,11.40,0),(20,19,2,16.50,5),(20,12,3,3.80,0),
(21,2,1,24.50,0),(21,6,2,15.80,0),(22,1,3,18.90,0),(22,14,2,22.00,5),
(23,5,2,32.00,10),(23,9,4,4.20,0),(24,7,1,19.90,0),(24,13,2,14.60,0),
(25,10,2,8.70,0),(25,3,1,12.30,0),(26,20,1,89.00,0),(26,17,1,7.90,0),
(27,4,3,21.00,0),(27,11,2,5.10,0),(28,16,1,27.50,0),(28,8,4,9.50,0),
(29,15,2,11.40,0),(29,1,2,18.90,0),(30,6,3,15.80,0),(30,12,6,3.80,0),
(31,2,1,24.50,0),(31,10,2,8.70,5),(32,5,1,32.00,0),(32,18,2,8.20,0),
(33,14,1,22.00,0),(33,9,5,4.20,0),(34,7,2,19.90,0),(34,13,1,14.60,0),
(35,19,1,16.50,0),(35,3,2,12.30,0),(36,1,4,18.90,5),(36,11,3,5.10,0),
(37,16,1,27.50,0),(37,8,2,9.50,0),(38,4,1,21.00,0),(38,15,2,11.40,0),
(39,6,2,15.80,0),(39,12,4,3.80,0),(40,2,2,24.50,0),(40,17,2,7.90,0),
(41,5,1,32.00,10),(41,10,1,8.70,0),(42,14,2,22.00,0),(42,18,1,8.20,0),
(43,1,3,18.90,0),(43,9,3,4.20,0),(44,7,1,19.90,5),(44,13,2,14.60,0),
(45,3,2,12.30,0),(45,16,1,27.50,0),(46,20,1,89.00,0),(46,6,2,15.80,0),
(47,11,4,5.10,0),(47,8,3,9.50,0),(48,4,2,21.00,0),(48,15,1,11.40,0),
(49,2,1,24.50,0),(49,19,2,16.50,5),(50,5,2,32.00,0),(50,12,5,3.80,0),
(51,1,5,18.90,5),(51,10,2,8.70,0),(52,14,1,22.00,0),(52,17,3,7.90,0),
(53,7,2,19.90,0),(53,13,1,14.60,0),(54,6,3,15.80,0),(54,18,2,8.20,0),
(55,3,1,12.30,0),(55,9,6,4.20,0),(56,16,1,27.50,0),(56,11,2,5.10,0),
(57,4,3,21.00,0),(57,15,2,11.40,5),(58,2,2,24.50,0),(58,8,4,9.50,0),
(59,5,1,32.00,0),(59,20,1,89.00,0),(60,1,4,18.90,0),(60,12,8,3.80,0);

-- INVENTARIO: PROVEEDORES (10)
INSERT INTO oltp_inventario.proveedores (nombre,pais,contacto,telefono,tipo_suministro) VALUES
('Isdin S.A.',                'Espana',  'comercial@isdin.com',       '+34-93-4003000','Dermocosmetica'),
('Sesderma Laboratories S.L.','Espana',  'info@sesderma.com',         '+34-96-3900400','Dermocosmetica'),
('Vichy Laboratoires',        'Francia', 'vichy@loreal.fr',           '+33-1-47560000','Cosmetica'),
('Payot Paris',               'Francia', 'contact@payot.fr',          '+33-1-45600000','Cosmetica'),
('Weider Nutrition Iberica',   'Espana',  'weider@weider.es',          '+34-91-3000100','Nutricion'),
('Solgar Vitamin & Herb',     'EEUU',    'solgar@solgar.com',         '+1-201-9680600','Nutricion'),
('Pfizer Consumer Healthcare','EEUU',    'centrum@pfizer.com',        '+1-212-7332323','Nutricion'),
('Kern Pharma S.L.',          'Espana',  'kern@kernpharma.com',       '+34-93-5700300','Nutricion'),
('Mustela - Laboratoires Expanscience','Francia','mustela@mustela.fr','+33-1-44690000','Bebe y Materna'),
('Medela AG',                 'Suiza',   'medela@medela.com',         '+41-41-5625050','Bebe y Materna');

-- INVENTARIO: CATEGORIAS INV (5)
INSERT INTO oltp_inventario.categorias_inv (nombre, descripcion) VALUES
('Cosmetica',        'Cremas, maquillaje y cuidado facial'),
('Nutricion',        'Suplementos y complementos vitaminicos'),
('Higiene Personal', 'Jabones, champu y desodorantes'),
('Dermocosmetica',   'Productos de farmacia para la piel'),
('Bebe y Materna',   'Productos para bebes y madres lactantes');

-- INVENTARIO: PRODUCTOS INV (20)
INSERT INTO oltp_inventario.productos_inv (producto_id,proveedor_id,categoria_id,stock_actual,stock_minimo) VALUES
(1,1,1,85,10),(2,2,1,60,8),(3,3,1,45,8),(4,4,1,30,5),
(5,5,2,70,10),(6,6,2,55,8),(7,7,2,80,10),(8,8,2,90,10),
(9,3,3,200,20),(10,3,3,75,10),(11,1,3,150,15),(12,4,3,300,30),
(13,1,4,65,8),(14,1,4,50,8),(15,2,4,40,5),(16,3,4,35,5),
(17,9,5,120,15),(18,9,5,95,10),(19,7,5,60,8),(20,10,5,15,3);

-- INVENTARIO: COSTES (20)
INSERT INTO oltp_inventario.costes_producto (producto_id,coste_unitario,fecha_actualizacion) VALUES
(1,9.50,'2022-01-01'),(2,12.00,'2022-01-01'),(3,6.20,'2022-01-01'),(4,10.50,'2022-01-01'),
(5,16.00,'2022-01-01'),(6,8.00,'2022-01-01'),(7,10.00,'2022-01-01'),(8,4.80,'2022-01-01'),
(9,2.10,'2022-01-01'),(10,4.40,'2022-01-01'),(11,2.60,'2022-01-01'),(12,1.90,'2022-01-01'),
(13,7.30,'2022-01-01'),(14,11.00,'2022-01-01'),(15,5.70,'2022-01-01'),(16,13.80,'2022-01-01'),
(17,3.95,'2022-01-01'),(18,4.10,'2022-01-01'),(19,8.30,'2022-01-01'),(20,44.50,'2022-01-01');

-- INVENTARIO: MOVIMIENTOS (60)
INSERT INTO oltp_inventario.movimientos_stock (producto_id,tipo,cantidad,fecha,motivo) VALUES
(1,'Entrada',50,'2022-01-05','Reposicion proveedor'),(1,'Salida',5,'2022-03-05','Venta'),
(2,'Entrada',40,'2022-01-10','Reposicion proveedor'),(2,'Salida',3,'2022-04-12','Venta'),
(3,'Entrada',30,'2022-01-15','Reposicion proveedor'),(3,'Salida',4,'2022-05-20','Venta'),
(4,'Entrada',25,'2022-02-01','Reposicion proveedor'),(4,'Salida',2,'2022-06-08','Venta'),
(5,'Entrada',60,'2022-02-05','Reposicion proveedor'),(5,'Salida',3,'2022-07-15','Venta'),
(6,'Entrada',45,'2022-02-10','Reposicion proveedor'),(6,'Salida',6,'2022-08-22','Venta'),
(7,'Entrada',70,'2022-03-01','Reposicion proveedor'),(7,'Salida',2,'2022-09-10','Venta'),
(8,'Entrada',80,'2022-03-05','Reposicion proveedor'),(8,'Salida',4,'2022-10-18','Venta'),
(9,'Entrada',150,'2022-03-10','Reposicion proveedor'),(9,'Salida',12,'2022-11-05','Venta'),
(10,'Entrada',60,'2022-04-01','Reposicion proveedor'),(10,'Salida',3,'2022-12-20','Venta'),
(11,'Entrada',120,'2022-04-05','Reposicion proveedor'),(11,'Salida',5,'2023-01-08','Venta'),
(12,'Entrada',250,'2022-04-10','Reposicion proveedor'),(12,'Salida',15,'2023-01-22','Venta'),
(13,'Entrada',55,'2022-05-01','Reposicion proveedor'),(13,'Salida',4,'2023-02-10','Venta'),
(14,'Entrada',45,'2022-05-05','Reposicion proveedor'),(14,'Salida',3,'2023-02-25','Venta'),
(15,'Entrada',35,'2022-05-10','Reposicion proveedor'),(15,'Salida',4,'2023-03-14','Venta'),
(16,'Entrada',30,'2022-06-01','Reposicion proveedor'),(16,'Salida',2,'2023-03-28','Venta'),
(17,'Entrada',100,'2022-06-05','Reposicion proveedor'),(17,'Salida',8,'2023-04-05','Venta'),
(18,'Entrada',80,'2022-06-10','Reposicion proveedor'),(18,'Salida',5,'2023-04-19','Venta'),
(19,'Entrada',50,'2022-07-01','Reposicion proveedor'),(19,'Salida',3,'2023-05-02','Venta'),
(20,'Entrada',12,'2022-07-05','Reposicion proveedor'),(20,'Salida',1,'2023-05-16','Venta'),
(1,'Entrada',40,'2023-06-01','Reposicion proveedor'),(5,'Entrada',30,'2023-07-01','Reposicion proveedor'),
(9,'Entrada',100,'2023-08-01','Reposicion proveedor'),(14,'Entrada',25,'2023-09-01','Reposicion proveedor'),
(2,'Salida',5,'2023-10-01','Venta'),(7,'Salida',3,'2023-11-01','Venta'),
(11,'Salida',8,'2023-12-01','Venta'),(3,'Entrada',20,'2024-01-01','Reposicion proveedor'),
(6,'Entrada',30,'2024-02-01','Reposicion proveedor'),(10,'Salida',4,'2024-03-01','Venta'),
(15,'Salida',3,'2024-04-01','Venta'),(18,'Entrada',40,'2024-05-01','Reposicion proveedor'),
(20,'Entrada',5,'2024-06-01','Reposicion proveedor'),(4,'Salida',3,'2024-07-01','Venta'),
(8,'Entrada',50,'2024-08-01','Reposicion proveedor'),(12,'Salida',20,'2024-09-01','Venta'),
(16,'Salida',2,'2024-10-01','Venta'),(19,'Entrada',20,'2024-11-01','Reposicion proveedor');

-- LOGISTICA: TRANSPORTISTAS (4)
INSERT INTO oltp_logistica.transportistas (nombre,telefono,cobertura,tipo_envio) VALUES
('SEUR Ceuta',      '+34-902-101010','Peninsula e islas','Urgente'),
('MRW Ceuta',       '+34-902-300400','Peninsula',        'Estandar'),
('Correos Express', '+34-902-197197','Nacional',         'Economico'),
('DHL Express',     '+34-902-122424','Internacional',    'Internacional');

-- LOGISTICA: ZONAS COBERTURA (8)
INSERT INTO oltp_logistica.zonas_cobertura (transportista_id,region,tiempo_estimado_dias) VALUES
(1,'Andalucia',2),(1,'Madrid',2),(1,'Cataluna',3),(1,'Canarias',4),
(2,'Andalucia',3),(2,'Madrid',4),(3,'Andalucia',5),(4,'Internacional',7);

-- LOGISTICA: ENVIOS (40)
-- Solo pedidos online tienen envio
INSERT INTO oltp_logistica.envios (pedido_id,transportista_id,fecha_envio,fecha_entrega,coste_envio,region_destino,estado) VALUES
(3,1,'2022-05-21','2022-05-23',5.90,'Cadiz','Entregado'),
(6,2,'2022-08-23','2022-08-26',4.50,'Madrid','Entregado'),
(9,1,'2022-11-06','2022-11-08',5.90,'Sevilla','Entregado'),
(12,3,'2023-01-23','2023-01-28',3.50,'Malaga','Entregado'),
(16,2,'2023-03-29','2023-04-01',4.50,'Cadiz','Entregado'),
(21,1,'2023-06-02','2023-06-04',5.90,'Barcelona','Entregado'),
(26,3,'2023-08-16','2023-08-21',3.50,'Madrid','Entregado'),
(30,2,'2023-10-16','2023-10-19',4.50,'Sevilla','Entregado'),
(34,1,'2023-12-16','2023-12-18',5.90,'Cadiz','Entregado'),
(38,3,'2024-02-22','2024-02-27',3.50,'Barcelona','Entregado'),
(42,2,'2024-04-18','2024-04-21',4.50,'Madrid','Entregado'),
(46,1,'2024-06-20','2024-06-22',5.90,'Granada','Entregado'),
(50,3,'2024-08-22','2024-08-27',3.50,'Sevilla','Entregado'),
(54,2,'2024-10-17','2024-10-20',4.50,'Cadiz','Entregado'),
(58,1,'2024-12-19','2024-12-21',5.90,'Malaga','Entregado'),
-- Pedidos presenciales con entrega a domicilio ocasional
(1,2,'2022-03-06','2022-03-09',4.50,'Ceuta','Entregado'),
(4,1,'2022-06-09','2022-06-11',5.90,'Ceuta','Entregado'),
(7,3,'2022-09-11','2022-09-16',3.50,'Ceuta','Entregado'),
(10,2,'2022-12-21','2022-12-24',4.50,'Ceuta','Entregado'),
(13,1,'2023-02-11','2023-02-13',5.90,'Ceuta','Entregado'),
(17,3,'2023-04-06','2023-04-11',3.50,'Ceuta','Entregado'),
(22,2,'2023-06-16','2023-06-19',4.50,'Ceuta','Entregado'),
(25,1,'2023-08-02','2023-08-04',5.90,'Ceuta','Entregado'),
(29,3,'2023-10-02','2023-10-07',3.50,'Ceuta','Entregado'),
(33,2,'2023-12-02','2023-12-05',4.50,'Ceuta','Entregado'),
(37,1,'2024-02-08','2024-02-10',5.90,'Ceuta','Entregado'),
(41,3,'2024-04-04','2024-04-09',3.50,'Ceuta','Entregado'),
(45,2,'2024-06-06','2024-06-09',4.50,'Ceuta','Entregado'),
(49,1,'2024-08-08','2024-08-10',5.90,'Ceuta','Entregado'),
(53,3,'2024-10-03','2024-10-08',3.50,'Ceuta','Entregado'),
(57,2,'2024-12-05','2024-12-08',4.50,'Ceuta','Entregado'),
(59,1,'2025-01-09','2025-01-11',5.90,'Cadiz','Entregado'),
(60,3,'2025-01-23','2025-01-28',3.50,'Granada','Entregado'),
(5,4,'2022-07-16','2022-07-23',12.00,'Marruecos','Entregado'),
(8,4,'2022-10-19','2022-10-26',12.00,'Marruecos','Entregado'),
(11,4,'2023-01-09','2023-01-16',12.00,'Marruecos','Entregado'),
(14,4,'2023-02-26','2023-03-05',12.00,'Marruecos','Entregado'),
(23,4,'2023-07-05','2023-07-12',12.00,'Marruecos','Entregado'),
(32,4,'2023-11-17','2023-11-24',12.00,'Marruecos','Entregado'),
(44,4,'2024-05-16','2024-05-23',12.00,'Marruecos','Entregado');

-- LOGISTICA: SEGUIMIENTO (50)
INSERT INTO oltp_logistica.seguimiento_entrega (envio_id,fecha_evento,estado,ubicacion) VALUES
(1,'2022-05-21 09:00','Recogido','Ceuta'),(1,'2022-05-22 14:00','En transito','Algeciras'),(1,'2022-05-23 10:00','Entregado','Cadiz'),
(2,'2022-08-23 10:00','Recogido','Ceuta'),(2,'2022-08-24 16:00','En transito','Sevilla'),(2,'2022-08-26 11:00','Entregado','Madrid'),
(3,'2022-11-06 09:00','Recogido','Ceuta'),(3,'2022-11-07 15:00','En transito','Malaga'),(3,'2022-11-08 10:00','Entregado','Sevilla'),
(4,'2023-01-23 10:00','Recogido','Ceuta'),(4,'2023-01-25 12:00','En transito','Malaga'),(4,'2023-01-28 09:00','Entregado','Malaga'),
(5,'2023-03-29 09:00','Recogido','Ceuta'),(5,'2023-03-30 14:00','En transito','Algeciras'),(5,'2023-04-01 11:00','Entregado','Cadiz'),
(6,'2023-06-02 10:00','Recogido','Ceuta'),(6,'2023-06-03 16:00','En transito','Madrid'),(6,'2023-06-04 10:00','Entregado','Barcelona'),
(7,'2023-08-16 09:00','Recogido','Ceuta'),(7,'2023-08-18 15:00','En transito','Sevilla'),(7,'2023-08-21 10:00','Entregado','Madrid'),
(8,'2023-10-16 10:00','Recogido','Ceuta'),(8,'2023-10-17 14:00','En transito','Malaga'),(8,'2023-10-19 11:00','Entregado','Sevilla'),
(9,'2023-12-16 09:00','Recogido','Ceuta'),(9,'2023-12-17 15:00','En transito','Algeciras'),(9,'2023-12-18 10:00','Entregado','Cadiz'),
(10,'2024-02-22 10:00','Recogido','Ceuta'),(10,'2024-02-24 12:00','En transito','Madrid'),(10,'2024-02-27 09:00','Entregado','Barcelona'),
(11,'2024-04-18 09:00','Recogido','Ceuta'),(11,'2024-04-19 15:00','En transito','Sevilla'),(11,'2024-04-21 11:00','Entregado','Madrid'),
(12,'2024-06-20 10:00','Recogido','Ceuta'),(12,'2024-06-21 16:00','En transito','Malaga'),(12,'2024-06-22 10:00','Entregado','Granada'),
(13,'2024-08-22 09:00','Recogido','Ceuta'),(13,'2024-08-24 15:00','En transito','Sevilla'),(13,'2024-08-27 10:00','Entregado','Sevilla'),
(14,'2024-10-17 10:00','Recogido','Ceuta'),(14,'2024-10-18 14:00','En transito','Algeciras'),(14,'2024-10-20 11:00','Entregado','Cadiz'),
(15,'2024-12-19 09:00','Recogido','Ceuta'),(15,'2024-12-20 15:00','En transito','Malaga'),(15,'2024-12-21 10:00','Entregado','Malaga'),
(16,'2022-03-06 10:00','Recogido','Ceuta'),(16,'2022-03-08 14:00','En transito','Ceuta'),(16,'2022-03-09 11:00','Entregado','Ceuta'),
(17,'2022-06-09 09:00','Recogido','Ceuta'),(17,'2022-06-10 16:00','En transito','Ceuta'),(17,'2022-06-11 10:00','Entregado','Ceuta');

-- FINANZAS: FACTURAS (60)
INSERT INTO oltp_finanzas.facturas (pedido_id,cliente_id,fecha,importe_total,estado)
SELECT p.pedido_id, p.cliente_id, p.fecha_pedido,
       SUM(d.cantidad * d.precio_unitario * (1 - d.descuento/100)), 'Pagada'
FROM oltp_ventas.pedidos p
JOIN oltp_ventas.detalle_pedido d ON p.pedido_id = d.pedido_id
GROUP BY p.pedido_id, p.cliente_id, p.fecha_pedido;

-- FINANZAS: LINEAS FACTURA
INSERT INTO oltp_finanzas.lineas_factura (factura_id,producto_id,cantidad,precio_unitario,importe)
SELECT f.factura_id, d.producto_id, d.cantidad, d.precio_unitario,
       d.cantidad * d.precio_unitario * (1 - d.descuento/100)
FROM oltp_finanzas.facturas f
JOIN oltp_ventas.detalle_pedido d ON f.pedido_id = d.pedido_id;

-- FINANZAS: PAGOS
INSERT INTO oltp_finanzas.pagos (factura_id,fecha_pago,importe,metodo)
SELECT factura_id, fecha + INTERVAL '1 day',
       importe_total,
       CASE WHEN (factura_id % 3) = 0 THEN 'Efectivo'
            WHEN (factura_id % 3) = 1 THEN 'Tarjeta'
            ELSE 'Bizum' END
FROM oltp_finanzas.facturas;

-- FINANZAS: ANALISIS COSTE
INSERT INTO oltp_finanzas.analisis_coste (producto_id,mes,anio,coste_total,ingreso_total,margen)
SELECT d.producto_id,
       EXTRACT(MONTH FROM p.fecha_pedido)::INT,
       EXTRACT(YEAR  FROM p.fecha_pedido)::INT,
       SUM(d.cantidad * cp.coste_unitario),
       SUM(d.cantidad * d.precio_unitario),
       SUM(d.cantidad * (d.precio_unitario - cp.coste_unitario))
FROM oltp_ventas.detalle_pedido d
JOIN oltp_ventas.pedidos p ON d.pedido_id = p.pedido_id
JOIN oltp_inventario.costes_producto cp ON d.producto_id = cp.producto_id
GROUP BY d.producto_id, EXTRACT(MONTH FROM p.fecha_pedido), EXTRACT(YEAR FROM p.fecha_pedido);

-- PASO 4: CREAR STAR SCHEMA (olap_star)
CREATE SCHEMA olap_star;

CREATE TABLE olap_star.dim_tiempo (
    tiempo_sk  SERIAL PRIMARY KEY,
    fecha      DATE UNIQUE NOT NULL,
    anio       INT,
    trimestre  INT,
    mes        INT,
    dia_semana INT,
    temporada  VARCHAR(20)
);

INSERT INTO olap_star.dim_tiempo (fecha,anio,trimestre,mes,dia_semana,temporada)
SELECT d::date,
       EXTRACT(YEAR FROM d),
       EXTRACT(QUARTER FROM d),
       EXTRACT(MONTH FROM d),
       EXTRACT(DOW FROM d),
       CASE
         WHEN EXTRACT(MONTH FROM d) IN (12,1,2) THEN 'Invierno'
         WHEN EXTRACT(MONTH FROM d) IN (3,4,5)  THEN 'Primavera'
         WHEN EXTRACT(MONTH FROM d) IN (6,7,8)  THEN 'Verano'
         ELSE 'Otono'
       END
FROM generate_series('2022-01-01'::date,'2025-12-31'::date,'1 day') d;

CREATE TABLE olap_star.dim_cliente (
    cliente_sk   SERIAL PRIMARY KEY,
    cliente_id   INT NOT NULL,
    nombre       VARCHAR(150),
    tipo_cliente VARCHAR(20),
    canal        VARCHAR(20),
    ciudad       VARCHAR(80),
    region       VARCHAR(80)
);

INSERT INTO olap_star.dim_cliente (cliente_id,nombre,tipo_cliente,canal,ciudad,region)
SELECT cliente_id,nombre,tipo_cliente,canal,ciudad,region FROM oltp_ventas.clientes;

CREATE TABLE olap_star.dim_producto (
    producto_sk    SERIAL PRIMARY KEY,
    producto_id    INT NOT NULL,
    nombre         VARCHAR(150),
    subcategoria   VARCHAR(100),
    categoria      VARCHAR(100),
    marca          VARCHAR(100),
    precio_lista   NUMERIC(10,2),
    coste_unitario NUMERIC(10,2),
    proveedor      VARCHAR(150)
);

INSERT INTO olap_star.dim_producto
  (producto_id,nombre,subcategoria,categoria,marca,precio_lista,coste_unitario,proveedor)
SELECT p.producto_id, p.nombre, 'N/A', c.nombre, p.marca,
       p.precio_venta, COALESCE(cp.coste_unitario,0),
       COALESCE(pv.nombre,'Desconocido')
FROM oltp_ventas.productos p
JOIN oltp_ventas.categorias c ON p.categoria_id = c.categoria_id
LEFT JOIN oltp_inventario.costes_producto cp ON p.producto_id = cp.producto_id
LEFT JOIN oltp_inventario.productos_inv   pi ON p.producto_id = pi.producto_id
LEFT JOIN oltp_inventario.proveedores     pv ON pi.proveedor_id = pv.proveedor_id;

CREATE TABLE olap_star.dim_empleado (
    empleado_sk     SERIAL PRIMARY KEY,
    empleado_id     INT NOT NULL,
    nombre          VARCHAR(150),
    cargo           VARCHAR(80),
    tienda_asignada VARCHAR(100),
    turno           VARCHAR(50)
);

INSERT INTO olap_star.dim_empleado (empleado_id,nombre,cargo,tienda_asignada,turno)
SELECT e.empleado_id, e.nombre, e.cargo, t.nombre, 'Variable'
FROM oltp_rrhh.empleados e
LEFT JOIN oltp_rrhh.tiendas t ON e.tienda_id = t.tienda_id;

CREATE TABLE olap_star.dim_tienda (
    tienda_sk SERIAL PRIMARY KEY,
    tienda_id INT NOT NULL,
    nombre    VARCHAR(100),
    zona      VARCHAR(80),
    direccion VARCHAR(200)
);

INSERT INTO olap_star.dim_tienda (tienda_id,nombre,zona,direccion)
SELECT tienda_id,nombre,zona,direccion FROM oltp_rrhh.tiendas;

CREATE TABLE olap_star.dim_transportista (
    transportista_sk SERIAL PRIMARY KEY,
    transportista_id INT NOT NULL,
    nombre           VARCHAR(150),
    cobertura        VARCHAR(100),
    tipo_envio       VARCHAR(50)
);

INSERT INTO olap_star.dim_transportista (transportista_id,nombre,cobertura,tipo_envio)
SELECT transportista_id,nombre,cobertura,tipo_envio FROM oltp_logistica.transportistas;

CREATE TABLE olap_star.dim_proveedor (
    proveedor_sk    SERIAL PRIMARY KEY,
    proveedor_id    INT NOT NULL,
    nombre          VARCHAR(150),
    pais            VARCHAR(80),
    tipo_suministro VARCHAR(50)
);

INSERT INTO olap_star.dim_proveedor (proveedor_id,nombre,pais,tipo_suministro)
SELECT proveedor_id,nombre,pais,tipo_suministro FROM oltp_inventario.proveedores;

-- FACT_VENTAS
CREATE TABLE olap_star.fact_ventas (
    venta_sk        SERIAL PRIMARY KEY,
    tiempo_sk       INT REFERENCES olap_star.dim_tiempo(tiempo_sk),
    cliente_sk      INT REFERENCES olap_star.dim_cliente(cliente_sk),
    producto_sk     INT REFERENCES olap_star.dim_producto(producto_sk),
    empleado_sk     INT REFERENCES olap_star.dim_empleado(empleado_sk),
    tienda_sk       INT REFERENCES olap_star.dim_tienda(tienda_sk),
    cantidad        INT,
    precio_unitario NUMERIC(10,2),
    descuento       NUMERIC(5,2),
    ingresos        NUMERIC(12,2),
    coste           NUMERIC(12,2),
    margen          NUMERIC(12,2)
);

INSERT INTO olap_star.fact_ventas
  (tiempo_sk,cliente_sk,producto_sk,empleado_sk,tienda_sk,
   cantidad,precio_unitario,descuento,ingresos,coste,margen)
SELECT
  dt.tiempo_sk, dc.cliente_sk, dp.producto_sk, de.empleado_sk, dti.tienda_sk,
  d.cantidad, d.precio_unitario, d.descuento,
  d.cantidad * d.precio_unitario * (1 - COALESCE(d.descuento,0)/100),
  d.cantidad * COALESCE(dp.coste_unitario,0),
  (d.cantidad * d.precio_unitario * (1 - COALESCE(d.descuento,0)/100))
  - (d.cantidad * COALESCE(dp.coste_unitario,0))
FROM oltp_ventas.detalle_pedido d
JOIN oltp_ventas.pedidos p       ON d.pedido_id   = p.pedido_id
JOIN olap_star.dim_tiempo   dt   ON dt.fecha       = p.fecha_pedido
JOIN olap_star.dim_cliente  dc   ON dc.cliente_id  = p.cliente_id
JOIN olap_star.dim_producto dp   ON dp.producto_id = d.producto_id
JOIN olap_star.dim_empleado de   ON de.empleado_id = p.empleado_id
JOIN olap_star.dim_tienda   dti  ON dti.tienda_id  = p.tienda_id;

-- FACT_ENVIOS
CREATE TABLE olap_star.fact_envios (
    envio_sk         SERIAL PRIMARY KEY,
    tiempo_sk        INT REFERENCES olap_star.dim_tiempo(tiempo_sk),
    cliente_sk       INT REFERENCES olap_star.dim_cliente(cliente_sk),
    tienda_sk        INT REFERENCES olap_star.dim_tienda(tienda_sk),
    transportista_sk INT REFERENCES olap_star.dim_transportista(transportista_sk),
    coste_envio      NUMERIC(10,2),
    dias_entrega     INT,
    region_destino   VARCHAR(100)
);

INSERT INTO olap_star.fact_envios
  (tiempo_sk,cliente_sk,tienda_sk,transportista_sk,coste_envio,dias_entrega,region_destino)
SELECT dt.tiempo_sk, dc.cliente_sk, dti.tienda_sk, dtr.transportista_sk,
       e.coste_envio,
       (e.fecha_entrega - e.fecha_envio),
       e.region_destino
FROM oltp_logistica.envios e
JOIN oltp_ventas.pedidos p           ON e.pedido_id        = p.pedido_id
JOIN olap_star.dim_tiempo       dt   ON dt.fecha            = e.fecha_envio
JOIN olap_star.dim_cliente      dc   ON dc.cliente_id       = p.cliente_id
JOIN olap_star.dim_tienda       dti  ON dti.tienda_id       = p.tienda_id
JOIN olap_star.dim_transportista dtr ON dtr.transportista_id= e.transportista_id;

-- FACT_INVENTARIO
CREATE TABLE olap_star.fact_inventario (
    inventario_sk SERIAL PRIMARY KEY,
    tiempo_sk     INT REFERENCES olap_star.dim_tiempo(tiempo_sk),
    producto_sk   INT REFERENCES olap_star.dim_producto(producto_sk),
    tienda_sk     INT REFERENCES olap_star.dim_tienda(tienda_sk),
    proveedor_sk  INT REFERENCES olap_star.dim_proveedor(proveedor_sk),
    entradas      INT DEFAULT 0,
    salidas       INT DEFAULT 0,
    stock_final   INT
);

INSERT INTO olap_star.fact_inventario
  (tiempo_sk,producto_sk,tienda_sk,proveedor_sk,entradas,salidas,stock_final)
SELECT dt.tiempo_sk, dp.producto_sk,
       1 AS tienda_sk,
       dpv.proveedor_sk,
       SUM(CASE WHEN m.tipo='Entrada' THEN m.cantidad ELSE 0 END),
       SUM(CASE WHEN m.tipo='Salida'  THEN m.cantidad ELSE 0 END),
       SUM(CASE WHEN m.tipo='Entrada' THEN m.cantidad ELSE -m.cantidad END)
FROM oltp_inventario.movimientos_stock m
JOIN olap_star.dim_tiempo   dt  ON dt.fecha        = m.fecha
JOIN olap_star.dim_producto dp  ON dp.producto_id  = m.producto_id
JOIN oltp_inventario.productos_inv pi ON m.producto_id = pi.producto_id
JOIN olap_star.dim_proveedor dpv ON dpv.proveedor_id = pi.proveedor_id
GROUP BY dt.tiempo_sk, dp.producto_sk, dpv.proveedor_sk;

-- PASO 5: CREAR SNOWFLAKE SCHEMA (olap_snow)
CREATE SCHEMA olap_snow;

CREATE TABLE olap_snow.dim_categoria (
    categoria_sk SERIAL PRIMARY KEY,
    nombre       VARCHAR(100) NOT NULL
);

INSERT INTO olap_snow.dim_categoria (nombre)
SELECT DISTINCT categoria FROM olap_star.dim_producto ORDER BY categoria;

CREATE TABLE olap_snow.dim_producto (
    producto_sk    SERIAL PRIMARY KEY,
    producto_id    INT NOT NULL,
    nombre         VARCHAR(150),
    categoria_sk   INT REFERENCES olap_snow.dim_categoria(categoria_sk),
    marca          VARCHAR(100),
    precio_lista   NUMERIC(10,2),
    coste_unitario NUMERIC(10,2)
);

INSERT INTO olap_snow.dim_producto (producto_id,nombre,categoria_sk,marca,precio_lista,coste_unitario)
SELECT sp.producto_id, sp.nombre, sc.categoria_sk, sp.marca, sp.precio_lista, sp.coste_unitario
FROM olap_star.dim_producto sp
JOIN olap_snow.dim_categoria sc ON sc.nombre = sp.categoria;

CREATE TABLE olap_snow.fact_ventas (
    venta_sk        SERIAL PRIMARY KEY,
    tiempo_sk       INT,
    cliente_sk      INT,
    producto_sk     INT REFERENCES olap_snow.dim_producto(producto_sk),
    empleado_sk     INT,
    tienda_sk       INT,
    cantidad        INT,
    precio_unitario NUMERIC(10,2),
    descuento       NUMERIC(5,2),
    ingresos        NUMERIC(12,2),
    coste           NUMERIC(12,2),
    margen          NUMERIC(12,2)
);

INSERT INTO olap_snow.fact_ventas
  (tiempo_sk,cliente_sk,producto_sk,empleado_sk,tienda_sk,
   cantidad,precio_unitario,descuento,ingresos,coste,margen)
SELECT fv.tiempo_sk, fv.cliente_sk, dp_snow.producto_sk,
       fv.empleado_sk, fv.tienda_sk,
       fv.cantidad, fv.precio_unitario, fv.descuento,
       fv.ingresos, fv.coste, fv.margen
FROM olap_star.fact_ventas fv
JOIN olap_star.dim_producto dp_star ON fv.producto_sk = dp_star.producto_sk
JOIN olap_snow.dim_producto dp_snow ON dp_snow.producto_id = dp_star.producto_id;

-- Verificacion rapida: ejecutar estas lineas para comprobar
SELECT 'OLTP clientes'      AS tabla, COUNT(*) FROM oltp_ventas.clientes
UNION ALL
SELECT 'OLTP pedidos',       COUNT(*) FROM oltp_ventas.pedidos
UNION ALL
SELECT 'OLTP detalle',       COUNT(*) FROM oltp_ventas.detalle_pedido
UNION ALL
SELECT 'OLTP empleados',     COUNT(*) FROM oltp_rrhh.empleados
UNION ALL
SELECT 'OLTP inventario mv', COUNT(*) FROM oltp_inventario.movimientos_stock
UNION ALL
SELECT 'OLTP envios',        COUNT(*) FROM oltp_logistica.envios
UNION ALL
SELECT 'OLTP facturas',      COUNT(*) FROM oltp_finanzas.facturas
UNION ALL
SELECT 'DW fact_ventas',     COUNT(*) FROM olap_star.fact_ventas
UNION ALL
SELECT 'DW fact_envios',     COUNT(*) FROM olap_star.fact_envios
UNION ALL
SELECT 'DW fact_inventario', COUNT(*) FROM olap_star.fact_inventario
UNION ALL
SELECT 'Snow fact_ventas',   COUNT(*) FROM olap_snow.fact_ventas;
