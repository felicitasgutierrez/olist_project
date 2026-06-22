-- ============================================================
-- DWH - Olist Analytics Dashboard
-- Ingeniería de Software 2026
-- ============================================================

-- Tabla de staging: detalle limpio, una fila por orden
CREATE TABLE IF NOT EXISTS olist_orders_clean (
    order_id                        VARCHAR PRIMARY KEY,
    customer_id                     VARCHAR NOT NULL,
    customer_unique_id              VARCHAR,
    order_status                    VARCHAR,
    order_purchase_date             TIMESTAMP,
    order_delivered_date            TIMESTAMP,
    order_estimated_delivery_date   TIMESTAMP,
    entregado_a_tiempo              BOOLEAN,
    customer_state                  VARCHAR(2),
    customer_city                   VARCHAR,
    precio_total                    NUMERIC(12, 2),
    flete_total                     NUMERIC(12, 2),
    ratio_flete                     NUMERIC(8, 4),
    review_score                    NUMERIC(3, 1),
    categoria_producto              VARCHAR
);

-- ============================================================
-- Tablas agregadas para el dashboard
-- ============================================================

-- KPIs generales por mes
CREATE TABLE IF NOT EXISTS kpis_por_mes (
    mes                         DATE PRIMARY KEY,
    gmv_total                   NUMERIC(14, 2),
    ticket_promedio             NUMERIC(12, 2),
    ticket_mediana              NUMERIC(12, 2),
    pct_entregas_a_tiempo       NUMERIC(5, 2),
    score_promedio_reviews      NUMERIC(3, 2),
    tasa_retencion              NUMERIC(5, 2),
    ratio_flete_gmv             NUMERIC(5, 2),
    cantidad_ordenes            INTEGER
);

-- Métricas por categoría de producto por mes
CREATE TABLE IF NOT EXISTS metricas_por_categoria (
    mes                 DATE,
    categoria           VARCHAR,
    gmv_total           NUMERIC(14, 2),
    cantidad_ordenes    INTEGER,
    ticket_promedio     NUMERIC(12, 2),
    PRIMARY KEY (mes, categoria)
);

-- Métricas por estado geográfico por mes
-- El campo estado usa las siglas de 2 letras de Brasil (SP, RJ, MG, etc.)
-- que Metabase reconoce automáticamente para el mapa
CREATE TABLE IF NOT EXISTS metricas_por_estado (
    mes                         DATE,
    estado                      VARCHAR(2),
    gmv_total                   NUMERIC(14, 2),
    cantidad_ordenes            INTEGER,
    pct_entregas_a_tiempo       NUMERIC(5, 2),
    score_promedio_reviews      NUMERIC(3, 2),
    PRIMARY KEY (mes, estado)
);

-- ============================================================
-- Tablas de traducción para filtros del dashboard
-- ============================================================

-- Traducción de categorías de producto (inglés → español)
CREATE TABLE IF NOT EXISTS categorias_traduccion (
    categoria_en    TEXT PRIMARY KEY,
    categoria_es    TEXT NOT NULL
);

INSERT INTO categorias_traduccion (categoria_en, categoria_es) VALUES
    ('auto',                                    'Automóvil'),
    ('bed_bath_table',                          'Cama, Baño y Mesa'),
    ('books_general_interest',                  'Libros de Interés General'),
    ('books_imported',                          'Libros Importados'),
    ('books_technical',                         'Libros Técnicos'),
    ('christmas_supplies',                      'Artículos de Navidad'),
    ('computers',                               'Computadoras'),
    ('computers_accessories',                   'Accesorios de Computadora'),
    ('consoles_games',                          'Consolas y Videojuegos'),
    ('construction_tools_construction',         'Herramientas de Construcción'),
    ('construction_tools_lights',               'Iluminación para Construcción'),
    ('construction_tools_safety',               'Seguridad en Construcción'),
    ('cool_stuff',                              'Artículos Varios'),
    ('diapers_and_hygiene',                     'Pañales e Higiene'),
    ('drinks',                                  'Bebidas'),
    ('electronics',                             'Electrónica'),
    ('fashion_bags_accessories',                'Bolsos y Accesorios de Moda'),
    ('fashion_childrens_clothes',               'Ropa Infantil'),
    ('fashion_male_clothing',                   'Ropa Masculina'),
    ('fashion_shoes',                           'Calzado'),
    ('fashion_underwear_beach',                 'Ropa Interior y Playa'),
    ('fixed_telephony',                         'Telefonía Fija'),
    ('flowers',                                 'Flores'),
    ('food',                                    'Alimentos'),
    ('food_drinks',                             'Alimentos y Bebidas'),
    ('furniture_bedroom',                       'Muebles de Dormitorio'),
    ('furniture_decor',                         'Muebles y Decoración'),
    ('furniture_living_room',                   'Muebles de Living'),
    ('garden_tools',                            'Herramientas de Jardín'),
    ('health_beauty',                           'Salud y Belleza'),
    ('home_appliances',                         'Electrodomésticos'),
    ('home_appliances_2',                       'Electrodomésticos 2'),
    ('home_comfort',                            'Confort del Hogar'),
    ('home_comfort_2',                          'Confort del Hogar 2'),
    ('home_construction',                       'Construcción del Hogar'),
    ('housewares',                              'Artículos del Hogar'),
    ('industry_commerce_and_business',          'Industria y Comercio'),
    ('kitchen_dining_laundry_garden_furniture', 'Cocina, Comedor y Jardín'),
    ('la_cuisine',                              'Cocina Gourmet'),
    ('luggage_accessories',                     'Equipaje y Accesorios'),
    ('market_place',                            'Marketplace'),
    ('musical_instruments',                     'Instrumentos Musicales'),
    ('music',                                   'Música'),
    ('office_furniture',                        'Muebles de Oficina'),
    ('party_supplies',                          'Artículos de Fiesta'),
    ('perfumery',                               'Perfumería'),
    ('pet_shop',                                'Mascotas'),
    ('phones',                                  'Teléfonos'),
    ('portable_kitchen_food_processors',        'Electrodomésticos de Cocina'),
    ('security_and_services',                   'Seguridad y Servicios'),
    ('signaling_and_security',                  'Señalización y Seguridad'),
    ('small_appliances',                        'Pequeños Electrodomésticos'),
    ('small_appliances_home_oven_and_coffee',   'Hornos y Cafeteras'),
    ('sports_leisure',                          'Deportes y Ocio'),
    ('stationery',                              'Papelería'),
    ('tablets_printing_image',                  'Tablets e Impresión'),
    ('telephony',                               'Telefonía'),
    ('toys',                                    'Juguetes'),
    ('watches_gifts',                           'Relojes y Regalos')
ON CONFLICT (categoria_en) DO NOTHING;

-- Traducción de estados de Brasil (sigla → nombre completo)
CREATE TABLE IF NOT EXISTS estados_traduccion (
    estado_sigla    VARCHAR(2) PRIMARY KEY,
    estado_nombre   TEXT NOT NULL
);

INSERT INTO estados_traduccion (estado_sigla, estado_nombre) VALUES
    ('AC', 'Acre'),
    ('AL', 'Alagoas'),
    ('AM', 'Amazonas'),
    ('AP', 'Amapá'),
    ('BA', 'Bahía'),
    ('CE', 'Ceará'),
    ('DF', 'Distrito Federal'),
    ('ES', 'Espírito Santo'),
    ('GO', 'Goiás'),
    ('MA', 'Maranhão'),
    ('MG', 'Minas Gerais'),
    ('MS', 'Mato Grosso do Sul'),
    ('MT', 'Mato Grosso'),
    ('PA', 'Pará'),
    ('PB', 'Paraíba'),
    ('PE', 'Pernambuco'),
    ('PI', 'Piauí'),
    ('PR', 'Paraná'),
    ('RJ', 'Río de Janeiro'),
    ('RN', 'Río Grande do Norte'),
    ('RO', 'Rondônia'),
    ('RR', 'Roraima'),
    ('RS', 'Río Grande do Sul'),
    ('SC', 'Santa Catarina'),
    ('SE', 'Sergipe'),
    ('SP', 'São Paulo'),
    ('TO', 'Tocantins')
ON CONFLICT (estado_sigla) DO NOTHING;
