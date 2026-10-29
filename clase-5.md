# Guion de clase — Clase 5: Data Engineering

Oct 28, 2026 · @Franco Guazzetti

## Índice de temas

La clase tiene cinco bloques y dura unas 2 horas, con un break a mitad; la práctica va después de todo el contenido teórico. Los minutos son una estimación para 120 min: ajustalos a tu comisión.

| # | Tema | Diapositivas | Min |
| --- | --- | --- | --- |
| 0 | Apertura: objetivos, agenda y grabación | 1–4 | 5 |
| 1 | Iceberg: de data lake a lakehouse | 5–12 | 18 |
| 2 | ACID, time travel y gobernanza del catálogo | 13–19 | 22 |
| — | Break | 20 | 10 |
| 3 | Layout físico: Glue Streaming, particionado y compactación | 21–28 | 22 |
| 4 | Práctica en vivo: la capa de almacenamiento | 29–35 | 30 |
| 5 | Pre-entrega 5 y cómo sigue el proyecto | 36–40 | 8 |
| — | Preguntas y cierre | 41–42 | 5 |

## Objetivos de la clase

Al terminar el Módulo 5, el grupo tiene que poder construir la capa de almacenamiento de un lakehouse: una tabla Iceberg en Glue alimentada por Flink.

- Diferenciar un data lake basado en Hive de un lakehouse basado en Iceberg.
- Explicar la jerarquía de metadatos de Iceberg y el rol del Glue Data Catalog.
- Aplicar time travel, rollback y evolución de esquemas sobre tablas Iceberg.
- Diseñar particionado y compactación para evitar el small files problem.
- Integrar la salida de Flink con Iceberg mediante IcebergSink y checkpointing.

Hilo conductor para decirlo en voz alta: **"S3 es la caja, Iceberg la organiza"**. Todo el módulo vuelve a esta idea.

## Guion de la clase

Cada bloque sigue el orden de las diapositivas y los minutos suman los del índice. Bajo cada diapositiva de contenido está su vocabulario técnico. Las frases entre comillas son sugeridas: adaptalas a tu voz.

### Apertura (5 min · diap. 1–4)

- **Diap. 1–2 · Título y objetivos (2 min).** Presentá el Módulo 5 y leé los cinco objetivos. Anticipá que la práctica de hoy es la Pre-entrega 5.
- **Diap. 3 · Agenda (1 min).** Tres bloques de contenido, la práctica en vivo y la pre-entrega. Avisá que el break llega después de ACID y catálogo.
- **Diap. 4 · Grabación (1 min).** Avisá que la clase se graba e iniciá la grabación.
- **Gancho (1 min).** "¿Quién tuvo alguna vez una consulta que tardaba más en arrancar que en ejecutarse?" Eso es lo que Iceberg viene a resolver.

### Bloque 1 · Iceberg: de data lake a lakehouse (18 min · diap. 5–12)

La diap. 5 es la portada del bloque: pasá directo a la 6.

- **Diap. 6 · Los límites del data lake (3 min).** En un data lake con layout Hive, la tabla es un prefijo de S3 con las particiones en la ruta (`s3://bucket/eventos/year=2024/month=05/`) y el Hive Metastore registra solo ese directorio. Para planificar una consulta, el motor ejecuta `ListObjectsV2` sobre cada prefijo, y esa API devuelve hasta 1.000 objetos por llamada: con millones de archivos son miles de requests antes de leer un solo byte. Además, una escritura es un conjunto de PUT independientes: si el job falla a mitad quedan archivos parciales visibles y no hay rollback. Y un lector que ya listó los archivos recibe un 404 `NoSuchKey` (el "File Not Found") si un escritor los borra o los compacta mientras tanto. Pregunta: "¿Alguien vio un File Not Found en un pipeline?"
  - **Vocabulario técnico**
  - **Layout Hive:** convención de directorios `clave=valor` donde la partición forma parte de la ruta del objeto.
  - **Hive Metastore:** catálogo que asocia una tabla con un directorio y sus particiones; no conoce los archivos individuales.
  - **`ListObjectsV2`:** API de S3 que lista los objetos de un prefijo, con un máximo de 1.000 por llamada.
  - **Atomicidad:** una operación se aplica completa o no se aplica.
  - **Aislamiento:** una lectura no observa los efectos parciales de escrituras concurrentes.
- **Diap. 7 · Formato de tabla (3 min).** Hay que separar dos niveles. El formato de archivo (Parquet, ORC) define cómo se codifican los bytes: Parquet guarda los valores columna por columna, en row groups con un footer de estadísticas. El formato de tabla (Iceberg) es una especificación de metadatos que define qué archivos componen la tabla en cada momento, con qué esquema y con qué partición. Como la lista de archivos vive en los metadatos y no en el listado de un directorio, Iceberg rastrea archivos y no carpetas: esa es la diferencia con el Hive Metastore. Cerrá con la definición de lakehouse: datos abiertos en S3, gobernanza en la capa de metadatos y varios motores (BI, SQL, ML, streaming) leyendo la misma copia.
  - **Vocabulario técnico**
  - **Formato de archivo:** codificación de los registros en disco (Parquet, ORC, Avro).
  - **Formato de tabla:** especificación de metadatos que define archivos, esquema, partición e historial de una tabla (Iceberg, Delta Lake, Hudi).
  - **Formato columnar:** almacena los valores por columna, así el motor lee solo las columnas que la consulta pide.
  - **Row group:** bloque de filas de un archivo Parquet, guardado columna por columna y con estadísticas propias.
  - **Formato abierto:** especificación pública que implementan varios motores (Athena, Flink, Spark, Trino).
  - **Lakehouse:** arquitectura que combina el almacenamiento abierto de un data lake con las garantías transaccionales de un data warehouse.
- **Diap. 8 · Las tres capas (3 min).** Leer una tabla recorre las capas en orden: el catálogo apunta al metadata file vigente; este apunta a la manifest list del snapshot actual; esta apunta a los manifest files; y estos listan los data files en Parquet, ORC o Avro. En Glue, el puntero del catálogo es el parámetro `metadata_location` de la tabla. Todos los archivos de metadatos y de datos son inmutables: un cambio crea archivos nuevos y mueve el puntero. Dibujalas en la pizarra: se reutilizan en las diap. 10 y 11.
  - **Vocabulario técnico**
  - **Catálogo:** servicio que guarda, por tabla, la ubicación del metadata file vigente. En AWS, el Glue Data Catalog.
  - **`metadata_location`:** parámetro de la tabla en Glue con la ruta S3 del metadata file vigente.
  - **Data file:** archivo Parquet, ORC o Avro que contiene filas de la tabla.
  - **Inmutabilidad:** ningún archivo se edita; cada cambio escribe archivos nuevos y reemplaza el puntero.
  - **`GlueCatalog`:** implementación de catálogo de Iceberg que usa el Glue Data Catalog.
- **Diap. 9 · Momentos de la clase (2 min).** Para recordar: el tiempo de una consulta se divide en planificación (decidir qué archivos leer) y ejecución (leerlos y procesarlos). En un data lake grande domina la planificación, porque exige listar prefijos y abrir footers; Iceberg la reduce a leer unos pocos archivos de metadatos. Para pensar: en Athena se puede comparar el tiempo de planificación con el de ejecución en las estadísticas de la consulta, y así saber si un dashboard lento sufre por volumen o por planificación. Pedí respuestas por chat durante 1 min.
  - **Vocabulario técnico**
  - **Planificación de consulta (query planning):** etapa en la que el motor decide qué archivos leer; ocurre antes de ejecutar.
  - **Ejecución:** lectura y procesamiento de los archivos elegidos en la planificación.
  - **Footer de Parquet:** sección final del archivo con el esquema y las estadísticas de cada row group; el lector lo abre antes de leer datos.
- **Diap. 10 · La jerarquía de metadatos (3 min).** El metadata file (`.metadata.json`) guarda el esquema actual y los anteriores, la partition spec, las propiedades de la tabla, la lista de snapshots y el `current-snapshot-id`. La manifest list (Avro) es una por snapshot: por cada manifest file registra su ruta, cuántos archivos agregó o eliminó y el rango de valores de las columnas de partición. Cada manifest file (Avro) lista data files con su ruta, su tupla de partición, su conteo de filas y, por columna, el valor mínimo, el máximo y el conteo de nulos. Con eso el planificador compara el WHERE contra los metadatos: primero descarta manifests por partición y después data files por mínimo y máximo, sin abrir ninguno.
  - **Vocabulario técnico**
  - **Snapshot:** estado completo de la tabla (lista de data files) tras un commit.
  - **`current-snapshot-id`:** campo del metadata file que identifica el snapshot vigente.
  - **Partition spec:** definición de cómo se calcula la partición de cada fila a partir de las columnas de la tabla.
  - **Estadísticas por columna:** mínimo, máximo, conteo de nulos y de valores, guardados por data file en los manifest files.
  - **File pruning:** descartar data files sin abrirlos, comparando el predicado del WHERE con esas estadísticas.
  - **Avro:** formato de serialización con esquema embebido, usado para manifests y manifest lists.
- **Diap. 11 · El commit atómico (3 min).** Ejemplo: Flink escribiendo cada 30 segundos. Con el sink de Iceberg el commit ocurre al completarse cada checkpoint, así que "cada 30 segundos" equivale a un intervalo de checkpoint de 30 s. Cinco pasos: se escriben los Parquet nuevos, se crea un manifest file que los referencia, se crea una manifest list que suma ese manifest a los anteriores, se escribe un metadata file nuevo y el catálogo reemplaza `metadata_location` con una operación compare-and-swap. Hasta ese último paso, los archivos nuevos no son visibles para ningún lector; si el job falla antes, quedan como archivos huérfanos que se limpian después. Remarcá que el analista que consulta en Athena sigue leyendo el snapshot anterior y no ve datos parciales.
  - **Vocabulario técnico**
  - **Commit:** operación que publica un snapshot nuevo reemplazando el puntero del catálogo.
  - **Compare-and-swap (CAS):** reemplaza un valor solo si todavía es el que el escritor tomó como base; es lo que hace atómico el commit.
  - **Checkpoint de Flink:** punto de guardado periódico del estado del job; el IcebergSink confirma el commit al completarse.
  - **Archivo huérfano:** archivo escrito que ningún snapshot referencia, por ejemplo tras un commit fallido. Iceberg lo elimina con la acción `remove_orphan_files`.
- **Diap. 12 · Cierre del bloque (1 min).** "S3 es la caja, Iceberg la organiza." S3 aporta almacenamiento de objetos inmutables, sin transacciones; Parquet aporta la codificación columnar; Iceberg agrega la capa de metadatos que convierte un conjunto de archivos en una tabla transaccional.
  - **Vocabulario técnico**
  - **Almacenamiento de objetos:** modelo clave-valor de objetos inmutables, sin renombrado atómico ni transacciones (S3).
  - **Tabla transaccional:** tabla cuyos cambios se publican de forma atómica y se leen con aislamiento.

### Bloque 2 · ACID, time travel y gobernanza del catálogo (22 min · diap. 13–19)

La diap. 13 es la portada del bloque: pasá directo a la 14.

- **Diap. 14 · Snapshots y aislamiento (4 min).** Un snapshot es la lista completa de data files que forman la tabla tras un commit, referenciada por una manifest list. Cada uno tiene un `snapshot-id`, un `parent-snapshot-id` que encadena el historial, un `timestamp-ms` y una operación (`append`, `overwrite`, `delete` o `replace`). Con snapshot isolation, un lector fija el snapshot vigente cuando abre la tabla y planifica toda la consulta sobre él, aunque lleguen commits nuevos. Para streaming: un job de Glue o Flink que lee la tabla nunca ve archivos del micro-batch que está aterrizando, solo snapshots confirmados.
  - **Vocabulario técnico**
  - **`snapshot-id` / `parent-snapshot-id`:** identificadores que encadenan los snapshots en un historial lineal.
  - **Operación del snapshot:** tipo de cambio que produjo el commit. `append` agrega archivos; `replace` los reescribe sin cambiar los datos (compactación).
  - **Snapshot isolation:** nivel de aislamiento en el que cada lectura ve la tabla tal como estaba en un único snapshot.
  - **Lectura consistente:** una consulta no mezcla archivos de snapshots distintos.
- **Diap. 15 · Concurrencia optimista (4 min).** Un escritor parte del snapshot base N, escribe sus archivos sin tomar locks y, al commit, le pide al catálogo reemplazar el puntero de N a N+1 con compare-and-swap. Si otro escritor ya movió el puntero a N+1, el CAS falla y el escritor reintenta (la cantidad de reintentos se configura con `commit.retry.num-retries`): revalida sus cambios contra el snapshot nuevo y, si no son incompatibles (dos `append`, por ejemplo), los reaplica sin reescribir datos; si hay un conflicto real, aborta con una excepción. Condición: un catálogo que soporte CAS atómico; sin eso no hay ACID. Conectalo con la práctica: por eso Glue es el catálogo.
  - **Vocabulario técnico**
  - **Concurrencia optimista (OCC):** los escritores no se bloquean; el conflicto se detecta al commit. Se opone al control pesimista, que toma locks antes de escribir.
  - **Compare-and-swap:** cambia el puntero solo si sigue en el valor que el escritor leyó.
  - **Conflicto de commit:** dos escritores parten del mismo snapshot base y el segundo no puede reaplicar sus cambios sobre el nuevo.
  - **Reintento con revalidación:** el escritor que perdió el CAS vuelve a validar sus cambios sobre el snapshot ganador antes de reintentar.
- **Diap. 16 · Momentos de la clase (2 min).** Time travel es una lectura de un snapshot anterior: no modifica la tabla ni el `current-snapshot-id`. Rollback sí es una escritura: genera un metadata file nuevo con el `current-snapshot-id` apuntando al snapshot elegido, y los snapshots posteriores dejan de ser el estado vigente. Pregunta: "¿Cuál usarías para auditar y cuál para deshacer un backfill mal hecho?"
  - **Vocabulario técnico**
  - **Time travel:** consulta de solo lectura sobre un snapshot histórico.
  - **Rollback:** commit que redefine el snapshot vigente como uno anterior.
  - **Backfill:** recarga de datos históricos sobre una tabla ya poblada.
- **Diap. 17 · Time travel (4 min).** El motor resuelve el snapshot por `snapshot-id` o por timestamp (el último snapshot cuyo `timestamp-ms` es menor o igual al pedido). La diapositiva usa `FOR SYSTEM_TIME AS OF`; Athena también acepta `FOR TIMESTAMP AS OF` y `FOR VERSION AS OF`. Usos: comparar resultados antes y después de un backfill, recuperar filas borradas por error y auditar. El límite es la retención: la limpieza de snapshots (`expireSnapshots`, o `VACUUM` en Athena) elimina los snapshots viejos y los data files que solo ellos referencian, y esos puntos dejan de ser alcanzables. Se controla con las propiedades de tabla `history.expire.max-snapshot-age-ms` y `history.expire.min-snapshots-to-keep`. No es un backup: vive en el mismo bucket y cada snapshot retenido mantiene archivos en S3 y agranda el metadata file, lo que encarece la planificación.
  - **Vocabulario técnico**
  - **`FOR SYSTEM_TIME AS OF` / `FOR VERSION AS OF`:** cláusulas SQL que seleccionan el snapshot por timestamp o por `snapshot-id`.
  - **Retención de snapshots:** política que define cuánto historial se conserva (por antigüedad y por cantidad mínima).
  - **`expireSnapshots`:** acción de Iceberg que borra snapshots expirados y sus archivos no referenciados.
  - **Backup:** copia independiente, en otra ubicación, que sobrevive a la pérdida del bucket original.
  - **Validar antes de clase:** ejecutá en Athena la sintaxis exacta que vayas a mostrar.
- **Diap. 18 · Evolución de esquemas (4 min).** Cada columna tiene un `field-id` entero, único e inmutable, registrado en el metadata file; los lectores resuelven las columnas por ID y no por nombre ni por posición. Por eso renombrar cambia solo el nombre en los metadatos, reordenar no toca los datos, y una columna que se borra y se vuelve a crear con el mismo nombre recibe un ID nuevo y no recupera los valores viejos. Una columna agregada devuelve `NULL` al leer archivos anteriores, que es lo que permite que las consultas viejas sigan funcionando contra datos viejos. Promociones de tipo permitidas: `int` a `long`, `float` a `double` y ampliar la precisión de un `decimal`. Reducir no se permite: de `double` a `float` hay pérdida de precisión. Pregunta: "¿Qué pasa en un data lake clásico si renombrás una columna?"
  - **Vocabulario técnico**
  - **`field-id`:** identificador entero y permanente de cada columna en el esquema de Iceberg.
  - **Schema evolution:** cambiar el esquema de una tabla (agregar, borrar, renombrar, reordenar, promover tipos) sin reescribir los datos.
  - **Promoción de tipo:** ampliar el tipo de una columna a otro que contiene todos los valores del original.
  - **Columna opcional (nullable):** columna que admite `NULL`; es la única que se puede agregar sin reescribir archivos.
  - **Compatibilidad hacia atrás:** una consulta escrita contra el esquema viejo sigue siendo válida contra el nuevo.
- **Diap. 19 · El catálogo manda (4 min).** En Glue, una tabla Iceberg se identifica por los parámetros `table_type = ICEBERG` y `metadata_location`; `GlueCatalog` los define y los actualiza en cada commit. Sin `table_type`, Athena interpreta la tabla como Hive (ubicación igual a un directorio) y falla al leerla. Un crawler infiere el esquema muestreando archivos, corre por agenda o a demanda y no entiende commits: en streaming agrega demora y puede inferir mal esquemas anidados, así que el catálogo lo actualiza el propio job. Lake Formation otorga permisos sobre databases, tablas y columnas, y aplica filtros de fila (data filters); IAM sobre el bucket solo distingue prefijos y objetos, sin ver filas ni columnas. Para que Redshift lea: `CREATE EXTERNAL SCHEMA ... FROM DATA CATALOG DATABASE 'lakehouse_db' IAM_ROLE '<arn>'`, con un rol que tenga permisos sobre el catálogo y sobre S3. Adelantá que esto es la base del Módulo 6.
  - **Vocabulario técnico**
  - **`table_type = ICEBERG`:** parámetro de la tabla en Glue que indica al motor que debe leerla como Iceberg.
  - **Crawler:** proceso de Glue que explora una ubicación, infiere el esquema y registra tablas.
  - **Lake Formation:** capa de gobernanza de AWS con permisos por tabla, columna y fila sobre el Data Catalog.
  - **Seguridad a nivel de columna y de fila:** control de qué columnas y qué filas ve cada principal.
  - **External schema (Redshift):** esquema que apunta a una database de Glue para consultar tablas de S3 desde Redshift.
  - **Rol IAM de servicio:** identidad con la que un servicio (Redshift, Flink) accede al catálogo y al bucket.

### Break (10 min · diap. 20)

Dejá la diap. 20 en pantalla. Avisá que a la vuelta viene la cuenta del small files problem, que justifica el particionado y la compactación.

### Bloque 3 · Layout físico: Glue Streaming, particionado y compactación (22 min · diap. 21–28)

La diap. 21 es la portada del bloque: pasá directo a la 22.

- **Diap. 22 · Glue Streaming (4 min).** Es un job de AWS Glue de tipo streaming que corre sobre Spark Structured Streaming: procesa micro-batches, y cada uno llega a tu código como un DataFrame (en Glue, mediante `glueContext.forEachBatch` con las opciones `windowSize` y `checkpointLocation`). La latencia es de segundos. Flink, en cambio, procesa evento por evento con estado administrado por checkpoints, y su latencia es de milisegundos. Por eso Flink va para lógica stateful de baja latencia y Glue para normalizar y refinar. Sin `checkpointLocation` no se guardan los offsets (en Kinesis, los sequence numbers por shard): tras una falla el job no sabe dónde retomar, y entonces duplica o pierde datos. El Data Catalog funciona como contrato de entrada: el esquema de la tabla define qué estructura se espera del JSON de origen.
  - **Vocabulario técnico**
  - **Spark Structured Streaming:** motor de streaming de Spark que trata el stream como una tabla que crece y lo procesa en micro-batches.
  - **Micro-batch / trigger:** lote pequeño de registros nuevos, procesado cada vez que se dispara el trigger (por ejemplo, cada 60 s).
  - **`checkpointLocation`:** ruta de S3 donde el job guarda offsets y estado para retomar tras una falla.
  - **Offset / sequence number:** posición de un registro dentro de un shard de Kinesis.
  - **DPU y auto scaling:** unidad de cómputo de Glue; en streaming serverless el número de workers se ajusta al throughput.
  - **Schema enforcement:** rechazar o tratar aparte los registros que no cumplen el esquema registrado.
  - **Procesamiento stateful:** el resultado depende de estado acumulado entre eventos (ventanas, joins, agregaciones).
- **Diap. 23 · Coder Tips (2 min).** Dos errores que muerden. Primero: `startingPosition` define desde qué punto del stream empieza a leer el job (`latest`, solo registros nuevos, o `trim_horizon`, desde el registro más antiguo retenido); definilo explícitamente, porque con `trim_horizon` un stream con 24 h de retención reprocesa un día completo y genera un pico de carga innecesario. Segundo: el job de Glue tiene que inicializarse (`Job(glueContext).init(args['JOB_NAME'], args)`); si no, el estado del job no se registra y los commits de estado en el Data Catalog no quedan guardados.
  - **Vocabulario técnico**
  - **`startingPosition`:** opción de la fuente Kinesis que fija la posición inicial de lectura (`latest` o `trim_horizon`).
  - **Retención del stream:** tiempo que Kinesis conserva los registros; 24 h por defecto, ampliable hasta 365 días.
  - **Reprocesamiento:** volver a leer registros ya leídos; genera duplicados si el destino no es idempotente.
  - **Inicialización del job:** llamada a `Job.init` que registra el job en Glue antes de procesar.
- **Diap. 24 · El small files problem (4 min).** Escenario: Glue Streaming con un commit por minuto, tabla particionada por fecha y cinco regiones. Pedí al grupo que haga la cuenta antes de mostrarla (1 min): cada commit escribe un archivo por partición, o sea 5 regiones × 60 commits por hora × 24 horas = 7.200 archivos por día. Cada archivo es una entrada más en los manifests y una request GET extra a S3 solo para leer su footer; además, un archivo de pocos KB tiene un row group diminuto, sin ganancia de compresión ni de lectura columnar, y el motor lanza miles de tareas casi vacías. El tamaño objetivo es de 128 a 512 MB por archivo (el default de escritura de Iceberg, `write.target-file-size-bytes`, es 512 MB); en streaming nunca se llega solo, porque el commit se dispara por tiempo y no por tamaño.
  - **Vocabulario técnico**
  - **Small files problem:** degradación de planificación y lectura causada por muchos archivos mucho menores al tamaño objetivo.
  - **Latencia base por GET:** tiempo fijo de cada request a S3, independiente del tamaño del objeto.
  - **Overhead de tareas:** costo de lanzar y coordinar una tarea del motor; con archivos diminutos supera al de procesarlos.
  - **`write.target-file-size-bytes`:** propiedad de tabla con el tamaño objetivo de cada data file.
  - **Crecimiento de manifests:** más archivos significan más entradas de manifest que el planificador debe leer.
- **Diap. 25 · Partition pruning (3 min).** Con una tabla particionada, el planificador evalúa el WHERE contra la partición de cada manifest y de cada data file y descarta las que no coinciden, sin listar ni abrir nada. La clave se elige por su cardinalidad (cantidad de valores distintos). Cardinalidad muy alta, como `user_id` con millones de usuarios, genera millones de particiones, cada una con pocos archivos chicos, y manifests enormes. Cardinalidad baja y despareja produce data skew: si el 99% de los eventos es de un solo tipo, hay una partición gigante, una sola tarea hace casi todo el trabajo y se pierde paralelismo. Regla de oro: la columna que siempre aparece en el WHERE y que tiene de cientos a pocos miles de valores, por ejemplo la fecha. Avisá que esta justificación va en el README de la pre-entrega.
  - **Vocabulario técnico**
  - **Partition pruning:** descartar particiones enteras evaluando el predicado del WHERE contra los valores de partición.
  - **Cardinalidad:** cantidad de valores distintos de una columna.
  - **Sobre-particionamiento:** elegir una clave de cardinalidad tan alta que cada partición queda con casi nada de datos.
  - **Data skew:** reparto desigual de filas entre particiones o tareas.
  - **Paralelismo:** cantidad de tareas que el motor ejecuta a la vez; depende de cómo se reparten los archivos.
- **Diap. 26 · Hidden partitioning (3 min).** Las transformaciones de partición de Iceberg son `year`, `month`, `day`, `hour`, `bucket(N, col)`, `truncate(W, col)` e `identity`. La partition spec guarda la relación entre la columna de origen y la transformación, así que cuando el analista filtra por `event_timestamp`, el planificador traduce ese predicado a uno sobre `day(event_timestamp)`. En Hive había que materializar columnas derivadas (`year`, `month`, `day`) en los datos, y el analista tenía que conocerlas y filtrarlas, porque si no la consulta escaneaba todo. Además, Iceberg permite partition evolution: se cambia la spec (por ejemplo, de `day` a `hour`), los archivos viejos conservan la anterior, los nuevos usan la nueva y ambas conviven en la misma tabla, sin reescribir nada.
  - **Vocabulario técnico**
  - **Partition transform:** función que deriva el valor de partición a partir de una columna (`day`, `hour`, `bucket`, `truncate`, `identity`).
  - **`bucket(N, col)`:** reparte los valores de una columna en N grupos mediante un hash; sirve para claves de alta cardinalidad.
  - **Partition evolution:** cambiar la partition spec de una tabla existente sin reescribir los datos anteriores.
  - **Columna derivada:** columna materializada a partir de otra, como el `year` de Hive.
  - **Predicado:** condición del WHERE que el motor usa para podar archivos.
- **Diap. 27 · Compaction (3 min).** Compactar es reescribir data files pequeños en pocos archivos grandes y publicar un snapshot con operación `replace`. En Iceberg se hace con el procedure `rewrite_data_files` (en Athena, `OPTIMIZE ... REWRITE DATA USING BIN_PACK`). Bin-packing, la estrategia por defecto, agrupa archivos hasta el tamaño objetivo sin cambiar el orden de las filas: es rápida y barata. Sorting reescribe ordenando por una o más columnas, con orden lexicográfico o con Z-Order, que intercala los bits de varias columnas para que filas cercanas en todas ellas queden en los mismos archivos; eso afina el pruning por mínimo y máximo en filtros fuera de la clave de partición, a cambio de más CPU. Auto-compaction: Glue Data Catalog ofrece table optimizers que compactan tablas Iceberg sin que programes nada, aunque conviene entender el proceso manual.
  - **Vocabulario técnico**
  - **Compactación (rewrite):** reescribir data files para cambiar su cantidad o su tamaño sin alterar los datos de la tabla.
  - **`rewrite_data_files`:** procedure de Iceberg que ejecuta la compactación; recibe la estrategia y filtros.
  - **Bin-packing:** estrategia que agrupa archivos pequeños hasta el tamaño objetivo, sin reordenar.
  - **Sort / Z-Order:** estrategia que reordena las filas, por una columna o por varias a la vez, para mejorar el pruning.
  - **Table optimizer (Glue):** servicio administrado que compacta tablas Iceberg de forma automática.
- **Diap. 28 · Escribir o leer rápido (3 min).** Es un trade-off entre el costo de escribir y el de leer. Write amplification: compactar lee y reescribe bytes que ya estaban en S3, y eso consume cómputo y requests facturables; compactar cada cinco minutos multiplica ese costo. Read efficiency: sin compactar, cada consulta paga la planificación y el I/O de miles de archivos y los dashboards tardan minutos en consultas simples. La práctica en AWS es un job de Glue asíncrono, disparado por agenda (trigger programado) o por evento (EventBridge), que compacta cada pocas horas y se limita a particiones que ya no reciben datos, por ejemplo el día anterior, para no reescribir lo que sigue cambiando. Mientras tanto, el streaming sigue escribiendo archivos chicos para mantener la frescura del dato.
  - **Vocabulario técnico**
  - **Write amplification:** relación entre los bytes que se escriben en total y los bytes nuevos que ingresaron; la compactación la aumenta.
  - **Read efficiency:** costo de planificación y de I/O que paga una consulta; baja con pocos archivos grandes.
  - **Frescura del dato (data freshness):** tiempo entre que un evento ocurre y es consultable en la tabla.
  - **Compactación parcial:** compactar solo ciertas particiones, filtrando con un `where`.
  - **Job asíncrono:** proceso de mantenimiento que corre aparte del pipeline de ingesta y no lo bloquea.

## Práctica en vivo · capa de almacenamiento (Bloque 4 · 30 min · diap. 29–35)

La práctica es una sola, se da después de todo el contenido y termina siendo la Pre-entrega 5: cinco pasos que dejan una tabla Iceberg particionada por día, registrada en Glue y consultable desde Athena, alimentada por el job de Flink. Al final hay una tabla para sumar prácticas cortas si preferís dividirla.

### Calentamiento: arquitecto forense (5 min · diap. 29–30)

La diap. 29 es la portada del bloque.

- Proyectá una arquitectura donde Flink escribe en una tabla Iceberg registrada en Glue, pero el catálogo no refleja el estado real de la tabla.
- Preguntá: "¿Qué error le devuelve Athena al equipo de BI cuando intenta leer esa tabla? Escriban el mensaje exacto en el chat."
- Diagnosticá con el grupo las dos causas probables (Bloque 2, diap. 19): si falta el parámetro `table_type = ICEBERG`, Athena trata la tabla como Hive y falla al leerla; si el parámetro `metadata_location` apunta a un JSON de metadatos viejo porque el job no actualizó el catálogo tras el commit, Athena lee un snapshot anterior al último.
- A completar antes de clase: el mensaje de error exacto de la arquitectura que vayas a proyectar → \[completar\].

### Paso 1 · La base en Glue (4 min · diap. 31)

**Objetivo:** registrar en el Glue Data Catalog la database `lakehouse_db`, donde van a vivir las tablas Iceberg, y activar el versionado de objetos en el bucket S3 del lakehouse.

- Declarar el recurso `aws_glue_catalog_database` con `name = "lakehouse_db"`.
- Declarar `aws_s3_bucket_versioning` sobre el bucket del lakehouse con `status = "Enabled"`.
- Ejecutar `terraform plan`, revisar los recursos a crear y aplicar con `terraform apply`.
- Verificar con `aws glue get-database --name lakehouse_db` (o en la consola de Glue, sección Databases) y con `aws s3api get-bucket-versioning --bucket <bucket>`.
- **Resultado esperado:** `lakehouse_db` existe en Glue sin tablas, el bucket devuelve `"Status": "Enabled"` y ambos recursos figuran en el state de Terraform.

```hcl
resource "aws_glue_catalog_database" "lakehouse" {
  name = "lakehouse_db"
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}
```

### Paso 2 · Permisos de Flink (4 min · diap. 32)

**Objetivo:** darle al rol IAM de ejecución de Flink (el del Módulo 4) los permisos mínimos para crear, leer y actualizar tablas en Glue, y para leer y escribir objetos en el bucket del lakehouse.

- Agregar al rol una policy con `glue:GetDatabase`, `glue:GetTable`, `glue:CreateTable` y `glue:UpdateTable`, limitada a los ARN del catálogo, de la database `lakehouse_db` y de sus tablas.
- Agregar `s3:GetObject`, `s3:PutObject` y `s3:DeleteObject` sobre `<bucket>/*`, y `s3:ListBucket` sobre el bucket.
- Revisar que ningún `actions` ni `resources` contenga un `*` suelto.
- **Resultado esperado:** el `terraform plan` solo agrega la policy y su attachment al rol, y el primer commit de Flink no devuelve `AccessDeniedException` de Glue ni de S3.
- **Validar antes de clase:** la diapositiva lista `GetTable`, `UpdateTable` y `CreateTable`; confirmá si `GlueCatalog` también pide `glue:GetDatabase` en tu cuenta.

```hcl
data "aws_iam_policy_document" "flink_lakehouse" {
  statement {
    actions = ["glue:GetDatabase", "glue:GetTable", "glue:CreateTable", "glue:UpdateTable"]
    resources = [
      "arn:aws:glue:${var.region}:${var.account_id}:catalog",
      aws_glue_catalog_database.lakehouse.arn,
      "arn:aws:glue:${var.region}:${var.account_id}:table/lakehouse_db/*",
    ]
  }

  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.lakehouse.arn, "${aws_s3_bucket.lakehouse.arn}/*"]
  }
}
```

### Paso 3 · El catálogo (6 min · diap. 33)

**Objetivo:** configurar la aplicación de Flink para que use `GlueCatalog` como catálogo de Iceberg y `S3FileIO` para leer y escribir los archivos en S3.

- Agregar al `pom.xml` (o `build.gradle`) el runtime de Iceberg para Flink y el módulo de AWS de Iceberg; la diapositiva los nombra `flink-connector-iceberg` e `iceberg-aws`.
- Crear el `CatalogLoader` con `CatalogLoader.custom(...)`, indicando la clase `org.apache.iceberg.aws.glue.GlueCatalog` y las propiedades `warehouse` e `io-impl`.
- Alinear las versiones de Flink, Iceberg y AWS SDK v2: una combinación cruzada falla en runtime con `NoSuchMethodError` o `ClassNotFoundException`, no al compilar.
- Pasar a los operadores el `CatalogLoader` o el `TableLoader`, nunca el objeto `Catalog`: Flink serializa el loader y el `Catalog` no es serializable.
- **Resultado esperado:** la aplicación arranca y `catalogLoader.loadCatalog().listTables(Namespace.of("lakehouse_db"))` responde sin excepciones.
- **Validar antes de clase:** el runtime oficial de Iceberg para Flink se publica como `iceberg-flink-runtime` con la versión de Flink en el nombre. Confirmá artefactos y versiones exactos contra tu proyecto.

```java
Map<String, String> props = new HashMap<>();
props.put("warehouse", "s3://<bucket-lakehouse>/warehouse");
props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");

CatalogLoader catalogLoader = CatalogLoader.custom(
    "glue", props, new org.apache.hadoop.conf.Configuration(),
    "org.apache.iceberg.aws.glue.GlueCatalog");

TableLoader tableLoader = TableLoader.fromCatalog(
    catalogLoader, TableIdentifier.of("lakehouse_db", "eventos"));
```

### Paso 4 · El IcebergSink (6 min · diap. 34)

**Objetivo:** crear la tabla Iceberg `lakehouse_db.eventos` particionada con `days(event_timestamp)` y conectar el stream al `IcebergSink`, de modo que cada checkpoint de Flink confirme un commit y genere un snapshot nuevo.

- Crear la tabla con `catalog.createTable(...)` y un `PartitionSpec` con `.day("event_timestamp")`. La partición se define al crear la tabla, no en el sink; `GlueCatalog` registra además `table_type = ICEBERG` en Glue.
- Instanciar el `IcebergSink` con el `TableLoader` de `eventos` y conectarlo al `DataStream<RowData>`.
- Activar `env.enableCheckpointing(60_000)`: un checkpoint por minuto, es decir un commit y un snapshot por minuto.
- **Resultado esperado:** después de cada checkpoint, `SELECT snapshot_id, committed_at FROM "lakehouse_db"."eventos$snapshots"` en Athena muestra una fila nueva.
- **Validar antes de clase:** si la versión de Iceberg del proyecto es anterior a `IcebergSink`, el equivalente es `FlinkSink.forRowData`.

```java
// Tabla de ejemplo, con partición por día sobre el event time
Catalog catalog = catalogLoader.loadCatalog();
Schema schema = new Schema(
    Types.NestedField.required(1, "event_id", Types.StringType.get()),
    Types.NestedField.required(2, "event_timestamp", Types.TimestampType.withZone()),
    Types.NestedField.optional(3, "region", Types.StringType.get()));
PartitionSpec spec = PartitionSpec.builderFor(schema).day("event_timestamp").build();
catalog.createTable(TableIdentifier.of("lakehouse_db", "eventos"), schema, spec);

// Checkpoint cada minuto y sink hacia la tabla
env.enableCheckpointing(60_000);
IcebergSink.forRowData(rowDataStream)
    .tableLoader(tableLoader)
    .append();
```

### Paso 5 · Validar y documentar (5 min · diap. 35)

**Objetivo:** comprobar de punta a punta que un evento publicado en Kinesis queda consultable en Athena, y dejar la evidencia en el repositorio.

- Publicar eventos en el stream de Kinesis (con el productor del proyecto o con `aws kinesis put-record`) y esperar el próximo checkpoint, hasta 1 minuto. Usá esa espera para explicar el README.
- En Athena, ejecutar el `SELECT` de abajo y comparar el conteo con la cantidad de eventos enviados.
- En S3, listar la carpeta `metadata/` de la tabla y confirmar un archivo `.metadata.json` nuevo; en `data/`, confirmar los Parquet bajo `event_timestamp_day=<fecha>`.
- En el README, incluir la captura de `lakehouse_db` en la consola de Glue y la justificación del particionado. Ejemplo: `days(event_timestamp)` genera un valor por día y es la columna que filtran las consultas (diap. 25).
- **Resultado esperado:** el conteo de Athena coincide con los eventos enviados, el bucket contiene `.metadata.json` y Parquet particionados por día, y el README documenta la elección. Con eso queda cubierta la Pre-entrega 5.

```sql
SELECT region, count(*) AS eventos
FROM lakehouse_db.eventos
GROUP BY region;
```

### Espacio para prácticas adicionales

Si preferís varias prácticas cortas en vez de una sola, o querés sumar una extra, completá esta tabla. Las dos primeras filas son ideas basadas en el contenido de los bloques 2 y 3.

| Práctica | Objetivo | Consigna | Min | Material |
| --- | --- | --- | --- | --- |
| A (idea) | Time travel y rollback | Consultar la tabla del Paso 5 en un snapshot anterior y compararla con la actual | \[completar\] | Tabla del Paso 5 |
| B (idea) | Evolución de esquemas | Renombrar una columna y verificar que las consultas siguen funcionando | \[completar\] | Tabla del Paso 5 |
| C | \[completar\] | \[completar\] | \[completar\] | \[completar\] |
| D | \[completar\] | \[completar\] | \[completar\] | \[completar\] |

## Pre-entrega 5 y cierre (Bloque 5 · 13 min · diap. 36–42)

La práctica ya dejó armado casi todo el entregable: el objetivo de este bloque es que cada persona sepa qué se evalúa y cómo sigue el proyecto.

### Pre-entrega 5 (8 min · diap. 36–40)

La diap. 36 es la portada del bloque.

- **Diap. 37 · ¿De qué se trata? (2 min).** Que la salida del pipeline de Flink sea una tabla formal de Iceberg y no un archivo huérfano en S3. Piezas: una base en Glue Data Catalog como metastore central, el IcebergSink conectado a ese catálogo y todo declarado en Terraform. Duración estimada: 6 a 8 horas. Entrega: repositorio con las carpetas infra/ y app/.
- **Diap. 38 · Pasos de la entrega (2 min).** Lo que revisa el docente al abrir el repo: aws\_glue\_catalog\_database llamado lakehouse\_db en Terraform, rol IAM de Flink con permisos sobre catálogo y bucket, bucket con versionado, dependencias de Iceberg con GlueCatalog configurado, IcebergSink escribiendo con checkpointing activo, y README con capturas de Glue y la justificación del particionado.
- **Diap. 39 · ¿Cómo se evalúa? (2 min).** Rúbrica sobre 100; se aprueba con 70. Remarcá que documentación y particionado pesa 8% y se apoya en el partition pruning del Bloque 3.
- **Diap. 40 · ¿Cómo continúa? (2 min).** Ya tienen el dato entrando por Kinesis, procesado por Flink y aterrizando en tablas ACID. El Módulo 6 es analítica en tiempo real con Amazon Redshift, que consulta estas mismas tablas de Glue vía external schema. Para llevar: el particionado elegido define qué tan rápido responden los dashboards del módulo que viene.

| Criterio de la rúbrica | Peso |
| --- | --- |
| Integración Flink, Iceberg y Glue: dependencias, GlueCatalog e IcebergSink apuntando a S3 | 32% |
| Infraestructura declarativa: catálogo, bucket y permisos reproducibles en Terraform | 30% |
| Checkpointing y consistencia: commits de metadatos sin errores de concurrencia | 18% |
| Verificación funcional: datos legibles desde Athena y metadatos Iceberg en S3 | 12% |
| Documentación y particionado: capturas de Glue y justificación según el partition pruning | 8% |

### Preguntas y cierre (5 min · diap. 41–42)

- **Diap. 41 · ¿Preguntas?** Espacio abierto para dudas del módulo. La invitación de la diapositiva: que traigan sus tablas que Athena se niega a leer.
- **Diap. 42 · Gracias.** Agradecé y detené la grabación.

## Glosario de términos

Los 40 términos centrales de la clase, en orden alfabético, con la definición tal como se usa en el módulo. El vocabulario ampliado de cada diapositiva está en el guion.

| Término | Definición |
| --- | --- |
| ACID | Atomicidad, consistencia, aislamiento y durabilidad. Iceberg las suma a tablas sobre S3. |
| Apache Iceberg | Formato de tabla abierto que organiza archivos Parquet, ORC o Avro con metadatos transaccionales. |
| Athena | Motor SQL serverless de AWS que consulta datos en S3 usando el catálogo de Glue. |
| Auto-compaction | Compactación que AWS gestiona sola para tablas Iceberg en Glue. |
| Bin-packing | Compactación que combina archivos chicos en grandes sin tocar el orden de los datos. |
| Catálogo | Capa que guarda el puntero a la versión actual de la tabla. En AWS, el Glue Data Catalog. |
| Checkpoint (Flink) | Punto de guardado periódico del estado del job. Con IcebergSink, cada checkpoint dispara un commit. |
| checkpointLocation | Ruta donde un job de Glue Streaming guarda su progreso para retomar tras una falla sin duplicar datos. |
| Compactación | Proceso que reescribe muchos archivos chicos en pocos archivos grandes. |
| Compare-and-swap | Operación atómica que cambia un valor solo si sigue siendo el esperado. Iceberg se lo exige al catálogo. |
| Crawler | Proceso de Glue que explora datos y registra tablas. No conviene en streaming por su demora. |
| Data lake | Archivos en S3 organizados por carpetas, sin transacciones ni aislamiento. |
| Data skew | Distribución despareja: una partición gigante y el resto casi vacías. |
| DPU | Unidad de cómputo de Glue. En streaming serverless se escala según el throughput. |
| Evolución de esquemas | Agregar, borrar, renombrar y reordenar columnas, o ampliar tipos, sin reescribir los datos. |
| expireSnapshots | Operación que borra snapshots viejos y sus archivos de S3; limita hasta dónde llega el time travel. |
| External schema | Esquema de Redshift que apunta a una base de Glue para consultar tablas del lakehouse. |
| Flink | Motor de procesamiento stateful de muy baja latencia, usado para escribir en Iceberg. |
| Footer (Parquet) | Final de cada archivo Parquet, con esquema y estadísticas. El lector lo abre antes de leer datos. |
| Glue Data Catalog | Metastore central de AWS donde se registran bases y tablas. |
| Glue Streaming | Jobs de Glue sobre Spark Structured Streaming para ETL y ELT incremental con micro-batches. |
| Hidden partitioning | Partición definida como transformación (por ejemplo days) de una columna; el analista filtra por la columna original. |
| Hive Metastore | Catálogo clásico que rastrea carpetas y particiones, no archivos. |
| IcebergSink | Sink de Flink que escribe en una tabla Iceberg y hace commit en cada checkpoint. |
| Lake Formation | Servicio de gobernanza de AWS con permisos por columna y por fila sobre el catálogo. |
| Lakehouse | Datos abiertos en S3, gobernanza en la capa de metadatos y varios motores sobre la misma copia. |
| Manifest file | Archivo que lista archivos de datos con el mínimo y el máximo de cada columna. |
| Manifest list | Archivo Avro que enumera los manifest files de un snapshot, con estadísticas. |
| Metadata file | JSON con el esquema actual, el particionamiento y todos los snapshots históricos. |
| Micro-batch | Procesamiento en lotes pequeños y frecuentes, con latencia de segundos. |
| OCC (concurrencia optimista) | Se asume que no hay conflictos y se valida al commit; si hay, el segundo escritor reintenta. |
| Partition pruning | El motor descarta particiones enteras según el WHERE, sin listar sus archivos. |
| Rollback | Mueve el current-snapshot-id a un snapshot anterior, de forma definitiva. |
| S3 listing | Listar los archivos de un prefijo de S3 para saber qué datos existen. |
| Small files problem | Demasiados archivos chicos degradan lecturas y agrandan los metadatos. Tamaño ideal: 128 a 512 MB. |
| Snapshot | Conjunto completo de archivos válidos en un punto del tiempo; se crea en cada commit exitoso. |
| Snapshot isolation | Los lectores ven una vista consistente mientras los escritores arman el siguiente snapshot. |
| Sorting (compactación) | Compactación que además reordena los datos, por ejemplo con Z-Order. Más costosa en CPU. |
| Time travel | Consultar la tabla como existía antes, por snapshot-id o por timestamp, sin tocar la tabla viva. |
| Write amplification | Costo extra de cómputo por leer y reescribir datos que ya estaban en S3 al compactar. |
