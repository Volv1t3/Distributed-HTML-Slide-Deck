# Propuesta de proyecto: Distributed Slides

## 1. Descripción del proyecto

**Distributed Slides** es un editor sencillo de presentaciones que permite crear diapositivas de forma privada y, posteriormente, compartirlas para que varias personas colaboren sobre una misma presentación. Su propósito principal es demostrar replicación, control de concurrencia, consistencia, tolerancia a fallas y recuperación en un sistema distribuido.

En el escenario inicial, Sofía crea una presentación en su dispositivo, añade diapositivas y elementos (textos y figuras) y guarda cada modificación localmente. Mientras la presentación sea **privada**, su copia local es la fuente de verdad: no necesita comunicación con el clúster ni bloqueos distribuidos. Al compartirla con Santiago, el sistema publica una instantánea completa de la presentación en el clúster. **Solo después de confirmarse esa publicación** pasa al modo **compartido**: el estado confirmado por el clúster se convierte en la fuente de verdad y las copias de los participantes son vistas locales sincronizadas.

En modo compartido, distintos usuarios pueden editar elementos diferentes de manera simultánea. Para modificar un mismo elemento, un usuario debe adquirir primero un **bloqueo exclusivo por elemento**. El bloqueo se concede mediante una operación confirmada por consenso; los demás usuarios pueden ver el elemento, pero sus solicitudes de edición serán rechazadas mientras otro participante tenga el bloqueo vigente. Los cambios confirmados se propagan a las demás réplicas y clientes. Las vistas pueden estar temporalmente desactualizadas, pero no autorizan por sí mismas una modificación.

**Alcance inicial:** una presentación pequeña, varias diapositivas, elementos de texto y figuras básicas, edición de contenido y estilo, y una interfaz ligera de colaboración. No se implementarán edición simultánea carácter por carácter, renderizado o despliegue distribuido de HTML, ni funciones avanzadas de diseño.

## 2. Modelo de colaboración y replicación

- **Modo `PRIVATE`:** operaciones y persistencia locales. Cada operación recibe un identificador único formado por `device_id` y un contador local creciente. Cada elemento mantiene su propia versión local.
- **Transición `PUBLISHING`:** se envía una instantánea de la presentación mediante `publish_deck`. La publicación es indivisible desde la perspectiva del estado compartido: no debe quedar visible una presentación parcialmente inicializada.
- **Modo `SHARED`:** tres réplicas independientes mantienen una máquina de estados replicada mediante **Raft** (elección de líder, log ordenado y confirmación por mayoría). Con tres réplicas, se requiere una mayoría de **dos** para confirmar modificaciones.
- **Control de concurrencia:** un mapa de bloqueos por recurso, tokens de bloqueo para descartar solicitudes antiguas y comprobación de la versión esperada de cada elemento. Para crear o reordenar elementos, puede bloquearse un recurso estructural de la diapositiva.
- **Lecturas y sincronización:** las lecturas de vista previa se atienden desde una réplica local y ofrecen **consistencia eventual**. La adquisición de bloqueos y las modificaciones ofrecen **linealizabilidad** una vez confirmadas. La descarga inicial debe proporcionar un estado que incluya, como mínimo, la publicación confirmada; no puede tratar una copia atrasada como inicialización válida.
- **Recuperación:** una réplica retrasada recupera las entradas o el estado que le faltan antes de responder como actualizada. Ante una partición, el lado sin mayoría puede mostrar datos locales, pero no confirmar cambios ni bloqueos nuevos.
- **Bloqueos con temporizador (extensión prevista):** `renew_lock` y `expire_lock` son transiciones confirmadas por Raft. El vencimiento no se decide de manera independiente en cada réplica ni depende de asumir relojes perfectamente sincronizados. Un nuevo bloqueo invalida el token anterior.

Para el modelo principal **no se requieren CRDTs LWW**: Raft ya establece un orden único de las modificaciones y las versiones detectan ediciones obsoletas. Los CRDTs quedan como alternativa de diseño que podría explorarse, pero no se mezclarán con el mecanismo autoritativo en la primera versión.

## 3. Operaciones de la aplicación

Las operaciones se organizan por su ámbito: **locales**, **distribuidas de la aplicación** e **internas de coordinación**. Las prioridades `Esencial`, `Recomendada` y `Opcional` describen el alcance propuesto, no el nivel de consistencia.

### 3.1. Operaciones locales (presentación privada)

| Operación | Propósito | Garantía | Mecanismo / datos | Prioridad |
|---|---|---|---|---|
| `create_private_deck` | Crear una presentación vacía en el dispositivo. | Persistencia local; sin garantía distribuida. | Identificador de presentación y almacenamiento local. | Esencial |
| `add_local_element` | Añadir texto o figura a una diapositiva privada. | Orden local. | ID único de elemento y registro en diario local. | Esencial |
| `update_local_element` | Cambiar texto, posición o estilo de un elemento privado. | Orden local. | Incremento de versión por elemento y contador local de operaciones. | Esencial |
| `delete_local_element` | Eliminar un elemento privado. | Orden local. | Actualización del estado y diario local. | Esencial |
| `update_local_slide` | Modificar fondo, tamaño o configuración de una diapositiva privada. | Orden local. | Actualización y versión local de la diapositiva. | Esencial |
| `save_local_snapshot` | Guardar el estado privado para recuperarlo tras reiniciar el editor. | Durabilidad local según guardado exitoso. | Archivo o almacenamiento persistente local. | Esencial |

### 3.2. Operaciones distribuidas (presentación compartida)

| Operación | Propósito | Garantía | Mecanismo | Prioridad |
|---|---|---|---|---|
| `publish_deck` | Publicar por primera vez una presentación privada como estado compartido completo. | **Linealizabilidad**. | Entrada en el log de Raft; creación atómica del estado inicial; `request_id` para reintentos. | Esencial |
| `sync_initial_snapshot` | Entregar al nuevo colaborador un estado que incluya la publicación confirmada. | Lectura validada por líder/mayoría o por índice mínimo confirmado y aplicado. | Instantánea con índice de versión/commit verificable. | Esencial |
| `create_element` | Crear un texto o figura dentro de una diapositiva compartida. | **Linealizabilidad**. | Log de Raft; validación del bloqueo estructural de la diapositiva. | Esencial |
| `acquire_lock` | Solicitar el derecho exclusivo de editar un elemento. | **Linealizabilidad**. | Raft + comprobación condicional atómica de estado libre + token único. | Esencial |
| `update_element` | Guardar cambios en contenido, posición o estilo de un elemento. | **Linealizabilidad**. | Raft + validación del token del bloqueo + versión esperada del elemento. | Esencial |
| `release_lock` | Liberar voluntariamente un bloqueo vigente. | **Linealizabilidad**. | Raft + comprobación de propietario y token. | Esencial |
| `get_slide` | Consultar el contenido de una diapositiva para visualizarla. | **Consistencia eventual**. | Lectura del estado aplicado en una réplica local. | Esencial |
| `get_element` | Consultar el contenido y la versión de un elemento. | **Consistencia eventual**. | Lectura del elemento en una réplica local. | Esencial |
| `get_deck_preview` | Obtener una vista resumida de la presentación y sus diapositivas. | **Consistencia eventual**. | Lectura local de una instantánea del deck. | Recomendada |
| `read_lock_status` | Mostrar quién parece estar editando un elemento. No concede permisos de edición. | **Consistencia eventual**. | Lectura local del mapa de bloqueos. | Recomendada |
| `delete_element` | Eliminar un elemento compartido sin permitir que ediciones antiguas lo recuperen. | **Linealizabilidad**. | Raft + bloqueo válido + tombstone. | Recomendada |
| `force_unlock` | Invalidar explícitamente un bloqueo desde una acción autorizada. | **Linealizabilidad**. | Raft + invalidación del token anterior. | Recomendada |
| `grant_access` | Registrar a Santiago como colaborador autorizado. | **Linealizabilidad**. | Estado de permisos replicado por Raft; identidades simuladas en el prototipo. | Recomendada |
| `update_slide_settings` | Cambiar fondo, dimensiones u otras propiedades compartidas de la diapositiva. | **Linealizabilidad**. | Raft + bloqueo/versionado del recurso de configuración. | Recomendada |
| `create_slide` | Añadir una diapositiva a la presentación compartida. | **Linealizabilidad**. | Log de Raft y control estructural del deck. | Opcional |
| `reorder_slides` | Cambiar el orden de las diapositivas. | **Linealizabilidad**. | Log de Raft + validación de versión/estructura del deck. | Opcional |
| `renew_lock` | Renovar la titularidad de un bloqueo activo. | **Linealizabilidad**. | Raft + validación de token + incremento de época de renovación. | Opcional; necesaria si se activa la expiración automática |

### 3.3. Operaciones internas de coordinación y recuperación

Estas acciones no necesariamente son botones del editor. Algunas pertenecen al protocolo Raft y otras a la lógica replicada de la aplicación.

| Operación / proceso | Propósito | Garantía esperada | Mecanismo | Prioridad |
|---|---|---|---|---|
| `raft_heartbeat` | Mantener contacto con seguidores y comunicar información del líder. | No es una operación de usuario ni una lectura linealizable. | Mensajes periódicos de Raft. | Esencial |
| `raft_election` | Elegir un nuevo líder si el actual falla. | A lo sumo un líder legítimo por término; seguridad de Raft. | Términos, votos y mayoría. | Esencial |
| `raft_log_catch_up` | Recuperar entradas faltantes de una réplica rezagada. | Conservación del orden de entradas comprometidas. | Replicación y recuperación del log de Raft. | Esencial |
| `state_sync` | Actualizar las copias locales de los clientes con cambios confirmados. | **Convergencia eventual** si hay conectividad y cesan las escrituras. | Consulta periódica de versiones/instantáneas; sin reemplazar el log autoritativo. | Esencial |
| `expire_lock` | Dar por terminado un bloqueo que no se renovó dentro del periodo permitido. | **Linealizabilidad** para la transición que invalida el bloqueo. | Propuesta del líder y confirmación por Raft; comprobación del token y época actuales. | Opcional; emparejada con `renew_lock` |

## 4. Reglas generales de las operaciones

1. **Un bloqueo visible no es un bloqueo concedido:** el cliente solo puede editar después de recibir confirmación exitosa de `acquire_lock`.
2. **Una escritura confirmada no debe perderse:** `create_element`, `update_element`, `delete_element` y demás escrituras compartidas solo se confirman tras el compromiso de la entrada correspondiente en Raft.
3. **Las réplicas no cambian a la vez:** cada una aplica las entradas comprometidas en orden; puede haber retraso de visualización sin permitir escrituras contradictorias.
4. **Versiones por elemento:** una modificación de contenido incrementa la versión de ese elemento; adquirir o renovar un bloqueo no cambia su versión de contenido.
5. **Reintentos idempotentes:** los comandos distribuidos llevan un `request_id` y mantienen resultados de deduplicación para no aplicar dos veces la misma solicitud.
6. **Sin mayoría no hay nuevas escrituras confirmadas:** durante una partición, las lecturas locales pueden continuar, pero no se reconoce como guardado un cambio que no haya sido comprometido.
7. **Permisos e identidad del prototipo:** el control de acceso puede modelarse con sesiones e identificadores simulados; autenticación real queda fuera del alcance inicial.

> **Estado de esta propuesta:** primera definición del producto y catálogo completo de operaciones. Las historias de usuario, el contrato detallado de entradas/salidas, los casos de prueba y las decisiones definitivas sobre expiración de bloqueos se especificarán después.
