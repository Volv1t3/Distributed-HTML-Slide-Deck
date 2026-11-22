# proyecto-starter

Repositorio inicial del Proyecto de CMP5001. Necesitas Python 3.11 o más
nuevo. No necesitas instalar paquetes. Para contenedores necesitas Docker
(Docker Desktop, Colima u OrbStack); sin Docker usa `--local`.

## Archivos

| Archivo | Qué hace | ¿Lo editas? |
| --- | --- | --- |
| `app.py` | tu réplica: qué hace con cada mensaje, pedido y tick | sí |
| `spec.py` | qué hace cada operación con una sola copia, y su garantía | sí |
| `scenarios/` | tus escenarios de falla | sí |
| `node.py` | runtime de una réplica: HTTP, disco, ticks, fallas | no |
| `cluster.py` | levanta y apaga las réplicas | no |
| `client.py` | cliente que guarda el historial | no |
| `chaos.py` | provoca fallas | no |
| `check.py` | revisa un historial | no |

## Primeros pasos

```sh
python3 cluster.py up --local      # o sin --local para usar Docker
python3 cluster.py status
python3 scenarios/normal.py
python3 check.py run/normal.jsonl
python3 cluster.py down
```

`cluster.py down`, y también un nuevo `up`, borran `run/`. Antes copian cada
historial y cada `roles.log` a `historiales/FECHA-HORA/`. Revisa esa copia
con `python3 check.py historiales/FECHA-HORA/normal.jsonl`. Entrega la
carpeta `historiales/`.

El `app.py` de ejemplo no replica. Por eso `check.py` encuentra una violación
de linearizability en `normal`: un `get` en una réplica no ve el `put` que se
hizo en otra. Tu trabajo es que eso no pase.

## Cómo funciona una réplica

`node.py` llama a tu código en un solo hilo, una cosa a la vez:

- `start()`: al arrancar. `self.disk` ya trae lo que guardaste antes de un kill.
- `on_request(req)`: un pedido de cliente. Devuelve la respuesta, o `None` y
  responde después con `self.reply(req.id, ...)`.
- `on_message(src, msg)`: un mensaje de otra réplica.
- `on_tick()`: cada 0.05 s. Úsalo para timeouts, heartbeats y gossip.
- `status()`: devuelve `{"role": ..., "term": ...}`. Si tu diseño tiene líder,
  usa `"role": "leader"` y su periodo. `chaos.py ... leader` y `check.py` lo
  usan.

Las réplicas no comparten memoria ni archivos. Lo que una sabe de otra llega
en mensajes. Un mensaje puede perderse si hay partición o la réplica está
caída; la red no avisa. Cada mensaje sale en su propio hilo, así que los
mensajes pueden llegar en otro orden.

Responde `{"ok": False, ...}` solo cuando esta réplica no puede atender el
pedido (por ejemplo, no tiene mayoría); el cliente reintenta en otra
réplica. Una negativa de tu app ("asiento ocupado") es una respuesta: va con
`"ok": True` y la negativa en `"result"`.

## Fallas

```sh
python3 chaos.py kill leader          # o un nombre: n1, n2, ...
python3 chaos.py start n1             # vuelve con su disco
python3 chaos.py start n1 --empty     # vuelve sin estado
python3 chaos.py pause n2
python3 chaos.py partition n1 / n2 n3
python3 chaos.py delay n3 300
python3 chaos.py heal
```

Con Docker, `kill`, `start` y `pause` actúan sobre el contenedor. La partición
y el delay los aplica `node.py` en los mensajes entre réplicas, igual en los
dos modos. El cliente siempre llega a todas las réplicas.

Los dos modos difieren en dos detalles. Con `--local`, `start` sobre una
réplica que corre la reinicia; con Docker no hace nada. Con `--local`, una
réplica pausada descarta los mensajes y responde `timeout` a los clientes;
con Docker, el contenedor congelado recibe los mensajes tarde, después de
reanudarse. Escribe tus escenarios para que funcionen igual en el modo que
uses.

`chaos.py kill leader` termina el escenario si todavía no hay líder. Espera
antes con `python3 chaos.py leader` o con unos segundos de pausa.

## Historial y checker

`client.py` guarda cada operación en un archivo `.jsonl`: id, cliente,
argumentos, hora de inicio, hora de fin, si recibió respuesta y el
resultado. Al final de un escenario con operaciones eventuales, llama
`client.final(...)` después de `heal` para leer en cada réplica.

`check.py` revisa linearizability, convergencia, que no haya dos líderes en el
mismo periodo, y mide latencia y el mayor tiempo sin respuestas. La
convergencia acepta cualquier escritura como ganadora, como en LWW. Si tus
lecturas devuelven varios valores a la vez (multi-value), define
`converged_ok` en `spec.py`. Para los líderes, `check.py` busca `roles.log`
junto al historial y avisa si no lo encuentra.

## Más réplicas

```sh
python3 cluster.py up -n 5
```
