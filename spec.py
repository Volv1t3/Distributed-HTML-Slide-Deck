"""Qué hace cada operación de tu app, como si hubiera una sola copia.

check.py usa este archivo para revisar tus historiales. Cámbialo para tu app.
Este ejemplo es un registro clave-valor: put escribe, get lee.
"""

# La garantía de cada operación: "linearizable" o "eventual". Para
# read-after-write usa "eventual"; muéstrala con un escenario tuyo que compare
# cada lectura de un cliente con su última escritura. Haz lo mismo con otra
# garantía que check.py no revisa, como el orden causal.
#
# Opcional: si tu diseño eventual devuelve algo que una sola copia no puede
# dar (por ejemplo, varios valores a la vez), define
#     def converged_ok(updates, result): ...
# que devuelve True si result es un valor final correcto para esas escrituras.
# check.py la usa en vez de aplicar las escrituras en orden.
GUARANTEE = {
    "put": "linearizable",
    "get": "eventual",
}

# Operaciones que solo leen (no cambian el estado).
READS = {"get"}

# Valor inicial de cada objeto. Es el mismo para todos los objetos: si tu app
# mezcla tipos (un registro y un conjunto), trata None en apply.
INITIAL = None


def key(op, args):
    """El objeto que toca la operación. Operaciones sobre objetos distintos se
    revisan por separado."""
    return args.get("key")


def apply(state, op, args):
    """Aplica op a state con una sola copia. Devuelve (nuevo_estado, resultado)."""
    if op == "put":
        return args["value"], "ok"
    if op == "get":
        return state, state
    raise ValueError(f"operación desconocida: {op}")
