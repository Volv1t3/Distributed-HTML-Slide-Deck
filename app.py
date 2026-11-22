"""Tu app. Este ejemplo funciona pero NO replica: cada réplica guarda su
propia copia y no le avisa a nadie. Sirve para ver la API. Reemplázalo.

Lo que tu App puede usar (ver node.py):
  self.id, self.peers           tu nombre y el de las demás réplicas
  self.send(dst, msg)           envía un dict a otra réplica
  self.broadcast(msg)           lo envía a todas las demás
  self.disk, self.save()        estado que sobrevive a un kill
  self.reply(req_id, resp)      responde un pedido que dejaste pendiente
  self.now()                    reloj local, en segundos
  self.log(...)                 escribe en run/NODO/node.log

Un pedido de cliente (req) tiene req.id, req.op, req.args y req.client.
Responde con {"ok": True, "result": ...}, {"ok": False, "error": ...} o
{"redirect": "n2"} para mandarlo a otra réplica. "ok": False significa que
esta réplica no pudo atender el pedido (por ejemplo, no tiene mayoría), y el
cliente reintenta. Una negativa de tu app ("asiento ocupado") es una
respuesta: va con "ok": True y la negativa en "result". Si devuelves None, el pedido
queda pendiente hasta que llames self.reply(req.id, ...).

req.args trae "local": True cuando la lectura viene de client.final: responde
con el estado de esta réplica, sin redirigir.
"""
from node import Node


class App(Node):
    def start(self):
        self.data = self.disk.setdefault("data", {})

    def on_request(self, req):
        if req.op == "put":
            self.data[req.args["key"]] = req.args["value"]
            self.save()
            return {"ok": True, "result": "ok"}
        if req.op == "get":
            return {"ok": True, "result": self.data.get(req.args["key"])}
        return {"ok": False, "error": f"operación desconocida: {req.op}"}

    def on_message(self, src, msg):
        pass

    def on_tick(self):
        pass

    def status(self):
        return {"role": "replica", "term": None}
