import json
import time
import requests
from kafka import KafkaProducer

def producer_kafka():
    producer = KafkaProducer(
        bootstrap_servers='localhost:29092'
    )
    return producer

def Random_user_raw():
    respuesta = requests.get("https://randomuser.me/api/")
    respuesta.raise_for_status()  
    return respuesta.content
 
def Random_user_event_json(respuesta: str) -> dict:
    
    data = json.loads(respuesta)  
    usuario = data["results"][0]  
 
    evento = {
        "usuario_id": usuario["login"]["uuid"],
        "nombre": f"{usuario['name']['first']} {usuario['name']['last']}",
        "email": usuario["email"],
        "pais": usuario["location"]["country"],
        "fecha_registro": usuario["registered"]["date"],
        "evento": "ALTA_USUARIO",
    }
    return evento

def Random_user_str(evento: dict) -> str:
    return json.dumps(evento)

def Random_user_byn(evento: str) -> bytes:
    return evento.encode("utf-8")

def main():
    producer = producer_kafka()
    topic = "actividad-topic"
    
    print(f"Enviando eventos al topic '{topic}'... (Ctrl+C para detener)")
    contador = 0
    try:
        while True:
            contador += 1
            respuesta = Random_user_raw()
            evento = Random_user_event_json(respuesta)
            evento_str = Random_user_str(evento)
            evento_byn = Random_user_byn(evento_str)

            print(f"Enviando el evento número: {contador}, con la información: {evento_str}")

            producer.send(topic, value=evento_byn)

            print(f"Evento enviado: {evento_str}")

            time.sleep(5)  

    except KeyboardInterrupt:
        print("Deteniendo el productor...")
    finally:
        producer.flush()
        producer.close()
        print("Productor detenido.")
    

if __name__ == "__main__":
    main()