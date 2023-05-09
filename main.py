# Importing all needed libraries.
import threading
from flask import Flask, request
from pymemcache.client import base
import requests
import time

# Importing all needed libraries.
from config import ConfigManager
from cerber import SecurityManager

# Defining the configuration manager.
config = ConfigManager("config.ini")

# Defining the security manager.
security_manager = SecurityManager(config.security.secret_key)

# Create memcached servers.
client1 = base.Client((config.memcached.cache_host_1, config.memcached.cache_port_1))
client2 = base.Client((config.memcached.cache_host_2, config.memcached.cache_port_2))
client3 = base.Client((config.memcached.cache_host_3, config.memcached.cache_port_3))

# Define the hash ring.
HASH_RING = {
    0 : client1,
    120 : client2,
    240 : client3
}

# Creating the security manager for the service discovery.
service_discovery_security_manager = SecurityManager(config.service_discovery.secret_key)

# Computing the HMAC for Service Discovery registration.
SERVICE_DISCOVERY_HMAC = service_discovery_security_manager._SecurityManager__encode_hmac(
    config.generate_info_for_service_discovery()
)

def send_heartbeats():
    '''
        This function sends heartbeat requests to the service discovery.
    '''
    # Getting the Service discovery hmac for message.
    service_discovery_hmac = service_discovery_security_manager._SecurityManager__encode_hmac({"status_code" : 200})
    while True:
        # Senting the request.
        response = requests.post(
            f"http://{config.service_discovery.host}:{config.service_discovery.port}/heartbeat/{config.general.name}",
            json = {"status_code" : 200},
            headers = {"Token" : service_discovery_hmac}
        )
        # Making a pause of 30 seconds before sending the next request.
        status_code = response.status_code
        time.sleep(30)


# Registering to the Service discovery.
while True:
    # Sending the request to the service discovery.
    resp = requests.post(
        f"http://{config.service_discovery.host}:{config.service_discovery.port}/{config.service_discovery.register_endpoint}",
        json = config.generate_info_for_service_discovery(),
        headers={"Token" : SERVICE_DISCOVERY_HMAC}
    )
    # If the request is succesfull then sending of heartbeat requests is starting.
    if resp.status_code == 200:
        threading.Thread(target=send_heartbeats).start()
        break

# Defining the function that finds the correct memcache service.
def find_memcache_service(memcache_query : str) -> int:
    '''
        This function based on the hash of the query is determining the responsible memcache.
            :param memcache_query: str
                The query of the service. Has the following logic:
                    sentiment -> sentiment[text]
                    intent -> intent[text]
                    ner -> ner[text]
                    seq2seq -> response[text]
            :return: int
                The index of responsible memcache service.
    '''
    chosen_service_index = 0
    min_difference = 360

    # Computing the hash of the query.
    hash_payload = int(hash(memcache_query))
    hash_mod_360 = hash_payload % 360

    # Searching for the responsible service.
    for index in HASH_RING:
        difference = abs(hash_mod_360 - index)
        if difference < min_difference:
            min_difference = difference
            chosen_service_index = index
    return chosen_service_index


app = Flask(__name__)


@app.route("/save", methods=["POST"])
def save():
    '''
        This function is called when the /save endpoint is triggered.
    '''
    # Checking the access token.
    check_response = security_manager.check_request(request)
    if check_response != "OK":
        return check_response, check_response["code"]
    else:
        # Extracting the field from the query.
        text = request.json["text"]
        service = request.json["service"]
        prediction = request.json["prediction"]

        # Creation of the query.
        memcache_query = f"{service}[{text}]"
        # Getting the index of the responsible memcache service.
        memcache_index = find_memcache_service(memcache_query)
        # Saving the value to the responsible memcache based on the hash.
        HASH_RING[memcache_index].set(memcache_query.replace(" ", "_"),
                                      prediction, expire=config.memcached.expire_sec)

        return {
            "message" : "Saved!",
        }, 200

@app.route("/cache", methods=["GET"])
def cache():
    '''
        This function returns the cached value if it exists when the /cache endpoint is called.
    '''
    # Checking the access token.
    check_response = security_manager.check_request(request)
    if check_response != "OK":
        return check_response, check_response["code"]
    else:
        # Extracting the field from the query.
        text = request.json["text"]
        service = request.json["service"]

        # Creation of the query.
        memcache_query = f"{service}[{text}]"
        # Getting the index of the responsible memcache service.
        memcache_index = find_memcache_service(memcache_query)
        # Getting the value from the responsible memcache service.
        cached_value = HASH_RING[memcache_index].get(memcache_query.replace(" ", "_"))

        if cached_value:
            return {
                "prediction" : cached_value.decode("utf-8")
            }, 200
        else:
            return {
                "message" : "No such data!"
            }, 404

# Running the service.
app.run(
    host="0.0.0.0",
    port=config.general.port
)
