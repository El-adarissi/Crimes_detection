from confluent_kafka import Producer
import json
import requests
import time

# Configuration du producteur Kafka
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # Adresse de votre broker Kafka
    'client.id': 'kafka-producer'
}

# Créer une instance de producteur Kafka
producer = Producer(kafka_config)

# URL de l'API
#url = 'https://data.boston.gov/api/3/action/datastore_search?resource_id=be047094-85fe-4104-a480-4fa3d03f9623'
url ='https://data.boston.gov/api/3/action/datastore_search?resource_id=64ad0053-842c-459b-9833-ff53d568f2e3'
while True:
    try:
        # Effectuer une demande GET pour obtenir les données en temps réel
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()

            # Vérifiez si la clé 'result' existe dans le dictionnaire
            if 'result' in data:
                result = data['result']

                # Vérifiez si la clé 'records' existe dans le résultat
                if 'records' in result:
                    records = result['records']

                    for record in records:
                        STREET = record.get("STREET", "").replace("\n", " ")
                        OFFENSE_DESCRIPTION = record.get("OFFENSE_DESCRIPTION", "")
                        SHOOTING = record.get("SHOOTING", "")
                        OFFENSE_CODE = record.get("OFFENSE_CODE", "")
                        DISTRICT = record.get("DISTRICT", "")
                        REPORTING_AREA = record.get("REPORTING_AREA", "")
                        OCCURRED_ON_DATE = record.get("OCCURRED_ON_DATE", "")
                        DAY_OF_WEEK = record.get("DAY_OF_WEEK", "")
                        MONTH = record.get("MONTH", "")
                        HOUR = record.get("HOUR", "")
                        Long = record.get("Long", "")
                        YEAR = record.get("YEAR", "")
                        Lat = record.get("Lat", "")
                        INCIDENT_NUMBER = record.get("INCIDENT_NUMBER", "")
                        _id = record.get("_id", "")
                        OFFENSE_CODE_GROUP = record.get("OFFENSE_CODE_GROUP", "")
                        UCR_PART = record.get("UCR_PART", "")
                        Location = record.get("Location", "")
                    

                        # Construct a message to be sent to Kafka
                        message = {
                            "id": _id,
                            "STREET": STREET,
                            "OFFENSE_DESCRIPTION": OFFENSE_DESCRIPTION,
                            "OFFENSE_CODE": OFFENSE_CODE,
                            "DISTRICT": DISTRICT,
                            "REPORTING_AREA": REPORTING_AREA,
                            "OCCURRED_ON_DATE": OCCURRED_ON_DATE,
                            "DAY_OF_WEEK": DAY_OF_WEEK,
                            "MONTH": MONTH,
                            "HOUR": HOUR,
                            "YEAR": YEAR,
                            "Location": Location,
                            "Long": Long,
                            "Lat": Lat,
                            "INCIDENT_NUMBER": INCIDENT_NUMBER,
                            "OFFENSE_CODE_GROUP": OFFENSE_CODE_GROUP,
                            "UCR_PART": UCR_PART,
                            "SHOOTING": SHOOTING
                        }

                    # Transformez l'enregistrement en une chaîne JSON
                        message_json = json.dumps(message)

                    # Publiez le message JSON dans un sujet Kafka
                        producer.produce('Topic_Crimes', value=message_json)

                        print("Message publié dans Kafka :")
                        print(message)
                        print() 
                        
                
                    time.sleep(4)
                    producer.flush()

        else:
            print(f"La requête a échoué avec le code d'état : {response.status_code}")
            time.sleep(4)

    except Exception as e:
        print(f"Une erreur s'est produite : {str(e)}")
        time.sleep(60)
 
