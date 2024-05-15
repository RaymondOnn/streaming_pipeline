
* This module provides functions for interacting with Kafka in your application.   
* It includes methods for: 
    * sending messages with or without keys to specific topics, 
    * creating and registering new topics in Kafka asynchronously, 
    * handling delivery reports, and 
    * managing AdminClient instances for Kafka topic management.   
* This module helps facilitate communication through Kafka messaging in your application.

### Functions:
* These functions handle different aspects of working with Kafka topics, messages, and delivery reports within your application.
    * `_send_delivery_report`: Sends a delivery report based on the error and message received.
    * `extract_key`: Splits the specified key value from the rest of the dictionary.
    * `get_admin_client`: Creates an instance of AdminClient for managing Kafka topics.
    * `_create_topic`: Creates and registers a new topic in Kafka with a specified number of partitions and replication factor.
    * `_confirm_topic_creation`: Confirms that a topic creation was successful.



::: src.flask_api.app.fx.pubsub
    options:
        members_order: source