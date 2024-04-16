import json
import logging
from typing import List

import requests

from app.entities.processed_agent_data import ProcessedAgentData
from app.interfaces.store_gateway import StoreGateway


class StoreApiAdapter(StoreGateway):
    def __init__(self, api_base_url):
        self.api_base_url = api_base_url
    def save_data(self, processed_agent_data_batch: List[ProcessedAgentData]) -> bool:
        try:
            data = [data.dict() for data in processed_agent_data_batch]
            
            for item in data:
                item['agent_data']['timestamp'] = item['agent_data']['timestamp'].isoformat()

            response = requests.post(f"{self.api_base_url}/processed_agent_data", json=data)
        
            if response.ok:
                logging.info("Data saved successfully")
                return True
            else:
                logging.error(f"Failed to save data. Status code: {response.status_code}")
                return False
        
        except Exception as e:
            logging.error(f"Error saving data: {e}")
            return False
    