import os
import requests
import json
import time
import logging
from backend.shared.dbx_utils import get_dbx_access_token

logger = logging.getLogger(__name__)

class GenieService:
    def __init__(self):
        self.workspace_url = os.getenv("WORKSPACE_INSTANCE", "").rstrip("/")
        self.space_id = os.getenv("GENIE_SPACE_ID")

    def _get_headers(self, token):
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

    def start_conversation(self):
        """Starts a new conversation in the Genie space."""
        if not self.space_id:
            raise ValueError("GENIE_SPACE_ID not configured")

        token = get_dbx_access_token()
        url = f"{self.workspace_url}/api/2.0/genie/spaces/{self.space_id}/conversations"
        
        logger.info(f"Starting new Genie conversation in space {self.space_id}")
        response = requests.post(url, headers=self._get_headers(token), json={})
        response.raise_for_status()
        return response.json()

    def ask_question(self, conversation_id, question):
        """
        Sends a question to an existing Genie conversation and waits for the response.
        """
        if not self.space_id:
            raise ValueError("GENIE_SPACE_ID not configured")

        token = get_dbx_access_token()
        url = f"{self.workspace_url}/api/2.0/genie/spaces/{self.space_id}/conversations/{conversation_id}/messages"
        
        payload = {
            "content": question
        }
        
        logger.info(f"Sending question to Genie conversation {conversation_id}")
        response = requests.post(url, headers=self._get_headers(token), json=payload)
        response.raise_for_status()
        
        message = response.json()
        message_id = message.get("id")
        
        # Poll for completion if status is not COMPLETED
        return self._poll_for_response(conversation_id, message_id, token)

    def _poll_for_response(self, conversation_id, message_id, token, timeout=60):
        """Polls the message status until it is COMPLETED or FAILED."""
        url = f"{self.workspace_url}/api/2.0/genie/spaces/{self.space_id}/conversations/{conversation_id}/messages/{message_id}"
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            response = requests.get(url, headers=self._get_headers(token))
            response.raise_for_status()
            message = response.json()
            
            status = message.get("status")
            logger.info(f"Genie message {message_id} status: {status}")
            
            if status == "COMPLETED":
                return message
            elif status == "FAILED":
                error = message.get("error", {}).get("message", "Unknown error")
                raise Exception(f"Genie message failed: {error}")
            
            time.sleep(1) # Wait 1 second before polling again
            
        raise Exception("Genie response timed out")
