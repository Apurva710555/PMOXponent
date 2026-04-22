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

    def start_conversation(self, question):
        """
        Starts a new Genie conversation with an initial question.
        Returns (conversation_id, message_id).
        """
        if not self.space_id:
            raise ValueError("GENIE_SPACE_ID not configured")

        token = get_dbx_access_token()
        # Correct Genie API endpoint to start a conversation
        url = f"{self.workspace_url}/api/2.0/genie/spaces/{self.space_id}/start-conversation"
        payload = {"content": question}

        logger.info(f"Starting new Genie conversation in space {self.space_id}")
        response = requests.post(url, headers=self._get_headers(token), json=payload)
        response.raise_for_status()
        data = response.json()

        conversation_id = data.get("conversation_id") or (data.get("conversation") or {}).get("id")
        message_id = data.get("message_id") or (data.get("message") or {}).get("id")
        return conversation_id, message_id, token

    def ask_question(self, conversation_id, question):
        """
        Sends a follow-up question to an existing Genie conversation and waits for the response.
        """
        if not self.space_id:
            raise ValueError("GENIE_SPACE_ID not configured")

        token = get_dbx_access_token()
        url = f"{self.workspace_url}/api/2.0/genie/spaces/{self.space_id}/conversations/{conversation_id}/messages"
        payload = {"content": question}

        logger.info(f"Sending follow-up question to Genie conversation {conversation_id}")
        response = requests.post(url, headers=self._get_headers(token), json=payload)
        response.raise_for_status()

        message = response.json()
        message_id = message.get("id") or message.get("message_id")

        # Poll for completion
        return self._poll_for_response(conversation_id, message_id, token)

    def _poll_for_response(self, conversation_id, message_id, token, timeout=90):
        """Polls the message status until COMPLETED or FAILED, then extracts text."""
        url = f"{self.workspace_url}/api/2.0/genie/spaces/{self.space_id}/conversations/{conversation_id}/messages/{message_id}"

        start_time = time.time()
        while time.time() - start_time < timeout:
            response = requests.get(url, headers=self._get_headers(token))
            response.raise_for_status()
            message = response.json()

            status = message.get("status")
            logger.info(f"Genie message {message_id} status: {status}")

            if status == "COMPLETED":
                return self._extract_answer(message)
            elif status in ("FAILED", "CANCELLED"):
                error = message.get("error", {}).get("message", "Unknown error")
                raise Exception(f"Genie message failed: {error}")

            time.sleep(2)  # Wait 2 seconds before next poll

        raise Exception("Genie response timed out after 90 seconds")

    def _extract_answer(self, message):
        """
        Extracts the human-readable answer text from a completed Genie message.
        Genie returns results in 'attachments' as TEXT or QUERY type.
        """
        attachments = message.get("attachments") or []
        text_parts = []
        for attachment in attachments:
            # Text-type attachment
            text_node = attachment.get("text") or {}
            if text_node.get("content"):
                text_parts.append(text_node["content"])
            # Query-type attachment — return the query description if present
            query_node = attachment.get("query") or {}
            if query_node.get("description"):
                text_parts.append(query_node["description"])

        if text_parts:
            return "\n\n".join(text_parts)

        # Fallback: top-level content field
        return message.get("content", "I processed your request but received no text response.")
