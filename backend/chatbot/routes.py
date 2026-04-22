from flask import Blueprint, request, jsonify
from backend.chatbot.genie_service import GenieService
import logging

logger = logging.getLogger(__name__)

chatbot_bp = Blueprint('chatbot_bp', __name__)
genie_service = GenieService()

@chatbot_bp.route('/api/chatbot/ask', methods=['POST'])
def ask():
    """
    Endpoint to interact with Databricks Genie.
    Expects JSON: { "question": "...", "conversation_id": "..." (optional) }
    """
    data = request.get_json() or {}
    question = data.get('question')
    conversation_id = data.get('conversation_id')
    
    if not question:
        return jsonify({"status": "error", "message": "Question is required"}), 400
    
    try:
        # If no conversation_id, start a new one — the question is sent as the first message
        if not conversation_id:
            logger.info("No conversation_id provided, starting new Genie conversation.")
            from backend.shared.dbx_utils import get_dbx_access_token
            conversation_id, message_id, _ = genie_service.start_conversation(question)
            token = get_dbx_access_token()
            answer = genie_service._poll_for_response(conversation_id, message_id, token)
        else:
            # Send a follow-up question to the existing conversation
            logger.info(f"Asking Genie: '{question}' (conversation: {conversation_id})")
            answer = genie_service.ask_question(conversation_id, question)

        return jsonify({
            "status": "success",
            "answer": answer,
            "conversation_id": conversation_id
        })
        
    except Exception as e:
        logger.error(f"Error in chatbot ask: {e}", exc_info=True)
        return jsonify({
            "status": "error", 
            "message": f"Genie Error: {str(e)}"
        }), 500
