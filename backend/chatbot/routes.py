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
            conversation_id, message_id, token = genie_service.start_conversation(question)
            # Reuse the token returned by start_conversation (fix L2-1: no double-fetch)
            answer = genie_service._poll_for_response(conversation_id, message_id, token)
        else:
            # Send a follow-up question to the existing conversation
            # Wrap in try/except to handle stale/expired conversation IDs (fix L2-5)
            try:
                logger.info(f"Asking Genie: '{question}' (conversation: {conversation_id})")
                answer = genie_service.ask_question(conversation_id, question)
            except Exception as conv_err:
                logger.warning(f"Stale conversation {conversation_id}, starting fresh: {conv_err}")
                conversation_id, message_id, token = genie_service.start_conversation(question)
                answer = genie_service._poll_for_response(conversation_id, message_id, token)

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
